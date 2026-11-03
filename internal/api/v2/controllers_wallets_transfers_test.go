package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/transfers"
	"github.com/formancehq/ledger/internal/transfers/transferstest"
)

type transferHarness struct {
	t       *testing.T
	router  http.Handler
	store   *transferstest.MemoryStore
	ledgers *transferstest.FakeLedgers
	fees    *transferstest.FakeFees
}

func newTransferHarness(t *testing.T, mutate ...func(*transfers.Config)) *transferHarness {
	t.Helper()
	cfg := transfers.DefaultConfig()
	cfg.ClientRetryCooldown = 0
	for _, m := range mutate {
		m(&cfg)
	}
	h := &transferHarness{
		t:       t,
		store:   transferstest.NewMemoryStore(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)),
		ledgers: transferstest.NewFakeLedgers(),
		fees:    transferstest.NewFakeFees(),
	}
	svc := transfers.NewService(h.store, transfers.NewLedgerExecutor(h.ledgers, h.fees), cfg, logging.Testing())

	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	h.router = NewRouter(systemController, auth.NewNoAuth(), "develop", WithTransferService(svc))

	h.ledgers.Fund("test", "users:u1:wallets:USD:available", "USD/2", 10_000)
	h.ledgers.Fund("test", "users:u1:wallets:USD:lien", "USD/2", 10_000)
	return h
}

func (h *transferHarness) post(path string, body any, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, api.Buffer(h.t, body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	return rec
}

func debitBody(ref string, amount, channelAmount string) map[string]any {
	return map[string]any{"amount": json.Number(amount), "reference": ref, "channelID": "ch1", "channelAmount": json.Number(channelAmount)}
}

func errorBody(t *testing.T, rec *httptest.ResponseRecorder) api.ErrorResponse {
	t.Helper()
	var e api.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &e))
	return e
}

func TestChannelDebitThroughTransfersKeepsResponseShape(t *testing.T) {
	h := newTransferHarness(t)

	rec := h.post("/test/wallets/u1-USD/debit", debitBody("r1", "1000", "900"))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Empty(t, rec.Header().Get("Idempotency-Hit"))

	var body struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	for _, k := range []string{"txid", "timestamp", "postings", "metadata", "balance_before", "balance_after"} {
		require.Contains(t, body.Data, k)
	}
	require.EqualValues(t, 10_000, body.Data["balance_before"])
	require.EqualValues(t, 9_000, body.Data["balance_after"])
	md := body.Data["metadata"].(map[string]any)
	require.Equal(t, "channels-USD", md["channel_ledger"])
	require.Equal(t, "revenue-USD", md["revenue_ledger"])
	require.Equal(t, "100", md["channel_user_fee_amount"])
	require.Equal(t, "ch1", md["channel_id"])
	require.Equal(t, "900", md["channel_amount"])

	require.Equal(t, 1, h.ledgers.Applied("test", "r1"))
	require.Equal(t, 1, h.ledgers.Applied("channels-USD", "r1"))
	require.Equal(t, 1, h.ledgers.Applied("revenue-USD", "r1-user-fee"))
	require.Equal(t, 1, h.fees.Rows("r1"))
}

func TestChannelDebitPartialFailureIsNotASuccessAndRetryCompletesIt(t *testing.T) {
	h := newTransferHarness(t)
	failing := true
	h.ledgers.BeforeCommit = func(m transfers.Movement) error {
		if failing && m.Ledger == "channels-USD" {
			return fmt.Errorf("channel ledger down")
		}
		return nil
	}

	rec := h.post("/test/wallets/u1-USD/debit", debitBody("r2", "1000", "900"))
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	e := errorBody(t, rec)
	require.Equal(t, ErrTransferPendingCompletion, e.ErrorCode)
	require.Contains(t, e.ErrorMessage, "principal")
	require.Contains(t, e.ErrorMessage, "posted")
	require.Contains(t, e.ErrorMessage, "same reference")
	require.Equal(t, 1, h.ledgers.Applied("test", "r2"), "the customer is debited")
	require.Zero(t, h.ledgers.Applied("channels-USD", "r2"))

	// Retrying the identical request completes it, without a second debit.
	failing = false
	rec = h.post("/test/wallets/u1-USD/debit", debitBody("r2", "1000", "900"))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "true", rec.Header().Get("Idempotency-Hit"))
	require.Equal(t, 1, h.ledgers.Applied("test", "r2"))
	require.Equal(t, 1, h.ledgers.Applied("channels-USD", "r2"))
	require.Equal(t, 1, h.ledgers.Applied("revenue-USD", "r2-user-fee"))

	// And a further retry returns the stored result without touching ledgers.
	calls := h.ledgers.PostCalls()
	rec = h.post("/test/wallets/u1-USD/debit", debitBody("r2", "1000", "900"))
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, calls, h.ledgers.PostCalls())
}

func TestChannelDebitRejections(t *testing.T) {
	h := newTransferHarness(t)

	rec := h.post("/test/wallets/u1-USD/debit", debitBody("r3", "999999", "900"))
	require.Equal(t, http.StatusPaymentRequired, rec.Code, rec.Body.String())
	require.Equal(t, "INSUFFICIENT_FUND", errorBody(t, rec).ErrorCode)

	rec = h.post("/test/wallets/u1-USD/debit", debitBody("r4", "1000", "900"))
	require.Equal(t, http.StatusCreated, rec.Code)
	rec = h.post("/test/wallets/u1-USD/debit", debitBody("r4", "1000", "800"))
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	require.Equal(t, "CONFLICT", errorBody(t, rec).ErrorCode)
}

func TestChannelDebitNeedsReviewAndInProgress(t *testing.T) {
	h := newTransferHarness(t, func(c *transfers.Config) { c.MaxAttempts = 1 })
	h.ledgers.BeforeCommit = func(m transfers.Movement) error {
		if m.Ledger == "revenue-USD" {
			return fmt.Errorf("revenue ledger down")
		}
		return nil
	}
	rec := h.post("/test/wallets/u1-USD/debit", debitBody("r5", "1000", "900"))
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.Equal(t, ErrTransferNeedsReview, errorBody(t, rec).ErrorCode)

	rec = h.post("/test/wallets/u1-USD/debit", debitBody("r5", "1000", "900"))
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, ErrTransferNeedsReview, errorBody(t, rec).ErrorCode)
	require.Equal(t, 1, h.ledgers.Applied("test", "r5"))
}

func TestChannelLienReleasePayThroughTransfers(t *testing.T) {
	h := newTransferHarness(t)
	body := map[string]any{
		"amount": json.Number("1000"), "reference": "rel1", "mode": "PAY", "channelID": "ch1", "channelAmount": json.Number("900"),
	}
	rec := h.post("/test/wallets/u1-USD/lien/release", body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, int64(9_000), h.ledgers.Balance("test", "users:u1:wallets:USD:lien", "USD/2"))
	require.Equal(t, 1, h.ledgers.Applied("channels-USD", "rel1"))
	require.Equal(t, 1, h.fees.Rows("rel1"))
	row := h.fees.Row("rel1")
	require.Equal(t, "PAY", row.Metadata["mode"])

	rec = h.post("/test/wallets/u1-USD/lien/release", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "true", rec.Header().Get("Idempotency-Hit"))
	require.Equal(t, int64(9_000), h.ledgers.Balance("test", "users:u1:wallets:USD:lien", "USD/2"))
}
