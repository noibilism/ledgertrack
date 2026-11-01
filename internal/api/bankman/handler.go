// Package bankman serves the part of Bankman's HTTP contract that the Cray middleware and snowman use for
// money movement, on top of LedgerTrack. It holds no ledger logic of its own: every request is translated and
// dispatched in process, through the same router and middleware, to the LedgerTrack account endpoints
// (/{ledger}/accounts/{id}/credit, debit, lien, lien/release, /{ledger}/transactions/{id}/revert), so the
// compat layer cannot do anything the native API refuses to do.
//
// Callers switch by changing one base URL. Mounted at /bankman/api/v1.
package bankman

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

// Metadata keys written on the LedgerTrack transactions the layer creates.
const (
	MetaTransactionID = "bankman_transaction_id" // the UUID handed back to Bankman callers
	MetaLienRef       = "bankman_lien_ref"       // LedgerTrack tx id of the lien a release belongs to
	MetaReversalOf    = "bankman_reversal_of"
	metaLegacySource  = "legacy_source_id" // set by the importer on opening balances and liens
)

// transactionIDNamespace derives stable Bankman-style UUIDs from LedgerTrack references, so a replay of the
// same request answers with the same transaction_id.
var transactionIDNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ledgertrack/bankman-compat"))

// TransactionID is the UUID returned to callers for a reference.
func TransactionID(reference string) string {
	return uuid.NewSHA1(transactionIDNamespace, []byte(reference)).String()
}

// Handler is the Bankman compatibility API.
type Handler struct {
	lookup services.LegacyLookupService
	serve  http.Handler
	ledger string
}

// New builds the handler. `serve` is the router that serves the LedgerTrack v2 API (paths without the /v2
// prefix); `ledger` is the wallet ledger name.
func New(lookup services.LegacyLookupService, serve http.Handler, ledger string) http.Handler {
	h := &Handler{lookup: lookup, serve: serve, ledger: ledger}
	r := chi.NewRouter()
	r.Route("/transactions", func(r chi.Router) {
		r.Post("/deposits/{accountID}", h.deposit)
		r.Post("/withdrawals/{accountID}", h.withdraw)
		r.Post("/liens/{accountID}", h.placeLien)
		r.Put("/liens/{lienID}", h.releaseLien)
		r.Post("/liens/{lienID}/release-and-withdraw", h.releaseAndWithdraw)
		r.Post("/reversal", h.reverseByBody)
		r.Post("/{id}/reversals", h.reverseByPath)
	})
	return r
}

// payload is the union of the request fields the middleware sends.
type payload struct {
	ExternalID          string      `json:"external_id"`
	Amount              json.Number `json:"amount"`
	SessionID           string      `json:"session_id"`
	TransactionRef      string      `json:"transaction_reference"`
	Date                string      `json:"date"`
	Description         string      `json:"description"`
	Channel             string      `json:"channel"`
	ExpiryDate          string      `json:"expiry_date"`
	OriginalTransaction string      `json:"original_transaction_id"`
	Reason              string      `json:"reason"`
	ReversalDate        string      `json:"reversal_date"`
}

func decode(r *http.Request) (*payload, error) {
	var p payload
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return &p, nil
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	return &p, nil
}

// ---- envelope ---------------------------------------------------------------------------------------------

func respond(w http.ResponseWriter, code int, message string, data any) {
	status := "success"
	if code >= 400 {
		status = "error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "message": message, "data": data})
}

func fail(w http.ResponseWriter, code int, message string) { respond(w, code, message, nil) }

// ---- in-process dispatch ----------------------------------------------------------------------------------

type reply struct {
	Code int
	Body []byte
}

func (rp reply) ok() bool { return rp.Code >= 200 && rp.Code < 300 }

// message extracts the human message of a LedgerTrack error body.
func (rp reply) message() string {
	var e struct {
		ErrorMessage string `json:"errorMessage"`
		Details      string `json:"details"`
	}
	if json.Unmarshal(rp.Body, &e) == nil && e.ErrorMessage != "" {
		return e.ErrorMessage
	}
	return strings.TrimSpace(string(rp.Body))
}

// call dispatches to the v2 router. The route context of the outer router is dropped, otherwise chi would reuse
// it and route on the wrong path. The Authorization header is forwarded so the inner authentication and
// permission checks apply to the call exactly as they would to a direct one.
func (h *Handler) call(r *http.Request, method, path string, body any) (reply, error) {
	var buf io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return reply{}, err
		}
		buf = bytes.NewReader(raw)
	}
	ctx := context.WithValue(r.Context(), chi.RouteCtxKey, (*chi.Context)(nil))
	req, err := http.NewRequestWithContext(ctx, method, path, buf)
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if v := r.Header.Get("Authorization"); v != "" {
		req.Header.Set("Authorization", v)
	}
	rec := httptest.NewRecorder()
	h.serve.ServeHTTP(rec, req)
	return reply{Code: rec.Code, Body: rec.Body.Bytes()}, nil
}

func (h *Handler) accountPath(a *models.Account, suffix string) string {
	return "/" + url.PathEscape(h.ledger) + "/accounts/" + a.ID.String() + suffix
}

// ---- amounts ----------------------------------------------------------------------------------------------

// majorAmount normalizes a Bankman amount to the decimal string the LedgerTrack API expects.
//
// LedgerTrack reads an amount without a '.' as minor units, Bankman amounts are always major units, so the
// result always carries a decimal point (except for zero-precision currencies where both readings agree).
// Amounts with more decimals than the currency allows are rounded half-even, as Bankman's decimal column did,
// rather than rejected.
func majorAmount(n json.Number, currency string) (string, error) {
	if strings.TrimSpace(n.String()) == "" {
		return "", errors.New("amount is required")
	}
	d, err := decimal.NewFromString(n.String())
	if err != nil {
		return "", fmt.Errorf("invalid amount %q", n.String())
	}
	def, ok := currencyregistry.Lookup(currency)
	if !ok {
		return "", fmt.Errorf("unsupported currency %s", currency)
	}
	d = d.RoundBank(int32(def.Precision))
	if d.Sign() <= 0 {
		return "", errors.New("amount must be greater than zero")
	}
	return d.StringFixed(int32(def.Precision)), nil
}

func majorFromMinor(minor int64, currency string) string {
	s, err := currencyregistry.FormatMinor(minor, currency)
	if err != nil {
		return strconv.FormatInt(minor, 10)
	}
	return s
}

// ---- resolution -------------------------------------------------------------------------------------------

// account resolves the path account and checks that it belongs to the client named by external_id.
func (h *Handler) account(w http.ResponseWriter, r *http.Request, id, externalID string) (*models.Account, bool) {
	account, err := h.lookup.Account(r.Context(), id)
	if err != nil {
		h.lookupError(w, "Account not found", err)
		return nil, false
	}
	if strings.TrimSpace(externalID) != "" {
		client, err := h.lookup.Client(r.Context(), externalID)
		if err != nil {
			h.lookupError(w, "Client not found", err)
			return nil, false
		}
		if client.ID != account.ClientID {
			fail(w, http.StatusNotFound, "Account not found")
			return nil, false
		}
	}
	return account, true
}

func (h *Handler) lookupError(w http.ResponseWriter, notFound string, err error) {
	if errors.Is(err, services.ErrLegacyNotFound) {
		fail(w, http.StatusNotFound, notFound)
		return
	}
	fail(w, http.StatusInternalServerError, "Internal error")
}

type ledgerTx struct {
	ID        int64             `json:"id"`
	Reference string            `json:"reference"`
	Metadata  map[string]string `json:"metadata"`
	Postings  []struct {
		Source      string          `json:"source"`
		Destination string          `json:"destination"`
		Asset       string          `json:"asset"`
		Amount      decimal.Decimal `json:"amount"`
	} `json:"postings"`
}

// findTx looks a transaction up by a v2 list filter. Returns nil when there is none.
func (h *Handler) findTx(r *http.Request, query url.Values) (*ledgerTx, error) {
	query.Set("pageSize", "2")
	rp, err := h.call(r, http.MethodGet, "/"+url.PathEscape(h.ledger)+"/transactions?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if !rp.ok() {
		return nil, fmt.Errorf("transaction lookup failed (%d): %s", rp.Code, rp.message())
	}
	var out struct {
		Cursor struct {
			Data []ledgerTx `json:"data"`
		} `json:"cursor"`
	}
	if err := json.Unmarshal(rp.Body, &out); err != nil {
		return nil, err
	}
	if len(out.Cursor.Data) == 0 {
		return nil, nil
	}
	return &out.Cursor.Data[0], nil
}

func byMetadata(key, value string) url.Values {
	v := url.Values{}
	v.Set("metadata["+key+"]", value)
	return v
}

// findByBankmanID resolves the id a caller holds: a transaction created through this layer, an imported
// lien or opening balance (legacy id), or a plain LedgerTrack transaction id.
func (h *Handler) findByBankmanID(r *http.Request, id string) (*ledgerTx, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, nil
	}
	if tx, err := h.findTx(r, byMetadata(MetaTransactionID, id)); err != nil || tx != nil {
		return tx, err
	}
	if tx, err := h.findTx(r, byMetadata(metaLegacySource, id)); err != nil || tx != nil {
		return tx, err
	}
	if n, err := strconv.ParseInt(id, 10, 64); err == nil {
		rp, err := h.call(r, http.MethodGet, "/"+url.PathEscape(h.ledger)+"/transactions/"+strconv.FormatInt(n, 10), nil)
		if err != nil {
			return nil, err
		}
		if rp.ok() {
			var out struct {
				Data ledgerTx `json:"data"`
			}
			if json.Unmarshal(rp.Body, &out) == nil && out.Data.ID != 0 {
				return &out.Data, nil
			}
		}
	}
	return nil, nil
}

// ---- posting ----------------------------------------------------------------------------------------------

type posted struct {
	Code       int
	TxID       string // the Bankman-style UUID
	Raw        map[string]any
	Idempotent bool
}

// post sends a wallet operation and maps the outcome to Bankman semantics. A duplicate reference answers with
// the transaction already posted for it (idempotent), since callers retry on timeouts.
func (h *Handler) post(w http.ResponseWriter, r *http.Request, path string, body map[string]any, reference string) (*posted, bool) {
	rp, err := h.call(r, http.MethodPost, path, body)
	if err != nil {
		fail(w, http.StatusInternalServerError, "Internal error")
		return nil, false
	}
	switch {
	case rp.ok():
		var out struct {
			Data map[string]any `json:"data"`
		}
		_ = json.Unmarshal(rp.Body, &out)
		return &posted{Code: rp.Code, TxID: TransactionID(reference), Raw: out.Data}, true
	case rp.Code == http.StatusConflict:
		existing, err := h.findTx(r, url.Values{"reference": []string{reference}})
		if err != nil || existing == nil {
			fail(w, http.StatusConflict, rp.message())
			return nil, false
		}
		return &posted{Code: http.StatusOK, TxID: TransactionID(reference), Idempotent: true}, true
	case rp.Code == http.StatusNotFound:
		fail(w, http.StatusNotFound, rp.message())
	case rp.Code >= 500:
		fail(w, http.StatusInternalServerError, "Internal error")
	default:
		// Bankman answers business rule failures (insufficient funds, frozen account, limits) with 422.
		fail(w, http.StatusUnprocessableEntity, rp.message())
	}
	return nil, false
}

func baseMetadata(txID string, p *payload, account *models.Account) map[string]string {
	md := map[string]string{
		MetaTransactionID: txID,
		"bankman_source":  "compat",
	}
	set := func(k, v string) {
		if strings.TrimSpace(v) != "" {
			md[k] = v
		}
	}
	set("session_id", p.SessionID)
	set("channel", p.Channel)
	set("description", p.Description)
	set("external_id", p.ExternalID)
	set("bankman_date", p.Date)
	set("bankman_account_id", account.ID.String())
	return md
}

func requireRef(w http.ResponseWriter, p *payload) bool {
	if strings.TrimSpace(p.TransactionRef) == "" {
		fail(w, http.StatusUnprocessableEntity, "The transaction reference field is required.")
		return false
	}
	return true
}

// ---- handlers ---------------------------------------------------------------------------------------------

func (h *Handler) deposit(w http.ResponseWriter, r *http.Request) {
	p, err := decode(r)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	account, ok := h.account(w, r, chi.URLParam(r, "accountID"), p.ExternalID)
	if !ok || !requireRef(w, p) {
		return
	}
	amount, err := majorAmount(p.Amount, account.Currency)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	txID := TransactionID(p.TransactionRef)
	res, ok := h.post(w, r, h.accountPath(account, "/credit"), map[string]any{
		"amount": amount, "reference": p.TransactionRef, "metadata": baseMetadata(txID, p, account),
	}, p.TransactionRef)
	if !ok {
		return
	}
	respond(w, res.Code, "Deposit successful", map[string]any{
		"transaction_id": res.TxID,
		"client_id":      p.ExternalID,
		"account_id":     chi.URLParam(r, "accountID"),
		"status":         "completed",
		"idempotent":     res.Idempotent,
	})
}

func (h *Handler) withdraw(w http.ResponseWriter, r *http.Request) {
	p, err := decode(r)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	account, ok := h.account(w, r, chi.URLParam(r, "accountID"), p.ExternalID)
	if !ok || !requireRef(w, p) {
		return
	}
	amount, err := majorAmount(p.Amount, account.Currency)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	txID := TransactionID(p.TransactionRef)
	res, ok := h.post(w, r, h.accountPath(account, "/debit"), map[string]any{
		"amount": amount, "reference": p.TransactionRef, "metadata": baseMetadata(txID, p, account),
	}, p.TransactionRef)
	if !ok {
		return
	}
	data := map[string]any{
		"transaction_id": res.TxID,
		"status":         "completed",
		"amount":         amount,
		"idempotent":     res.Idempotent,
	}
	if before, after, found := balances(res.Raw); found {
		data["account_balance"] = map[string]any{
			"previous_available_balance": majorFromMinor(before, account.Currency),
			"current_available_balance":  majorFromMinor(after, account.Currency),
			"previous_ledger_balance":    majorFromMinor(before, account.Currency),
			"current_ledger_balance":     majorFromMinor(after, account.Currency),
		}
	}
	respond(w, res.Code, "Withdrawal successful", data)
}

func balances(raw map[string]any) (before, after int64, ok bool) {
	b, bok := raw["balance_before"].(float64)
	a, aok := raw["balance_after"].(float64)
	return int64(b), int64(a), bok && aok
}

func (h *Handler) placeLien(w http.ResponseWriter, r *http.Request) {
	p, err := decode(r)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	account, ok := h.account(w, r, chi.URLParam(r, "accountID"), p.ExternalID)
	if !ok || !requireRef(w, p) {
		return
	}
	amount, err := majorAmount(p.Amount, account.Currency)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	txID := TransactionID(p.TransactionRef)
	md := baseMetadata(txID, p, account)
	if p.ExpiryDate != "" {
		md["expiry_date"] = p.ExpiryDate
	}
	res, ok := h.post(w, r, h.accountPath(account, "/lien"), map[string]any{
		"amount": amount, "reference": p.TransactionRef, "metadata": md,
	}, p.TransactionRef)
	if !ok {
		return
	}
	respond(w, res.Code, "Lien placed successfully", map[string]any{
		"transaction_id": res.TxID,
		"status":         "completed",
		"idempotent":     res.Idempotent,
	})
}

func releaseReference(lien *ledgerTx) string {
	return "bankman-lien-release:" + strconv.FormatInt(lien.ID, 10)
}

// release frees (or pays out) the funds of a lien. A lien can be released once, whichever way: the reference
// is derived from the lien, so a second attempt, with any caller reference, finds the first and answers
// idempotently. Bankman queues this as a job and the middleware only checks for 2xx; here it is synchronous
// and answers 202 with status completed.
func (h *Handler) release(w http.ResponseWriter, r *http.Request, mode, message string) {
	p, err := decode(r)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	lien, err := h.findByBankmanID(r, chi.URLParam(r, "lienID"))
	if err != nil {
		fail(w, http.StatusInternalServerError, "Internal error")
		return
	}
	if lien == nil || len(lien.Postings) != 1 {
		fail(w, http.StatusNotFound, "Lien transaction not found")
		return
	}
	accountRef := lien.Metadata["account_id"]
	if accountRef == "" {
		accountRef = lien.Metadata["bankman_account_id"]
	}
	account, ok := h.account(w, r, accountRef, p.ExternalID)
	if !ok {
		return
	}
	ref := releaseReference(lien)
	amount := majorFromMinor(lien.Postings[0].Amount.IntPart(), account.Currency)

	txID := TransactionID(ref)
	md := baseMetadata(txID, p, account)
	md[MetaLienRef] = strconv.FormatInt(lien.ID, 10)
	if p.TransactionRef != "" {
		md["caller_reference"] = p.TransactionRef
	}
	res, ok := h.post(w, r, h.accountPath(account, "/lien/release"), map[string]any{
		"amount": amount, "reference": ref, "mode": mode, "metadata": md,
	}, ref)
	if !ok {
		return
	}
	respond(w, http.StatusAccepted, message, map[string]any{
		"transaction_id": res.TxID,
		"lien_id":        chi.URLParam(r, "lienID"),
		"status":         "completed",
		"idempotent":     res.Idempotent,
	})
}

func (h *Handler) releaseLien(w http.ResponseWriter, r *http.Request) {
	h.release(w, r, "release_only", "Lien release queued")
}

func (h *Handler) releaseAndWithdraw(w http.ResponseWriter, r *http.Request) {
	h.release(w, r, "release_and_debit", "Lien release and withdrawal queued")
}

func (h *Handler) reverseByBody(w http.ResponseWriter, r *http.Request) {
	p, err := decode(r)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	h.reverse(w, r, p, p.OriginalTransaction)
}

func (h *Handler) reverseByPath(w http.ResponseWriter, r *http.Request) {
	p, err := decode(r)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	h.reverse(w, r, p, chi.URLParam(r, "id"))
}

func (h *Handler) reverse(w http.ResponseWriter, r *http.Request, p *payload, originalID string) {
	if strings.TrimSpace(originalID) == "" {
		fail(w, http.StatusUnprocessableEntity, "The original transaction id field is required.")
		return
	}
	orig, err := h.findByBankmanID(r, originalID)
	if err != nil {
		fail(w, http.StatusInternalServerError, "Internal error")
		return
	}
	if orig == nil {
		fail(w, http.StatusNotFound, "Transaction not found")
		return
	}
	// A transaction is reversed once: the reversal id is derived from the original.
	reversalID := TransactionID("bankman-reversal:" + strconv.FormatInt(orig.ID, 10))
	md := map[string]string{
		MetaTransactionID: reversalID,
		MetaReversalOf:    originalID,
		"bankman_source":  "compat",
	}
	if p.Reason != "" {
		md["reason"] = p.Reason
	}
	if p.ReversalDate != "" {
		md["bankman_date"] = p.ReversalDate
	}
	rp, err := h.call(r, http.MethodPost, "/"+url.PathEscape(h.ledger)+"/transactions/"+strconv.FormatInt(orig.ID, 10)+"/revert", map[string]any{"metadata": md})
	if err != nil {
		fail(w, http.StatusInternalServerError, "Internal error")
		return
	}
	switch {
	case rp.ok():
		respond(w, http.StatusCreated, "Transaction reversed", map[string]any{
			"transaction_id":          reversalID,
			"original_transaction_id": originalID,
			"status":                  "completed",
		})
	case rp.Code == http.StatusNotFound:
		fail(w, http.StatusNotFound, "Transaction not found")
	case rp.Code >= 500:
		fail(w, http.StatusInternalServerError, "Internal error")
	default:
		fail(w, http.StatusUnprocessableEntity, rp.message())
	}
}
