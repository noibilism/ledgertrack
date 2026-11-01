package bankman

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

// Importer metadata keys (see internal/legacyimport).
const (
	metaLegacyAccountNumber = "legacy_account_number"
)

// accountView is the account as Bankman's Account model serializes it to API callers. Its hidden attributes
// (client_id, product_id, wallet_role, limits, overdraft, timestamps) are left out, as Bankman leaves them out.
type accountView struct {
	ID               string  `json:"id"`
	AccountNumber    string  `json:"account_number"`
	AccountName      *string `json:"account_name"`
	Currency         string  `json:"currency"`
	CountryCode      *string `json:"country_code"`
	Status           string  `json:"status"`
	AccountType      *string `json:"account_type"`
	AvailableBalance string  `json:"available_balance"`
	ActualBalance    string  `json:"actual_balance"`
	LockedAmount     string  `json:"locked_amount"`
	LastActivityAt   *string `json:"last_activity_at"`
}

// bankmanID is the id callers know the account by: the Bankman id for imported accounts, the LedgerTrack id
// for accounts opened after cutover.
func bankmanID(a *models.Account) string {
	if v, ok := a.Metadata["legacy_source_id"].(string); ok && v != "" {
		return v
	}
	return a.ID.String()
}

func metaString(a *models.Account, key string) *string {
	if v, ok := a.Metadata[key].(string); ok && v != "" {
		return &v
	}
	return nil
}

// balances reads the available and lien (locked) amounts of an account, in minor units.
func (h *Handler) balances(r *http.Request, a *models.Account) (available, lien int64, err error) {
	rp, err := h.call(r, http.MethodGet, h.accountPath(a, ""), nil)
	if err != nil {
		return 0, 0, err
	}
	if !rp.ok() {
		return 0, 0, errStatus(rp)
	}
	var detail struct {
		Data struct {
			WalletInfo struct {
				AvailableBalance int64  `json:"available_balance"`
				LienAddress      string `json:"lien_address"`
			} `json:"wallet_info"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rp.Body, &detail); err != nil {
		return 0, 0, err
	}
	available = detail.Data.WalletInfo.AvailableBalance

	asset, err := currencyregistry.AssetFor(a.Currency)
	if err != nil {
		return 0, 0, err
	}
	lienPath := "/" + url.PathEscape(h.ledger) + "/accounts/" + url.PathEscape(detail.Data.WalletInfo.LienAddress) + "?expand=volumes"
	lp, err := h.call(r, http.MethodGet, lienPath, nil)
	if err != nil {
		return 0, 0, err
	}
	switch {
	case lp.Code == http.StatusNotFound:
		return available, 0, nil // never touched, so nothing is held
	case !lp.ok():
		return 0, 0, errStatus(lp)
	}
	var vols struct {
		Data struct {
			Volumes map[string]struct {
				Balance json.Number `json:"balance"`
			} `json:"volumes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(lp.Body, &vols); err != nil {
		return 0, 0, err
	}
	if v, ok := vols.Data.Volumes[asset]; ok && v.Balance.String() != "" {
		d, err := decimal.NewFromString(v.Balance.String())
		if err != nil {
			return 0, 0, err
		}
		lien = d.IntPart()
	}
	return available, lien, nil
}

type statusError struct{ rp reply }

func (e statusError) Error() string { return e.rp.message() }

func errStatus(rp reply) error { return statusError{rp} }

// fourDecimals renders minor units the way Bankman's decimal:4 casts do ("1500.0000").
func fourDecimals(minor int64, currency string) string {
	def, ok := currencyregistry.Lookup(currency)
	if !ok {
		return decimal.NewFromInt(minor).StringFixed(4)
	}
	return decimal.NewFromInt(minor).Shift(int32(-def.Precision)).StringFixed(4)
}

func (h *Handler) view(r *http.Request, a *models.Account) (*accountView, error) {
	available, lien, err := h.balances(r, a)
	if err != nil {
		return nil, err
	}
	number := a.AccountNumber
	if v := metaString(a, metaLegacyAccountNumber); v != nil {
		number = *v
	}
	var last *string
	if a.LastActivityAt != nil {
		s := a.LastActivityAt.UTC().Format(time.RFC3339)
		last = &s
	}
	return &accountView{
		ID:               bankmanID(a),
		AccountNumber:    number,
		Currency:         a.Currency,
		Status:           a.Status,
		AvailableBalance: fourDecimals(available, a.Currency),
		// Bankman's ledger balance includes funds under lien.
		ActualBalance:  fourDecimals(available+lien, a.Currency),
		LockedAmount:   fourDecimals(lien, a.Currency),
		LastActivityAt: last,
	}, nil
}

// listAccounts answers GET /accounts?external_id=… or ?client_id=… (the Bankman client id).
func (h *Handler) listAccounts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var (
		client *models.Client
		err    error
	)
	switch {
	case strings.TrimSpace(q.Get("external_id")) != "":
		client, err = h.lookup.Client(r.Context(), q.Get("external_id"))
		if err != nil {
			h.lookupError(w, "Client not found with the provided external_id", err)
			return
		}
	case strings.TrimSpace(q.Get("client_id")) != "":
		client, err = h.lookup.ClientByID(r.Context(), q.Get("client_id"))
		if err != nil {
			if errors.Is(err, services.ErrLegacyNotFound) {
				respond(w, http.StatusOK, "Success", []accountView{})
				return
			}
			fail(w, http.StatusInternalServerError, "Failed to fetch accounts")
			return
		}
	default:
		// Bankman lists every account here. That is never what a caller wants from a ledger of this size.
		fail(w, http.StatusUnprocessableEntity, "external_id or client_id is required")
		return
	}

	rp, err := h.call(r, http.MethodGet, "/"+url.PathEscape(h.ledger)+"/accounts?client_id="+client.ID.String(), nil)
	if err != nil || !rp.ok() {
		fail(w, http.StatusInternalServerError, "Failed to fetch accounts")
		return
	}
	var list struct {
		Data struct {
			Accounts []models.Account `json:"accounts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rp.Body, &list); err != nil {
		fail(w, http.StatusInternalServerError, "Failed to fetch accounts")
		return
	}
	views := make([]accountView, 0, len(list.Data.Accounts))
	for i := range list.Data.Accounts {
		v, err := h.view(r, &list.Data.Accounts[i])
		if err != nil {
			fail(w, http.StatusInternalServerError, "Failed to fetch accounts")
			return
		}
		views = append(views, *v)
	}
	respond(w, http.StatusOK, "Success", views)
}

// showAccount answers GET /accounts/{id}.
func (h *Handler) showAccount(w http.ResponseWriter, r *http.Request) {
	account, err := h.lookup.Account(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		h.lookupError(w, "Account not found", err)
		return
	}
	v, err := h.view(r, account)
	if err != nil {
		fail(w, http.StatusInternalServerError, "Failed to fetch account details")
		return
	}
	respond(w, http.StatusOK, "Success", v)
}
