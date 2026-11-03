package bankman

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

// Bankman transaction types (models/Transaction.php) a LedgerTrack operation maps to.
const (
	typeDeposit    = "deposit"
	typeWithdrawal = "withdrawal"
	typeLien       = "lien"
	typeRelease    = "lien_release"
	typeTransfer   = "transfer"
	typeReversal   = "reversal"
)

type balancePair struct {
	Before *string `json:"before"`
	After  *string `json:"after"`
}

type clientView struct {
	ID         string  `json:"id"`
	ExternalID *string `json:"external_id"`
	Email      *string `json:"email"`
}

type accountRef struct {
	ID            string      `json:"id"`
	AccountNumber string      `json:"account_number"`
	AccountName   *string     `json:"account_name"`
	Client        *clientView `json:"client"`
}

// transactionView is Bankman's transformTransaction() output.
type transactionView struct {
	ID                    string                  `json:"id"`
	InternalReference     string                  `json:"internal_reference"`
	ExternalReference     *string                 `json:"external_reference"`
	ProcessorReference    *string                 `json:"processor_reference"`
	TransactionType       string                  `json:"transaction_type"`
	ProcessingType        *string                 `json:"processing_type"`
	ProcessingChannel     *string                 `json:"processing_channel"`
	Direction             string                  `json:"direction"`
	Amount                string                  `json:"amount"`
	Currency              string                  `json:"currency"`
	Balances              map[string]*balancePair `json:"balances"`
	Status                string                  `json:"status"`
	Description           *string                 `json:"description"`
	Metadata              map[string]string       `json:"metadata"`
	CreatedAt             string                  `json:"created_at"`
	ApprovedAt            string                  `json:"approved_at"`
	SourceAccount         *accountRef             `json:"source_account"`
	DestinationAccount    *accountRef             `json:"destination_account"`
	IsReversal            bool                    `json:"is_reversal"`
	IsReversed            bool                    `json:"is_reversed"`
	CanReverse            bool                    `json:"can_reverse"`
	OriginalTransactionID *string                 `json:"original_transaction_id"`
}

func strPtr(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

// classify maps the LedgerTrack operation to a Bankman type and direction ("" direction: derive from the
// net movement of the available balance).
func classify(tx *ledgerTx) (txType, direction string) {
	if tx.Metadata[MetaReversalOf] != "" {
		return typeReversal, ""
	}
	switch tx.Metadata["cba_operation"] {
	case "credit", "opening_deposit", "legacy_opening_balance":
		return typeDeposit, "credit"
	case "debit":
		return typeWithdrawal, "debit"
	case "lien", "legacy_lien":
		return typeLien, "debit"
	case "lien_release":
		return typeRelease, "credit"
	default:
		return typeTransfer, ""
	}
}

func (tx *ledgerTx) bankmanID() string {
	for _, k := range []string{MetaTransactionID, metaLegacySource} {
		if v := tx.Metadata[k]; v != "" {
			return v
		}
	}
	return strconv.FormatInt(tx.ID, 10)
}

// netOn is the net movement of an address over the transaction, in minor units.
func (tx *ledgerTx) netOn(address, asset string) decimal.Decimal {
	net := decimal.Zero
	for _, p := range tx.Postings {
		if p.Asset != asset {
			continue
		}
		if p.Destination == address {
			net = net.Add(p.Amount)
		}
		if p.Source == address {
			net = net.Sub(p.Amount)
		}
	}
	return net
}

func (tx *ledgerTx) volumeAfter(address, asset string) (decimal.Decimal, bool) {
	v, ok := tx.PostCommitVolumes[address][asset]
	if !ok {
		return decimal.Zero, false
	}
	return v.Input.Sub(v.Output), true
}

func fixed4(minorAmount decimal.Decimal, currency string) string {
	return fourDecimals(minorAmount.IntPart(), currency)
}

// transactionFor builds the Bankman view of one transaction as seen from `account`.
func (h *Handler) transactionFor(r *http.Request, tx *ledgerTx, account *models.Account) *transactionView {
	asset, _ := currencyregistry.AssetFor(account.Currency)
	available := "users:" + account.WalletID + ":wallets:" + account.Currency + ":available"
	lien := "users:" + account.WalletID + ":wallets:" + account.Currency + ":lien"

	txType, direction := classify(tx)
	net := tx.netOn(available, asset)
	if direction == "" {
		if net.Sign() < 0 {
			direction = "debit"
		} else {
			direction = "credit"
		}
		if txType == typeReversal {
			// a reversal undoes the direction of the original: money back in is a credit
			if net.Sign() >= 0 {
				direction = "credit"
			}
		}
	}
	amount := net.Abs()
	if amount.IsZero() && len(tx.Postings) > 0 {
		amount = tx.Postings[0].Amount // e.g. a lien paid out of the held funds
	}

	balances := map[string]*balancePair{"ledger": {}, "available": {}, "locked": {}}
	if after, ok := tx.volumeAfter(available, asset); ok {
		before := after.Sub(net)
		balances["available"] = &balancePair{Before: strPtr(fixed4(before, account.Currency)), After: strPtr(fixed4(after, account.Currency))}
		if lienAfter, ok := tx.volumeAfter(lien, asset); ok {
			lienBefore := lienAfter.Sub(tx.netOn(lien, asset))
			balances["locked"] = &balancePair{Before: strPtr(fixed4(lienBefore, account.Currency)), After: strPtr(fixed4(lienAfter, account.Currency))}
			balances["ledger"] = &balancePair{
				Before: strPtr(fixed4(before.Add(lienBefore), account.Currency)),
				After:  strPtr(fixed4(after.Add(lienAfter), account.Currency)),
			}
		}
	}

	ref := &accountRef{ID: bankmanID(account), AccountNumber: account.AccountNumber, Client: h.clientView(r, account)}
	if v := metaString(account, metaLegacyAccountNumber); v != nil {
		ref.AccountNumber = *v
	}
	view := &transactionView{
		ID:                tx.bankmanID(),
		InternalReference: tx.Reference,
		TransactionType:   txType,
		ProcessingChannel: strPtr(tx.Metadata["channel"]),
		Direction:         direction,
		Amount:            fixed4(amount, account.Currency),
		Currency:          account.Currency,
		Balances:          balances,
		Status:            "completed",
		Description:       strPtr(tx.Metadata["description"]),
		Metadata:          tx.Metadata,
		CreatedAt:         tx.Timestamp,
		ApprovedAt:        tx.Timestamp,
		IsReversal:        txType == typeReversal,
		IsReversed:        tx.Reverted,
		CanReverse:        !tx.Reverted && (txType == typeDeposit || txType == typeWithdrawal),
	}
	if direction == "debit" {
		view.SourceAccount = ref
	} else {
		view.DestinationAccount = ref
	}
	if orig := tx.Metadata[MetaReversalOf]; orig != "" {
		view.OriginalTransactionID = &orig
	}
	return view
}

func (h *Handler) clientView(r *http.Request, a *models.Account) *clientView {
	client, err := h.lookup.ClientByID(r.Context(), a.ClientID.String())
	if err != nil {
		return nil
	}
	cv := &clientView{ID: client.ID.String()}
	if v, ok := client.KYCData["legacy_source_id"].(string); ok && v != "" {
		cv.ID = v
	}
	if v, ok := client.KYCData["legacy_external_id"].(string); ok && v != "" {
		cv.ExternalID = &v
	}
	return cv
}

// accountOf finds the account a transaction belongs to from the metadata every wallet operation carries.
func (h *Handler) accountOf(r *http.Request, tx *ledgerTx) (*models.Account, error) {
	for _, k := range []string{"account_id", "bankman_account_id", "legacy_account_source_id"} {
		if v := tx.Metadata[k]; v != "" {
			return h.lookup.Account(r.Context(), v)
		}
	}
	return nil, services.ErrLegacyNotFound
}

// showTransaction answers GET /transactions/{id}.
func (h *Handler) showTransaction(w http.ResponseWriter, r *http.Request) {
	tx, err := h.findByBankmanID(r, chi.URLParam(r, "id"))
	if err != nil {
		fail(w, http.StatusInternalServerError, "Failed to retrieve transaction")
		return
	}
	if tx == nil {
		fail(w, http.StatusNotFound, "Transaction not found")
		return
	}
	account, err := h.accountOf(r, tx)
	if err != nil {
		fail(w, http.StatusNotFound, "Transaction not found")
		return
	}
	respond(w, http.StatusOK, "Success", h.transactionFor(r, tx, account))
}

// transactionStatus answers GET /transactions/{id}/status. Every LedgerTrack transaction is final, so it is
// always completed; there is no job or webhook behind it.
func (h *Handler) transactionStatus(w http.ResponseWriter, r *http.Request) {
	tx, err := h.findByBankmanID(r, chi.URLParam(r, "id"))
	if err != nil {
		fail(w, http.StatusInternalServerError, "Failed to retrieve transaction")
		return
	}
	if tx == nil {
		fail(w, http.StatusNotFound, "Transaction not found")
		return
	}
	respond(w, http.StatusOK, "Success", map[string]any{
		"transaction_id": tx.bankmanID(),
		"status":         "completed",
		"job_id":         nil,
		"webhook_url":    nil,
		"created_at":     tx.Timestamp,
		"updated_at":     tx.Timestamp,
	})
}

// Filters of Bankman's history that this layer cannot honour exactly. They are refused rather than ignored,
// because an ignored filter returns rows the caller asked to exclude.
var unsupportedHistoryFilters = []string{"transaction_type", "search", "external_id", "source_account_id", "destination_account_id"}

const maxHistoryPage = 50

// history answers GET /transactions/accounts/{account_id}/history.
func (h *Handler) history(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for _, f := range unsupportedHistoryFilters {
		if q.Get(f) != "" {
			fail(w, http.StatusUnprocessableEntity, "Filter not supported yet: "+f)
			return
		}
	}
	account, err := h.lookup.Account(r.Context(), chi.URLParam(r, "accountID"))
	if err != nil {
		h.lookupError(w, "Account not found", err)
		return
	}

	page := atoiDefault(q.Get("page"), 1)
	perPage := atoiDefault(q.Get("per_page"), 15)
	if page < 1 || perPage < 1 || perPage > 100 || page > maxHistoryPage {
		fail(w, http.StatusUnprocessableEntity, "Validation failed: page must be 1-"+strconv.Itoa(maxHistoryPage)+" and per_page 1-100")
		return
	}
	// status and currency can only match everything or nothing: every transaction is completed, and an account has
	// one currency.
	if (q.Get("status") != "" && q.Get("status") != "completed") || (q.Get("currency") != "" && !strings.EqualFold(q.Get("currency"), account.Currency)) {
		respond(w, http.StatusOK, "Success", historyPage([]*transactionView{}, page, perPage, false))
		return
	}

	params := url.Values{"pageSize": {strconv.Itoa(perPage)}}
	if v := q.Get("start_date"); v != "" {
		params.Set("startTime", v+"T00:00:00Z")
	}
	if v := q.Get("end_date"); v != "" {
		params.Set("endTime", v+"T23:59:59Z")
	}

	items, hasMore, err := h.walkCursor(r, h.accountPath(account, "/history"), params, page)
	if err != nil {
		fail(w, http.StatusInternalServerError, "Failed to retrieve transaction history")
		return
	}

	views := make([]*transactionView, 0, len(items))
	for i := range items {
		views = append(views, h.transactionFor(r, &items[i], account))
	}
	respond(w, http.StatusOK, "Success", historyPage(views, page, perPage, hasMore))
}

func atoiDefault(s string, d int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return d
}

// historyPage is Bankman's {transactions, pagination}. LedgerTrack pages by cursor and does not count the
// rows, so total and last_page are null.
func historyPage(views []*transactionView, page, perPage int, hasMore bool) map[string]any {
	var from, to any
	if len(views) > 0 {
		from, to = (page-1)*perPage+1, (page-1)*perPage+len(views)
	}
	return map[string]any{
		"transactions": views,
		"pagination": map[string]any{
			"current_page":   page,
			"per_page":       perPage,
			"total":          nil,
			"last_page":      nil,
			"from":           from,
			"to":             to,
			"has_more_pages": hasMore,
		},
	}
}

// walkCursor returns page N of a native cursor list by following N-1 cursors.
func (h *Handler) walkCursor(r *http.Request, path string, params url.Values, page int) ([]ledgerTx, bool, error) {
	var (
		items   []ledgerTx
		hasMore bool
		cursor  string
	)
	for i := 1; i <= page; i++ {
		p := params
		if cursor != "" {
			p = url.Values{"cursor": {cursor}}
		}
		rp, err := h.call(r, http.MethodGet, path+"?"+p.Encode(), nil)
		if err != nil {
			return nil, false, err
		}
		if !rp.ok() {
			return nil, false, errStatus(rp)
		}
		var out struct {
			Cursor struct {
				Data    []ledgerTx `json:"data"`
				HasMore bool       `json:"hasMore"`
				Next    string     `json:"next"`
			} `json:"cursor"`
		}
		if err := json.Unmarshal(rp.Body, &out); err != nil {
			return nil, false, err
		}
		items, hasMore, cursor = out.Cursor.Data, out.Cursor.HasMore, out.Cursor.Next
		if i < page && (!hasMore || cursor == "") {
			return nil, false, nil
		}
	}
	return items, hasMore, nil
}

// allHistory answers GET /transactions/accounts/all-history: every wallet transaction, newest first.
func (h *Handler) allHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for _, f := range unsupportedHistoryFilters {
		if q.Get(f) != "" {
			fail(w, http.StatusUnprocessableEntity, "Filter not supported yet: "+f)
			return
		}
	}
	page := atoiDefault(q.Get("page"), 1)
	perPage := atoiDefault(q.Get("per_page"), 15)
	if page < 1 || perPage < 1 || perPage > 100 || page > maxHistoryPage {
		fail(w, http.StatusUnprocessableEntity, "Validation failed: page must be 1-"+strconv.Itoa(maxHistoryPage)+" and per_page 1-100")
		return
	}
	if q.Get("status") != "" && q.Get("status") != "completed" {
		respond(w, http.StatusOK, "Success", historyPage([]*transactionView{}, page, perPage, false))
		return
	}
	// Every wallet operation touches the wallet's "available" address, so that pattern selects wallet
	// transactions and leaves out fees, settlement and other internal movements.
	currency := "" // any currency
	if c := strings.ToUpper(strings.TrimSpace(q.Get("currency"))); c != "" {
		currency = c
	}
	pattern := "users::wallets::available"
	if currency != "" {
		pattern = "users::wallets:" + currency + ":available"
	}
	params := url.Values{"pageSize": {strconv.Itoa(perPage)}, "account": {pattern}}
	if v := q.Get("start_date"); v != "" {
		params.Set("startTime", v+"T00:00:00Z")
	}
	if v := q.Get("end_date"); v != "" {
		params.Set("endTime", v+"T23:59:59Z")
	}
	items, hasMore, err := h.walkCursor(r, "/"+url.PathEscape(h.ledger)+"/transactions", params, page)
	if err != nil {
		fail(w, http.StatusInternalServerError, "Failed to retrieve transaction history")
		return
	}
	views := make([]*transactionView, 0, len(items))
	for i := range items {
		account, err := h.accountOf(r, &items[i])
		if err != nil {
			continue
		}
		views = append(views, h.transactionFor(r, &items[i], account))
	}
	respond(w, http.StatusOK, "Success", historyPage(views, page, perPage, hasMore))
}
