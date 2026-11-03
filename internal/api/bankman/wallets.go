package bankman

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
)

// resolutionMessage is the constant Bankman answers with when the wallet is missing or not provably unique, so
// that a caller cannot tell the two apart.
const resolutionMessage = "wallet_selection_ambiguous_or_missing"

func role(a *models.Account) string {
	if a.WalletRole == nil {
		return "unknown"
	}
	return *a.WalletRole
}

func (h *Handler) walletError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, services.ErrWalletResolution):
		fail(w, http.StatusConflict, resolutionMessage)
	case errors.Is(err, services.ErrWalletRoleConflict):
		fail(w, http.StatusConflict, strings.TrimPrefix(err.Error(), services.ErrWalletRoleConflict.Error()+": "))
	case errors.Is(err, services.ErrWalletNotFound):
		fail(w, http.StatusNotFound, "Account not found")
	case errors.Is(err, services.ErrWalletValidation):
		fail(w, http.StatusUnprocessableEntity, strings.TrimPrefix(err.Error(), services.ErrWalletValidation.Error()+": "))
	default:
		fail(w, http.StatusInternalServerError, "Wallet request failed")
	}
}

// resolved answers the purpose-specific trusted-service shape, which has no message.
func resolved(w http.ResponseWriter, a *models.Account) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   map[string]any{"id": bankmanID(a), "currency": a.Currency, "wallet_role": role(a)},
	})
}

func (h *Handler) resolveMainWallet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	a, err := h.roles.ResolveMainWallet(r.Context(), q.Get("external_id"), q.Get("currency"))
	if err != nil {
		h.walletError(w, err)
		return
	}
	resolved(w, a)
}

func (h *Handler) resolveR2Subaccount(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	a, err := h.roles.ResolveR2Subaccount(r.Context(), q.Get("external_id"), q.Get("currency"), q.Get("r2_relationship_public_id"))
	if err != nil {
		h.walletError(w, err)
		return
	}
	resolved(w, a)
}

func walletData(a *models.Account) map[string]any {
	return map[string]any{
		"id": bankmanID(a), "currency": a.Currency, "wallet_role": role(a),
		"r2_relationship_public_id": a.R2RelationshipPublicID,
	}
}

func (h *Handler) listClassifications(w http.ResponseWriter, r *http.Request) {
	accounts, err := h.roles.List(r.Context(), r.URL.Query().Get("external_id"))
	if err != nil {
		h.walletError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(accounts))
	for i := range accounts {
		a := &accounts[i]
		d := walletData(a)
		d["account_type"] = a.AccountType
		d["status"] = a.Status
		out = append(out, d)
	}
	respond(w, http.StatusOK, "Wallet classifications retrieved", out)
}

func (h *Handler) showClassification(w http.ResponseWriter, r *http.Request) {
	a, err := h.roles.Show(r.Context(), chi.URLParam(r, "account"), r.URL.Query().Get("external_id"))
	if err != nil {
		h.walletError(w, err)
		return
	}
	d := walletData(a)
	d["status"] = a.Status
	respond(w, http.StatusOK, "Wallet classification retrieved", d)
}

func readJSON(r *http.Request, into any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, into)
}

func (h *Handler) classify(w http.ResponseWriter, r *http.Request) {
	var in services.ClassifyWalletInput
	if err := readJSON(r, &in); err != nil {
		fail(w, http.StatusUnprocessableEntity, "invalid JSON body")
		return
	}
	a, replayed, err := h.roles.Classify(r.Context(), chi.URLParam(r, "account"), in)
	if err != nil {
		h.walletError(w, err)
		return
	}
	msg := "Wallet classification saved"
	if replayed {
		msg = "Wallet classification retrieved"
	}
	respond(w, http.StatusOK, msg, walletData(a))
}

func (h *Handler) createR2Subaccount(w http.ResponseWriter, r *http.Request) {
	var in services.CreateR2SubaccountInput
	if err := readJSON(r, &in); err != nil {
		fail(w, http.StatusUnprocessableEntity, "invalid JSON body")
		return
	}
	a, replayed, err := h.roles.CreateR2Subaccount(r.Context(), in)
	if err != nil {
		h.walletError(w, err)
		return
	}
	if replayed {
		respond(w, http.StatusOK, "R2 subaccount wallet retrieved", walletData(a))
		return
	}
	respond(w, http.StatusCreated, "R2 subaccount wallet created", walletData(a))
}
