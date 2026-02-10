package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"
	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/go-chi/chi/v5"
)

// WalletController handles wallet operations
// It implements the "Wallet Wrapper" pattern defined in WalletPRD.md

type CreateWalletRequest struct {
	UserID   string `json:"userID"`
	Currency string `json:"currency"`
}

type WalletTransactionRequest struct {
	Amount        int64             `json:"amount"`
	Reference     string            `json:"reference"`
	Metadata      map[string]string `json:"metadata"`
	ChannelID     string            `json:"channelID"`
	ChannelAmount int64             `json:"channelAmount"`
}

type ReleaseLienRequest struct {
	Amount        int64  `json:"amount"`
	Reference     string `json:"reference"`
	Mode          string `json:"mode"` // "release_only" or "release_and_debit"
	ChannelID     string `json:"channelID"`
	ChannelAmount int64  `json:"channelAmount"`
}

// CurrencyRegistry - hardcoded for now as per PRD "Currency Registry" requirement
var currencyRegistry = map[string]struct {
	Precision int
	Enabled   bool
}{
	"USD": {Precision: 2, Enabled: true},
	"EUR": {Precision: 2, Enabled: true},
	"BTC": {Precision: 8, Enabled: true},
	"NGN": {Precision: 2, Enabled: true},
	"GHS": {Precision: 2, Enabled: true},
	"KES": {Precision: 2, Enabled: true},
	"ZMW": {Precision: 2, Enabled: true},
}

func init() {
	if env := os.Getenv("ALLOWED_CURRENCIES"); env != "" {
		currencyRegistry = make(map[string]struct {
			Precision int
			Enabled   bool
		})
		parts := strings.Split(env, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			// Default precision 2
			currencyRegistry[strings.ToUpper(p)] = struct {
				Precision int
				Enabled   bool
			}{Precision: 2, Enabled: true}
		}
	}
}

func createWallet(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateWalletRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.UserID == "" || req.Currency == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("userID and currency are required"))
			return
		}

		// Validate currency
		reg, ok := currencyRegistry[req.Currency]
		if !ok || !reg.Enabled {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("currency %s not supported or disabled", req.Currency))
			return
		}

		// Deterministic Wallet ID
		walletID := fmt.Sprintf("%s-%s", req.UserID, req.Currency)

		api.Created(w, map[string]string{
			"walletID": walletID,
			"userID":   req.UserID,
			"currency": req.Currency,
		})
	}
}

func creditWallet(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		walletID := chi.URLParam(r, "walletID")

		// Parse walletID to get userID and currency
		lastDash := strings.LastIndex(walletID, "-")
		if lastDash == -1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid walletID format"))
			return
		}
		userID := walletID[:lastDash]
		currency := walletID[lastDash+1:]

		var req WalletTransactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.Amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount must be positive"))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}

		// Construct Transaction
		// Credit: users/{user_id}/wallets/{currency}/available
		// Debit: system/control/{currency}

		accountUser := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
		accountSystem := fmt.Sprintf("system:control:%s", currency)

		script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, currency, req.Amount, accountSystem, accountUser)

		params := ledger.Parameters[ledger.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledger.CreateTransaction{
				RunScript: vm.RunScript{
					Script: vm.Script{
						Plain: script,
					},
					Reference: req.Reference,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		if req.Metadata != nil {
			params.Input.RunScript.Metadata = metadata.Metadata{}
			for k, v := range req.Metadata {
				params.Input.RunScript.Metadata[k] = v
			}
		}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		api.Created(w, tx)
	}
}

func debitWallet(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		walletID := chi.URLParam(r, "walletID")

		lastDash := strings.LastIndex(walletID, "-")
		if lastDash == -1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid walletID format"))
			return
		}
		userID := walletID[:lastDash]
		currency := walletID[lastDash+1:]

		var req WalletTransactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.Amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount must be positive"))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}

		// Validation for Multi-Ledger Logic
		if req.ChannelID != "" {
			if req.ChannelAmount <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channelAmount must be positive"))
				return
			}
			if req.ChannelAmount > req.Amount {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channel amount cannot exceed wallet debit amount"))
				return
			}
		}

		// 1. Debit Wallet
		// Debit: users/{user_id}/wallets/{currency}/available
		// Credit: system/control/{currency}

		accountUser := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
		accountSystem := fmt.Sprintf("system:control:%s", currency)

		script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s
			destination = @%s
		)
	`, currency, req.Amount, accountUser, accountSystem)

		params := ledger.Parameters[ledger.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledger.CreateTransaction{
				RunScript: vm.RunScript{
					Script: vm.Script{
						Plain: script,
					},
					Reference: req.Reference,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		if req.Metadata != nil {
			params.Input.RunScript.Metadata = metadata.Metadata{}
			for k, v := range req.Metadata {
				params.Input.RunScript.Metadata[k] = v
			}
		} else {
			params.Input.RunScript.Metadata = metadata.Metadata{}
		}

		// Store multi-ledger transaction links
		respMetadata := map[string]string{}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// 2. Channel & Revenue Logic
		var warningMsg string
		if req.ChannelID != "" {
			// Process Channel Debit
			channelLedgerName := fmt.Sprintf("channels-%s", currency)
			cl, err := sys.GetLedgerController(r.Context(), channelLedgerName)
			if err != nil {
				// Failed to get channel ledger. Log error but don't fail main tx (already committed).
				// In strict ACID, this is bad. Here we return error but partial success.
				// Since we can't revert the wallet tx easily without more logic, we proceed.
				// Ideally we should return 500 or 207 Multi-Status.
				// For now, fail request (client should retry/investigate).
				// But wait, wallet tx IS committed.
				// Let's assume best effort and try to return success with error in metadata?
				// Or just return 500.
				// Prompt said: "System Error (Partial Failure) -> Response 500".
				common.HandleCommonWriteErrors(w, r, err)
				return
			}

			// Debit Channel: Channel -> World
			channelAccount := fmt.Sprintf("channel:%s", req.ChannelID)
			channelScript := fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s allowing unbounded overdraft
					destination = @world
				)
			`, currency, req.ChannelAmount, channelAccount)

			cParams := ledger.Parameters[ledger.CreateTransaction]{
				Input: ledger.CreateTransaction{
					RunScript: vm.RunScript{
						Script: vm.Script{
							Plain: channelScript,
						},
						Reference: req.Reference, // Same reference
					},
					Runtime: ledgerinternal.RuntimeMachine,
				},
			}
			_, cTx, _, err := cl.CreateTransaction(r.Context(), cParams)
			if err != nil {
				common.HandleCommonWriteErrors(w, r, err)
				return
			}
			respMetadata["channel_ledger"] = channelLedgerName
			respMetadata["channel_tx_id"] = fmt.Sprintf("%d", cTx.Transaction.ID)

			// Check Overdraft using PostCommitVolumes from the transaction result
			if volumes, ok := cTx.Transaction.PostCommitVolumes[channelAccount]; ok {
				asset := fmt.Sprintf("%s/2", currency)
				if vol, ok := volumes[asset]; ok {
					// ALWAYS return balance for debug
					warningMsg = fmt.Sprintf("Channel balance: %s %s", vol.Balance().String(), currency)
					if vol.Balance().Sign() < 0 {
						warningMsg = fmt.Sprintf("Channel balance is negative: %s %s", vol.Balance().String(), currency)
					}
				} else {
					// DEBUG
					warningMsg = fmt.Sprintf("DEBUG: Asset %s not found. Keys: %v", asset, volumes)
				}
			} else {
				// DEBUG
				warningMsg = fmt.Sprintf("DEBUG: Account %s not found. Keys: %v", channelAccount, cTx.Transaction.PostCommitVolumes)
			}

			// Process Revenue Credit
			revenue := req.Amount - req.ChannelAmount
			if revenue > 0 {
				revenueLedgerName := fmt.Sprintf("revenue-%s", currency)
				rl, err := sys.GetLedgerController(r.Context(), revenueLedgerName)
				if err != nil {
					common.HandleCommonWriteErrors(w, r, err)
					return
				}

				// Credit Revenue: World -> Revenue Accumulated
				revenueScript := fmt.Sprintf(`
					send [%s/2 %d] (
						source = @world
						destination = @revenue:accumulated
					)
				`, currency, revenue)

				rParams := ledger.Parameters[ledger.CreateTransaction]{
					Input: ledger.CreateTransaction{
						RunScript: vm.RunScript{
							Script: vm.Script{
								Plain: revenueScript,
							},
							Reference: req.Reference,
						},
						Runtime: ledgerinternal.RuntimeMachine,
					},
				}
				_, rTx, _, err := rl.CreateTransaction(r.Context(), rParams)
				if err != nil {
					common.HandleCommonWriteErrors(w, r, err)
					return
				}
				respMetadata["revenue_ledger"] = revenueLedgerName
				respMetadata["revenue_tx_id"] = fmt.Sprintf("%d", rTx.Transaction.ID)
			}
		}

		// Update Metadata in Response (we can't easily update the Tx object itself without saving metadata back to ledger)
		// But the user wants the RESPONSE to contain this metadata.
		// So we construct the response manually.

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"txid":      tx.Transaction.ID,
				"timestamp": tx.Transaction.Timestamp,
				"postings":  tx.Transaction.Postings,
				"metadata":  respMetadata,
			},
		}

		// Merge original metadata
		for k, v := range tx.Transaction.Metadata {
			respMetadata[k] = v
		}

		if warningMsg != "" {
			response["warning"] = warningMsg
		}

		api.Created(w, response)
	}
}

func lienWallet(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		walletID := chi.URLParam(r, "walletID")

		lastDash := strings.LastIndex(walletID, "-")
		if lastDash == -1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid walletID format"))
			return
		}
		userID := walletID[:lastDash]
		currency := walletID[lastDash+1:]

		var req WalletTransactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.Amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount must be positive"))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}

		// Lien:
		// Debit: users/{user_id}/wallets/{currency}/available
		// Credit: users/{user_id}/wallets/{currency}/lien

		accountAvailable := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
		accountLien := fmt.Sprintf("users:%s:wallets:%s:lien", userID, currency)

		script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s
			destination = @%s
		)
	`, currency, req.Amount, accountAvailable, accountLien)

		params := ledger.Parameters[ledger.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledger.CreateTransaction{
				RunScript: vm.RunScript{
					Script: vm.Script{
						Plain: script,
					},
					Reference: req.Reference,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		api.Created(w, tx)
	}
}

func releaseLien(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		walletID := chi.URLParam(r, "walletID")

		lastDash := strings.LastIndex(walletID, "-")
		if lastDash == -1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid walletID format"))
			return
		}
		userID := walletID[:lastDash]
		currency := walletID[lastDash+1:]

		var req ReleaseLienRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}
		if req.Amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount is required for release"))
			return
		}

		// Validation for Multi-Ledger Logic
		if req.ChannelID != "" {
			if req.ChannelAmount <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channelAmount must be positive"))
				return
			}
			if req.ChannelAmount > req.Amount {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channel amount cannot exceed wallet debit amount"))
				return
			}
		}

		// Lien Release Logic
		accountLien := fmt.Sprintf("users:%s:wallets:%s:lien", userID, currency)

		var script string
		if req.Mode == "PAY" {
			// Pay: Lien -> World (Spend)
			script = fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s
					destination = @world
				)
			`, currency, req.Amount, accountLien)
		} else {
			// Release/Cancel: Lien -> Available
			accountAvailable := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
			script = fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s
					destination = @%s
				)
			`, currency, req.Amount, accountLien, accountAvailable)
		}

		params := ledger.Parameters[ledger.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledger.CreateTransaction{
				RunScript: vm.RunScript{
					Script: vm.Script{
						Plain: script,
					},
					Reference: req.Reference, // Reusing reference might cause conflict if not handled?
					// Actually ReleaseLien usually needs a NEW reference for the release tx.
					// But we only have 'reference' in input.
					// Let's assume input reference is the Lien Reference, but we need a new reference for this TX.
					// Or maybe we use "release-" + reference?
					// Prompt samples used "lien-ref".
					// Ledger CreateTransaction checks unique reference.
					// If we reuse "lien-ref", it will fail if it's the same ledger.
					// Let's append suffix if needed, or assume the user provided a UNIQUE reference for the release action.
					// The sample payload: "reference": "lien-ref".
					// If the original lien creation used "lien-ref", this will fail.
					// I'll assume "reference" here is the ID of the release transaction.
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		respMetadata := map[string]string{}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// Channel & Revenue Logic (Only on PAY mode?)
		// Prompt says "debit the channels ledger for every release".
		// I'll assume primarily for PAY. If CANCEL, we probably shouldn't charge channel?
		// But strict requirement "every release".
		// I'll do it if ChannelID is present.

		var warningMsg string
		if req.ChannelID != "" {
			channelLedgerName := fmt.Sprintf("channels-%s", currency)
			cl, err := sys.GetLedgerController(r.Context(), channelLedgerName)
			if err != nil {
				common.HandleCommonWriteErrors(w, r, err)
				return
			}

			// Debit Channel: Channel -> World
			channelAccount := fmt.Sprintf("channel:%s", req.ChannelID)
			channelScript := fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s allowing unbounded overdraft
					destination = @world
				)
			`, currency, req.ChannelAmount, channelAccount)

			cParams := ledger.Parameters[ledger.CreateTransaction]{
				Input: ledger.CreateTransaction{
					RunScript: vm.RunScript{
						Script: vm.Script{
							Plain: channelScript,
						},
						Reference: req.Reference,
					},
					Runtime: ledgerinternal.RuntimeMachine,
				},
			}
			_, cTx, _, err := cl.CreateTransaction(r.Context(), cParams)
			if err != nil {
				common.HandleCommonWriteErrors(w, r, err)
				return
			}
			respMetadata["channel_ledger"] = channelLedgerName
			respMetadata["channel_tx_id"] = fmt.Sprintf("%d", cTx.Transaction.ID)

			// Check Overdraft using PostCommitVolumes from the transaction result
			if volumes, ok := cTx.Transaction.PostCommitVolumes[channelAccount]; ok {
				asset := fmt.Sprintf("%s/2", currency)
				if vol, ok := volumes[asset]; ok {
					if vol.Balance().Sign() < 0 {
						warningMsg = fmt.Sprintf("Channel balance is negative: %s %s", vol.Balance().String(), currency)
					}
				}
			}

			// Revenue Logic
			revenue := req.Amount - req.ChannelAmount
			if revenue > 0 {
				revenueLedgerName := fmt.Sprintf("revenue-%s", currency)
				rl, err := sys.GetLedgerController(r.Context(), revenueLedgerName)
				if err != nil {
					common.HandleCommonWriteErrors(w, r, err)
					return
				}

				revenueScript := fmt.Sprintf(`
					send [%s/2 %d] (
						source = @world
						destination = @revenue:accumulated
					)
				`, currency, revenue)

				rParams := ledger.Parameters[ledger.CreateTransaction]{
					Input: ledger.CreateTransaction{
						RunScript: vm.RunScript{
							Script: vm.Script{
								Plain: revenueScript,
							},
							Reference: req.Reference,
						},
						Runtime: ledgerinternal.RuntimeMachine,
					},
				}
				_, rTx, _, err := rl.CreateTransaction(r.Context(), rParams)
				if err != nil {
					common.HandleCommonWriteErrors(w, r, err)
					return
				}
				respMetadata["revenue_ledger"] = revenueLedgerName
				respMetadata["revenue_tx_id"] = fmt.Sprintf("%d", rTx.Transaction.ID)
			}
		}

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"txid":      tx.Transaction.ID,
				"timestamp": tx.Transaction.Timestamp,
				"postings":  tx.Transaction.Postings,
				"metadata":  respMetadata,
			},
		}
		for k, v := range tx.Transaction.Metadata {
			respMetadata[k] = v
		}

		if warningMsg != "" {
			response["warning"] = warningMsg
		}

		api.Created(w, response)
	}
}

func getWalletHistory(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		walletID := chi.URLParam(r, "walletID")

		lastDash := strings.LastIndex(walletID, "-")
		if lastDash == -1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid walletID format"))
			return
		}
		userID := walletID[:lastDash]
		currency := walletID[lastDash+1:]

		// Define accounts associated with the wallet
		accountAvailable := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
		accountLien := fmt.Sprintf("users:%s:wallets:%s:lien", userID, currency)
		accounts := []interface{}{accountAvailable, accountLien}

		// Build Query
		// Start with filtering by accounts
		var qb query.Builder = query.Match("account", accounts)

		// Add optional filters
		if reference := r.URL.Query().Get("reference"); reference != "" {
			qb = query.And(qb, query.Match("reference", reference))
		}
		if startTime := r.URL.Query().Get("startTime"); startTime != "" {
			qb = query.And(qb, query.Gte("timestamp", startTime))
		}
		if endTime := r.URL.Query().Get("endTime"); endTime != "" {
			qb = query.And(qb, query.Lte("timestamp", endTime))
		}

		// Using getPaginatedQuery helper which handles cursor decoding and page size
		pq, err := getPaginatedQuery[any](r, common.PaginationConfig{
			DefaultPageSize: 15,
			MaxPageSize:     100,
		}, "id", bunpaginate.OrderDesc, func(q *storagecommon.ResourceQuery[any]) {
			// Append our mandatory wallet filters
			if q.Builder == nil {
				q.Builder = qb
			} else {
				q.Builder = query.And(q.Builder, qb)
			}
		})
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListTransactions(r.Context(), pq)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(tx ledgerinternal.Transaction) any {
			return renderTransaction(r, tx)
		}))
	}
}

func getWalletStatement(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		walletID := chi.URLParam(r, "walletID")

		lastDash := strings.LastIndex(walletID, "-")
		if lastDash == -1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid walletID format"))
			return
		}
		userID := walletID[:lastDash]
		currency := walletID[lastDash+1:]

		accountAvailable := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
		accountLien := fmt.Sprintf("users:%s:wallets:%s:lien", userID, currency)
		accounts := []interface{}{accountAvailable, accountLien}

		var qb query.Builder = query.Match("account", accounts)

		if reference := r.URL.Query().Get("reference"); reference != "" {
			qb = query.And(qb, query.Match("reference", reference))
		}
		if startTime := r.URL.Query().Get("startTime"); startTime != "" {
			qb = query.And(qb, query.Gte("timestamp", startTime))
		}
		if endTime := r.URL.Query().Get("endTime"); endTime != "" {
			qb = query.And(qb, query.Lte("timestamp", endTime))
		}

		// Force Order Ascending for Statement
		pq, err := getPaginatedQuery[any](r, common.PaginationConfig{
			DefaultPageSize: 15,
			MaxPageSize:     100,
		}, "id", bunpaginate.OrderAsc, func(q *storagecommon.ResourceQuery[any]) {
			if q.Builder == nil {
				q.Builder = qb
			} else {
				q.Builder = query.And(q.Builder, qb)
			}
		})
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListTransactions(r.Context(), pq)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(tx ledgerinternal.Transaction) any {
			return renderTransaction(r, tx)
		}))
	}
}
