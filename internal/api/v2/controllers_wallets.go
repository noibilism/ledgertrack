package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"
	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
	"github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/go-chi/chi/v5"
)

// WalletController handles wallet operations
// It implements the "Wallet Wrapper" pattern defined in WalletPRD.md

type CreateWalletRequest struct {
	UserID   string `json:"userID"`
	Currency string `json:"currency"`
}

type WalletTransactionRequest struct {
	Amount        json.Number       `json:"amount"`
	Reference     string            `json:"reference"`
	Metadata      map[string]string `json:"metadata"`
	ChannelID     string            `json:"channelID"`
	ChannelAmount json.Number       `json:"channelAmount"`
}

type ReleaseLienRequest struct {
	Amount        json.Number `json:"amount"`
	Reference     string      `json:"reference"`
	Mode          string      `json:"mode"` // "release_only" or "release_and_debit"
	ChannelID     string      `json:"channelID"`
	ChannelAmount json.Number `json:"channelAmount"`
}

func postRevenueEntries(
	ctx context.Context,
	sys systemcontroller.Controller,
	currency string,
	baseReference string,
	userFeeAmount int64,
	processingFeeAmount int64,
) (string, *int64, error) {
	if userFeeAmount <= 0 && processingFeeAmount <= 0 {
		return "", nil, nil
	}

	revenueLedgerName := fmt.Sprintf("revenue-%s", currency)
	rl, err := sys.GetLedgerController(ctx, revenueLedgerName)
	if err != nil {
		return "", nil, err
	}

	entries := []struct {
		suffix      string
		destination string
		amount      int64
	}{
		{
			suffix:      "user-fee",
			destination: "revenue:accumulated",
			amount:      userFeeAmount,
		},
		{
			suffix:      "processing-fee",
			destination: "revenue:channel_processing_cost",
			amount:      processingFeeAmount,
		},
	}

	var revenueTxIDPtr *int64
	for _, entry := range entries {
		if entry.amount <= 0 {
			continue
		}

		revenueScript := fmt.Sprintf(`
			send [%s/2 %d] (
				source = @world
				destination = @%s
			)
		`, currency, entry.amount, entry.destination)

		rParams := ledger.Parameters[ledger.CreateTransaction]{
			Input: ledger.CreateTransaction{
				RunScript: vm.RunScript{
					Script: vm.Script{
						Plain: revenueScript,
					},
					Reference: fmt.Sprintf("%s-%s", baseReference, entry.suffix),
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}
		_, rTx, _, err := rl.CreateTransaction(ctx, rParams)
		if err != nil {
			return "", nil, err
		}
		if rTx.Transaction.ID != nil {
			x := int64(*rTx.Transaction.ID)
			revenueTxIDPtr = &x
		}
	}

	return revenueLedgerName, revenueTxIDPtr, nil
}

func listCurrencies() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		api.Ok(w, map[string]any{
			"currencies": currencyregistry.List(),
		})
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
		req.Currency = strings.ToUpper(req.Currency)
		reg, ok := currencyregistry.Lookup(req.Currency)
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

func parseAmount(amount json.Number, currency string) (int64, error) {
	if amount == "" {
		return 0, nil
	}

	reg, ok := currencyregistry.Lookup(currency)
	if !ok {
		reg = currencyregistry.Definition{Precision: 2, Enabled: true}
	}

	s := amount.String()
	if strings.Contains(s, ".") {
		f, err := amount.Float64()
		if err != nil {
			return 0, err
		}
		// Convert to atomic units
		mult := math.Pow(10, float64(reg.Precision))
		return int64(math.Round(f * mult)), nil
	}

	return amount.Int64()
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

		amount, err := parseAmount(req.Amount, currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}

		if amount <= 0 {
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
	`, currency, amount, accountSystem, accountUser)

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
			fmt.Fprintf(os.Stderr, "DEBUG ERROR: %v\n", err)
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// Calculate balances
		var balanceBefore, balanceAfter int64
		assetName := fmt.Sprintf("%s/2", currency)

		preCommitVolumes := tx.Transaction.PostCommitVolumes.SubtractPostings(tx.Transaction.Postings)

		if vol, ok := preCommitVolumes[accountUser]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceBefore = bal.Int64()
			}
		}
		if vol, ok := tx.Transaction.PostCommitVolumes[accountUser]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceAfter = bal.Int64()
			}
		}

		api.Created(w, map[string]interface{}{
			"txid":           tx.Transaction.ID,
			"timestamp":      tx.Transaction.Timestamp,
			"postings":       tx.Transaction.Postings,
			"metadata":       tx.Transaction.Metadata,
			"balance_before": balanceBefore,
			"balance_after":  balanceAfter,
		})
	}
}

func debitWallet(sys systemcontroller.Controller, channelFeeConfigService channelservices.ChannelFeeConfigService) http.HandlerFunc {
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

		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}

		// Validation for Multi-Ledger Logic
		channelAmount, err := parseAmount(req.ChannelAmount, currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid channelAmount: %v", err))
			return
		}

		if req.ChannelID != "" {
			if channelAmount <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channelAmount must be positive"))
				return
			}
		}

		amount, err := parseAmount(req.Amount, currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}

		var computedFees *channelservices.ComputedChannelFees
		if req.ChannelID != "" && channelFeeConfigService != nil {
			if amount <= 0 {
				computedFees, err = channelFeeConfigService.Compute(r.Context(), channelservices.ComputeChannelFeesRequest{
					ChannelID:       req.ChannelID,
					Currency:        currency,
					PrincipalAmount: channelAmount,
				})
				if err != nil {
					api.BadRequest(w, common.ErrValidation, err)
					return
				}
				amount = computedFees.TotalAmount
			} else {
				computedFees, err = channelFeeConfigService.Compute(r.Context(), channelservices.ComputeChannelFeesRequest{
					ChannelID:       req.ChannelID,
					Currency:        currency,
					PrincipalAmount: channelAmount,
					TotalAmount:     &amount,
				})
				if err != nil {
					api.BadRequest(w, common.ErrValidation, err)
					return
				}
			}
		}

		if amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount must be positive"))
			return
		}

		if req.ChannelID != "" && channelAmount > amount {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("channel amount cannot exceed wallet debit amount"))
			return
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
	`, currency, amount, accountUser, accountSystem)

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

		params.Input.RunScript.Metadata = metadata.Metadata{}
		for k, v := range req.Metadata {
			params.Input.RunScript.Metadata[k] = v
		}
		if req.ChannelID != "" {
			params.Input.RunScript.Metadata["channel_id"] = req.ChannelID
			params.Input.RunScript.Metadata["channel_amount"] = fmt.Sprintf("%d", channelAmount)
			if computedFees != nil {
				params.Input.RunScript.Metadata["channel_user_fee_amount"] = fmt.Sprintf("%d", computedFees.UserFeeAmount)
				params.Input.RunScript.Metadata["channel_processing_fee_amount"] = fmt.Sprintf("%d", computedFees.ProcessingFee)
				params.Input.RunScript.Metadata["channel_net_revenue_amount"] = fmt.Sprintf("%d", computedFees.NetRevenueAmount)
			}
		}

		// Store multi-ledger transaction links
		respMetadata := map[string]string{}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "insufficient fund") {
				api.WriteErrorResponse(w, http.StatusPaymentRequired, common.ErrInsufficientFund, err)
				return
			}
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// Calculate balances
		var balanceBefore, balanceAfter int64
		assetName := fmt.Sprintf("%s/2", currency)

		preCommitVolumes := tx.Transaction.PostCommitVolumes.SubtractPostings(tx.Transaction.Postings)

		if vol, ok := preCommitVolumes[accountUser]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceBefore = bal.Int64()
			}
		}
		if vol, ok := tx.Transaction.PostCommitVolumes[accountUser]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceAfter = bal.Int64()
			}
		}

		// 2. Handle Multi-Ledger (Channel & Revenue) if applicable
		var warningMsg string
		if req.ChannelID != "" {
			// Process Channel Debit
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
			`, currency, channelAmount, channelAccount)

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

			userFeeAmount := amount - channelAmount
			processingFeeAmount := int64(0)
			netRevenueAmount := userFeeAmount
			if computedFees != nil {
				userFeeAmount = computedFees.UserFeeAmount
				processingFeeAmount = computedFees.ProcessingFee
				netRevenueAmount = computedFees.NetRevenueAmount
			}

			respMetadata["channel_user_fee_amount"] = fmt.Sprintf("%d", userFeeAmount)
			respMetadata["channel_processing_fee_amount"] = fmt.Sprintf("%d", processingFeeAmount)
			respMetadata["channel_net_revenue_amount"] = fmt.Sprintf("%d", netRevenueAmount)

			// Process Revenue Credit
			if userFeeAmount > 0 || processingFeeAmount > 0 {
				revenueLedgerName, revenueTxIDPtr, err := postRevenueEntries(
					r.Context(),
					sys,
					currency,
					req.Reference,
					userFeeAmount,
					processingFeeAmount,
				)
				if err != nil {
					common.HandleCommonWriteErrors(w, r, err)
					return
				}
				respMetadata["revenue_ledger"] = revenueLedgerName
				if revenueTxIDPtr != nil {
					respMetadata["revenue_tx_id"] = fmt.Sprintf("%d", *revenueTxIDPtr)
				}

				if channelFeeConfigService != nil {
					walletIDCopy := walletID
					var ledgerTxIDPtr *int64
					if tx.Transaction.ID != nil {
						x := int64(*tx.Transaction.ID)
						ledgerTxIDPtr = &x
					}
					var channelTxIDPtr *int64
					if cTx.Transaction.ID != nil {
						x := int64(*cTx.Transaction.ID)
						channelTxIDPtr = &x
					}
					if err := channelFeeConfigService.Record(r.Context(), &channelmodels.ChannelFeeRecord{
						ChannelID:           req.ChannelID,
						Currency:            currency,
						WalletID:            &walletIDCopy,
						Reference:           req.Reference,
						LedgerTxID:          ledgerTxIDPtr,
						ChannelTxID:         channelTxIDPtr,
						RevenueTxID:         revenueTxIDPtr,
						OccurredAt:          time.Now().UTC(),
						TotalAmount:         amount,
						PrincipalAmount:     channelAmount,
						UserFeeAmount:       userFeeAmount,
						ProcessingFeeAmount: processingFeeAmount,
						NetRevenueAmount:    netRevenueAmount,
						Metadata: map[string]any{
							"wallet_id": walletID,
						},
					}); err != nil {
						warningMsg = fmt.Sprintf("fee record write failed: %s", err.Error())
					}
				}
			}
		}

		// Update Metadata in Response
		// Merge original metadata
		for k, v := range tx.Transaction.Metadata {
			respMetadata[k] = v
		}

		response := map[string]interface{}{
			"txid":           tx.Transaction.ID,
			"timestamp":      tx.Transaction.Timestamp,
			"postings":       tx.Transaction.Postings,
			"metadata":       respMetadata,
			"balance_before": balanceBefore,
			"balance_after":  balanceAfter,
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

		amount, err := parseAmount(req.Amount, currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}

		if amount <= 0 {
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
	`, currency, amount, accountAvailable, accountLien)

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
			if strings.Contains(strings.ToLower(err.Error()), "conflict") ||
				strings.Contains(strings.ToLower(err.Error()), "duplicate") ||
				strings.Contains(strings.ToLower(err.Error()), "already exists") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// Calculate balances
		var balanceBefore, balanceAfter int64
		assetName := fmt.Sprintf("%s/2", currency)

		preCommitVolumes := tx.Transaction.PostCommitVolumes.SubtractPostings(tx.Transaction.Postings)

		if vol, ok := preCommitVolumes[accountAvailable]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceBefore = bal.Int64()
			}
		}
		if vol, ok := tx.Transaction.PostCommitVolumes[accountAvailable]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceAfter = bal.Int64()
			}
		}

		api.Created(w, map[string]interface{}{
			"txid":           tx.Transaction.ID,
			"timestamp":      tx.Transaction.Timestamp,
			"postings":       tx.Transaction.Postings,
			"metadata":       tx.Transaction.Metadata,
			"balance_before": balanceBefore,
			"balance_after":  balanceAfter,
		})
	}
}

func releaseLien(sys systemcontroller.Controller, channelFeeConfigService channelservices.ChannelFeeConfigService) http.HandlerFunc {
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

		amount, err := parseAmount(req.Amount, currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}

		channelAmount, err := parseAmount(req.ChannelAmount, currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid channelAmount: %v", err))
			return
		}

		mode := strings.ToUpper(strings.TrimSpace(req.Mode))
		if mode == "" {
			mode = "RELEASE_ONLY"
		}
		if mode != "PAY" && mode != "RELEASE_ONLY" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid mode"))
			return
		}

		var computedFees *channelservices.ComputedChannelFees
		if req.ChannelID != "" {
			if channelAmount <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channelAmount must be positive"))
				return
			}

			if channelFeeConfigService != nil {
				if amount <= 0 {
					computedFees, err = channelFeeConfigService.Compute(r.Context(), channelservices.ComputeChannelFeesRequest{
						ChannelID:       req.ChannelID,
						Currency:        currency,
						PrincipalAmount: channelAmount,
					})
				} else {
					amountCopy := amount
					computedFees, err = channelFeeConfigService.Compute(r.Context(), channelservices.ComputeChannelFeesRequest{
						ChannelID:       req.ChannelID,
						Currency:        currency,
						PrincipalAmount: channelAmount,
						TotalAmount:     &amountCopy,
					})
				}
				if err != nil {
					switch {
					case strings.Contains(err.Error(), channelservices.ErrChannelFeesValidation.Error()):
						api.BadRequest(w, common.ErrValidation, err)
					case errors.Is(err, channelservices.ErrChannelFeesValidation):
						api.BadRequest(w, common.ErrValidation, err)
					case errors.Is(err, channelservices.ErrChannelFeesNotFound):
						api.NotFound(w, err)
					default:
						common.InternalServerError(w, r, err)
					}
					return
				}
				amount = computedFees.TotalAmount
			} else {
				if amount <= 0 {
					api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount is required for release"))
					return
				}
			}

			if amount < channelAmount {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channel amount cannot exceed wallet debit amount"))
				return
			}
		} else if amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount is required for release"))
			return
		}

		// Lien Release Logic
		accountLien := fmt.Sprintf("users:%s:wallets:%s:lien", userID, currency)

		var script string
		if mode == "PAY" {
			// Pay: Lien -> World (Spend)
			script = fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s
					destination = @world
				)
			`, currency, amount, accountLien)
		} else {
			// Release/Cancel: Lien -> Available
			accountAvailable := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
			script = fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s
					destination = @%s
				)
			`, currency, amount, accountLien, accountAvailable)
		}

		runMetadata := metadata.Metadata{}
		if req.ChannelID != "" && mode == "PAY" {
			runMetadata["channel_id"] = req.ChannelID
			runMetadata["channel_amount"] = fmt.Sprintf("%d", channelAmount)
			if computedFees != nil {
				runMetadata["channel_user_fee_amount"] = fmt.Sprintf("%d", computedFees.UserFeeAmount)
				runMetadata["channel_processing_fee_amount"] = fmt.Sprintf("%d", computedFees.ProcessingFee)
				runMetadata["channel_net_revenue_amount"] = fmt.Sprintf("%d", computedFees.NetRevenueAmount)
			} else {
				userFeeAmount := amount - channelAmount
				if userFeeAmount < 0 {
					userFeeAmount = 0
				}
				runMetadata["channel_user_fee_amount"] = fmt.Sprintf("%d", userFeeAmount)
				runMetadata["channel_processing_fee_amount"] = "0"
				runMetadata["channel_net_revenue_amount"] = fmt.Sprintf("%d", userFeeAmount)
			}
		}

		params := ledger.Parameters[ledger.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledger.CreateTransaction{
				RunScript: vm.RunScript{
					Script: vm.Script{
						Plain: script,
					},
					Reference: req.Reference,
					Metadata:  runMetadata,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		respMetadata := map[string]string{}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// Calculate balances
		var balanceBefore, balanceAfter int64
		assetName := fmt.Sprintf("%s/2", currency)

		preCommitVolumes := tx.Transaction.PostCommitVolumes.SubtractPostings(tx.Transaction.Postings)

		// For release, the relevant account depends on Mode.
		// If PAY, we might care about Lien balance?
		// If RELEASE, we care about Available (which got credited) or Lien (which got debited)?
		// User likely wants "Available Balance" of the wallet.
		// So we always track 'accountAvailable'.

		accountAvailable := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)

		if vol, ok := preCommitVolumes[accountAvailable]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceBefore = bal.Int64()
			}
		}
		if vol, ok := tx.Transaction.PostCommitVolumes[accountAvailable]; ok {
			if v, ok := vol[assetName]; ok {
				bal := new(big.Int).Sub(v.Input, v.Output)
				balanceAfter = bal.Int64()
			}
		}

		var warningMsg string
		if req.ChannelID != "" && mode == "PAY" {
			channelLedgerName := fmt.Sprintf("channels-%s", currency)
			cl, err := sys.GetLedgerController(r.Context(), channelLedgerName)
			if err != nil {
				common.HandleCommonWriteErrors(w, r, err)
				return
			}

			channelAccount := fmt.Sprintf("channel:%s", req.ChannelID)
			channelScript := fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s allowing unbounded overdraft
					destination = @world
				)
			`, currency, channelAmount, channelAccount)

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

			userFeeAmount := amount - channelAmount
			processingFeeAmount := int64(0)
			netRevenueAmount := userFeeAmount
			if computedFees != nil {
				userFeeAmount = computedFees.UserFeeAmount
				processingFeeAmount = computedFees.ProcessingFee
				netRevenueAmount = computedFees.NetRevenueAmount
			} else if userFeeAmount < 0 {
				userFeeAmount = 0
				netRevenueAmount = 0
			}

			respMetadata["channel_user_fee_amount"] = fmt.Sprintf("%d", userFeeAmount)
			respMetadata["channel_processing_fee_amount"] = fmt.Sprintf("%d", processingFeeAmount)
			respMetadata["channel_net_revenue_amount"] = fmt.Sprintf("%d", netRevenueAmount)

			if userFeeAmount > 0 || processingFeeAmount > 0 {
				revenueLedgerName, revenueTxIDPtr, err := postRevenueEntries(
					r.Context(),
					sys,
					currency,
					req.Reference,
					userFeeAmount,
					processingFeeAmount,
				)
				if err != nil {
					common.HandleCommonWriteErrors(w, r, err)
					return
				}
				respMetadata["revenue_ledger"] = revenueLedgerName
				if revenueTxIDPtr != nil {
					respMetadata["revenue_tx_id"] = fmt.Sprintf("%d", *revenueTxIDPtr)
				}

				if channelFeeConfigService != nil {
					walletIDCopy := walletID
					var ledgerTxIDPtr *int64
					if tx.Transaction.ID != nil {
						x := int64(*tx.Transaction.ID)
						ledgerTxIDPtr = &x
					}
					var channelTxIDPtr *int64
					if cTx.Transaction.ID != nil {
						x := int64(*cTx.Transaction.ID)
						channelTxIDPtr = &x
					}
					if err := channelFeeConfigService.Record(r.Context(), &channelmodels.ChannelFeeRecord{
						ChannelID:           req.ChannelID,
						Currency:            currency,
						WalletID:            &walletIDCopy,
						Reference:           req.Reference,
						LedgerTxID:          ledgerTxIDPtr,
						ChannelTxID:         channelTxIDPtr,
						RevenueTxID:         revenueTxIDPtr,
						OccurredAt:          time.Now().UTC(),
						TotalAmount:         amount,
						PrincipalAmount:     channelAmount,
						UserFeeAmount:       userFeeAmount,
						ProcessingFeeAmount: processingFeeAmount,
						NetRevenueAmount:    netRevenueAmount,
						Metadata: map[string]any{
							"wallet_id": walletID,
							"mode":      mode,
						},
					}); err != nil {
						warningMsg = fmt.Sprintf("fee record write failed: %s", err.Error())
					}
				}
			}
		}

		response := map[string]interface{}{
			"txid":           tx.Transaction.ID,
			"timestamp":      tx.Transaction.Timestamp,
			"postings":       tx.Transaction.Postings,
			"metadata":       respMetadata,
			"balance_before": balanceBefore,
			"balance_after":  balanceAfter,
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

		// Build Query
		// Filter by accounts: account = available OR account = lien
		var qb query.Builder = query.Or(
			query.Match("account", accountAvailable),
			query.Match("account", accountLien),
		)

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

			// Ensure volumes are expanded
			q.Expand = []string{"volumes"}
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

func getWalletBalances(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		userID := r.URL.Query().Get("userID")
		currency := r.URL.Query().Get("currency")

		if userID == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("userID is required"))
			return
		}

		// Pattern to match wallet accounts
		// Default: metadata[user_id]=userID AND metadata[type]=wallet (all currencies)
		// With currency: users:{userID}:wallets:{currency}:available

		// Collect addresses of interest
		interestedAccounts := []string{}

		if currency != "" {
			interestedAccounts = append(interestedAccounts, fmt.Sprintf("users:%s:wallets:%s:available", userID, currency))
		} else {
			// Iterate over all supported currencies
			for _, c := range currencyregistry.EnabledCodes() {
				interestedAccounts = append(interestedAccounts, fmt.Sprintf("users:%s:wallets:%s:available", userID, c))
			}
		}

		type Balance struct {
			Currency string `json:"currency"`
			Amount   int64  `json:"amount"`
		}

		balances := make([]Balance, 0)

		if len(interestedAccounts) > 0 {
			var addressMatches []query.Builder
			for _, acc := range interestedAccounts {
				addressMatches = append(addressMatches, query.Match("address", acc))
			}

			balancesQ := storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{
				Opts:    ledgerstore.GetAggregatedVolumesOptions{},
				Builder: query.Or(addressMatches...),
			}

			balancesMap, err := l.GetAggregatedBalances(r.Context(), balancesQ)
			if err != nil {
				common.HandleCommonErrors(w, r, err)
				return
			}

			for asset, amount := range balancesMap {
				// Strip precision suffix (e.g. USD/2 -> USD)
				assetName, _, _ := strings.Cut(asset, "/")

				var found bool
				for i := range balances {
					if balances[i].Currency == assetName {
						balances[i].Amount += amount.Int64()
						found = true
						break
					}
				}

				if !found {
					balances = append(balances, Balance{
						Currency: assetName,
						Amount:   amount.Int64(),
					})
				}
			}
		}

		api.Ok(w, map[string]interface{}{
			"balances": balances,
		})
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

		var qb query.Builder = query.Or(
			query.Match("account", accountAvailable),
			query.Match("account", accountLien),
		)

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
