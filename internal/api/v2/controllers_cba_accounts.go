package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type accountBalanceResponse struct {
	AccountID uuid.UUID `json:"account_id"`
	WalletID  string    `json:"wallet_id"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
}

type accountWalletInfoResponse struct {
	WalletID         string `json:"wallet_id"`
	Currency         string `json:"currency"`
	AvailableBalance int64  `json:"available_balance"`
	AvailableAddress string `json:"available_address"`
	LienAddress      string `json:"lien_address"`
}

type accountDetailsResponse struct {
	models.Account
	WalletInfo accountWalletInfoResponse `json:"wallet_info"`
}

func openAccount(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		common.WithBody[services.OpenAccountInput](w, r, func(req services.OpenAccountInput) {
			account, err := accountService.Open(r.Context(), req)
			if err != nil {
				handleAccountError(w, r, err)
				return
			}

			if req.OpeningDeposit != "" {
				if err := creditOpeningDeposit(r.Context(), l, account, req.OpeningDeposit); err != nil {
					handleAccountError(w, r, err)
					return
				}
			}

			api.Created(w, account)
		})
	}
}

func listCBAAccounts(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter := repositories.AccountFilter{}
		if clientID := strings.TrimSpace(r.URL.Query().Get("client_id")); clientID != "" {
			id, err := uuid.Parse(clientID)
			if err != nil {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid client_id: %w", err))
				return
			}
			filter.ClientID = &id
		}
		if productID := strings.TrimSpace(r.URL.Query().Get("product_id")); productID != "" {
			id, err := uuid.Parse(productID)
			if err != nil {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid product_id: %w", err))
				return
			}
			filter.ProductID = &id
		}
		if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
			filter.Status = &status
		}

		accounts, err := accountService.List(r.Context(), filter)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"accounts": accounts,
		})
	}
}

func ledgerAwareAccountList(routerOptions routerOptions) http.HandlerFunc {
	coreHandler := listAccounts(routerOptions.paginationConfig)
	cbaHandler := listCBAAccounts(routerOptions.accountService)
	return func(w http.ResponseWriter, r *http.Request) {
		if chi.URLParam(r, "ledger") == "ledgertrack" {
			cbaHandler.ServeHTTP(w, r)
			return
		}
		coreHandler.ServeHTTP(w, r)
	}
}

func readCBAAccount(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		l := common.LedgerFromContext(r.Context())
		balance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}

		api.Ok(w, accountDetailsResponse{
			Account: *account,
			WalletInfo: accountWalletInfoResponse{
				WalletID:         account.WalletID,
				Currency:         account.Currency,
				AvailableBalance: balance,
				AvailableAddress: walletAvailableAddress(account.WalletID, account.Currency),
				LienAddress:      walletLienAddress(account.WalletID, account.Currency),
			},
		})
	}
}

func ledgerAwareAccountRead(accountService services.AccountService) http.HandlerFunc {
	coreHandler := http.HandlerFunc(readAccount)
	cbaHandler := readCBAAccount(accountService)
	return func(w http.ResponseWriter, r *http.Request) {
		if chi.URLParam(r, "ledger") != "ledgertrack" {
			coreHandler.ServeHTTP(w, r)
			return
		}
		if _, err := getCBAAccountID(r); err == nil {
			cbaHandler.ServeHTTP(w, r)
			return
		}
		coreHandler.ServeHTTP(w, r)
	}
}

func activateAccount(accountService services.AccountService) http.HandlerFunc {
	return lifecycleAccountHandler(accountService.Activate)
}

func suspendAccount(accountService services.AccountService) http.HandlerFunc {
	return lifecycleAccountHandler(accountService.Suspend)
}

func freezeAccount(accountService services.AccountService) http.HandlerFunc {
	return lifecycleAccountHandler(accountService.Freeze)
}

func dormantAccount(accountService services.AccountService) http.HandlerFunc {
	return lifecycleAccountHandler(accountService.Dormant)
}

func reactivateAccount(accountService services.AccountService) http.HandlerFunc {
	return lifecycleAccountHandler(accountService.Reactivate)
}

func closeAccount(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		l := common.LedgerFromContext(r.Context())
		balance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}

		account, err = accountService.Close(r.Context(), accountID, balance)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}
		api.Ok(w, account)
	}
}

func getAccountBalance(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		l := common.LedgerFromContext(r.Context())
		balance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}

		api.Ok(w, accountBalanceResponse{
			AccountID: account.ID,
			WalletID:  account.WalletID,
			Currency:  account.Currency,
			Balance:   balance,
		})
	}
}

func getAccountHistory(accountService services.AccountService) http.HandlerFunc {
	return transactionCursorForAccount(accountService, bunpaginate.OrderDesc)
}

func getAccountStatement(accountService services.AccountService) http.HandlerFunc {
	return transactionCursorForAccount(accountService, bunpaginate.OrderAsc)
}

func creditAccount(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		var req WalletTransactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		amount, err := parseAmount(req.Amount, account.Currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}

		l := common.LedgerFromContext(r.Context())
		currentBalance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		usageAt := time.Now().UTC()
		if _, err := accountService.ValidateCredit(r.Context(), accountID, amount, currentBalance, usageAt); err != nil {
			handleAccountError(w, r, err)
			return
		}

		accountUser := walletAvailableAddress(account.WalletID, account.Currency)
		accountSystem := systemControlAddress(account.WalletID, account.Currency)
		script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, account.Currency, amount, accountSystem, accountUser)

		params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  buildAccountMetadata(req.Metadata, account, "credit"),
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
			handleAccountError(w, r, err)
			return
		}
		if err := accountService.RecordCreditUsage(r.Context(), accountID, amount, req.Reference, usageAt); err != nil {
			handleAccountError(w, r, err)
			return
		}

		api.Created(w, accountTransactionResponse(tx.Transaction, accountUser, account.Currency))
	}
}

func debitAccount(accountService services.AccountService, sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		var req WalletTransactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		amount, err := parseAmount(req.Amount, account.Currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}
		channelAmount, err := parseAmount(req.ChannelAmount, account.Currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid channelAmount: %v", err))
			return
		}
		if req.ChannelID != "" {
			if channelAmount <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channelAmount must be positive"))
				return
			}
			if channelAmount > amount {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channel amount cannot exceed wallet debit amount"))
				return
			}
		}

		l := common.LedgerFromContext(r.Context())
		currentBalance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		usageAt := time.Now().UTC()
		if _, err := accountService.ValidateDebit(r.Context(), accountID, amount, currentBalance, usageAt); err != nil {
			handleAccountError(w, r, err)
			return
		}

		accountUser := walletAvailableAddress(account.WalletID, account.Currency)
		accountSystem := systemControlAddress(account.WalletID, account.Currency)
		script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s
			destination = @%s
		)
	`, account.Currency, amount, accountUser, accountSystem)

		params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  buildAccountMetadata(req.Metadata, account, "debit"),
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

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

		respMetadata := accountTransactionMetadata(tx.Transaction.Metadata)
		var warningMsg string
		if req.ChannelID != "" {
			channelLedgerName := fmt.Sprintf("channels-%s", account.Currency)
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
			`, account.Currency, channelAmount, channelAccount)
			cParams := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
				Input: ledgercontroller.CreateTransaction{
					RunScript: vm.RunScript{
						Script:    vm.Script{Plain: channelScript},
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

			if volumes, ok := cTx.Transaction.PostCommitVolumes[channelAccount]; ok {
				asset := fmt.Sprintf("%s/2", account.Currency)
				if vol, ok := volumes[asset]; ok {
					warningMsg = fmt.Sprintf("Channel balance: %s %s", vol.Balance().String(), account.Currency)
					if vol.Balance().Sign() < 0 {
						warningMsg = fmt.Sprintf("Channel balance is negative: %s %s", vol.Balance().String(), account.Currency)
					}
				}
			}

			revenue := amount - channelAmount
			if revenue > 0 {
				revenueLedgerName := fmt.Sprintf("revenue-%s", account.Currency)
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
				`, account.Currency, revenue)
				rParams := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
					Input: ledgercontroller.CreateTransaction{
						RunScript: vm.RunScript{
							Script:    vm.Script{Plain: revenueScript},
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

		if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
			handleAccountError(w, r, err)
			return
		}
		if err := accountService.RecordDebitUsage(r.Context(), accountID, amount, req.Reference, usageAt); err != nil {
			handleAccountError(w, r, err)
			return
		}

		response := accountTransactionResponse(tx.Transaction, accountUser, account.Currency)
		response["metadata"] = respMetadata
		if warningMsg != "" {
			response["warning"] = warningMsg
		}
		api.Created(w, response)
	}
}

func lienAccount(accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		var req WalletTransactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		amount, err := parseAmount(req.Amount, account.Currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}

		l := common.LedgerFromContext(r.Context())
		currentBalance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		usageAt := time.Now().UTC()
		if _, err := accountService.ValidateLien(r.Context(), accountID, amount, currentBalance, usageAt); err != nil {
			handleAccountError(w, r, err)
			return
		}

		accountAvailable := walletAvailableAddress(account.WalletID, account.Currency)
		accountLien := walletLienAddress(account.WalletID, account.Currency)
		script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s
			destination = @%s
		)
	`, account.Currency, amount, accountAvailable, accountLien)

		params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  buildAccountMetadata(req.Metadata, account, "lien"),
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
			handleAccountError(w, r, err)
			return
		}
		if err := accountService.RecordDebitUsage(r.Context(), accountID, amount, req.Reference, usageAt); err != nil {
			handleAccountError(w, r, err)
			return
		}
		api.Created(w, accountTransactionResponse(tx.Transaction, accountAvailable, account.Currency))
	}
}

func releaseAccountLien(accountService services.AccountService, sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		var req ReleaseLienRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		amount, err := parseAmount(req.Amount, account.Currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid amount: %v", err))
			return
		}
		if req.Reference == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("reference is required"))
			return
		}
		if amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount is required for release"))
			return
		}
		channelAmount, err := parseAmount(req.ChannelAmount, account.Currency)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid channelAmount: %v", err))
			return
		}
		if req.ChannelID != "" {
			if channelAmount <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channelAmount must be positive"))
				return
			}
			if channelAmount > amount {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("channel amount cannot exceed wallet debit amount"))
				return
			}
		}

		if _, err := accountService.ValidateRelease(r.Context(), accountID, req.Mode); err != nil {
			handleAccountError(w, r, err)
			return
		}

		l := common.LedgerFromContext(r.Context())
		accountAvailable := walletAvailableAddress(account.WalletID, account.Currency)
		accountLien := walletLienAddress(account.WalletID, account.Currency)

		var script string
		mode := strings.TrimSpace(req.Mode)
		if mode == "PAY" {
			script = fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s
					destination = @world
				)
			`, account.Currency, amount, accountLien)
		} else {
			script = fmt.Sprintf(`
				send [%s/2 %d] (
					source = @%s
					destination = @%s
				)
			`, account.Currency, amount, accountLien, accountAvailable)
		}

		params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  buildAccountMetadata(nil, account, "lien_release"),
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		_, tx, _, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		respMetadata := accountTransactionMetadata(tx.Transaction.Metadata)
		var warningMsg string
		if req.ChannelID != "" {
			channelLedgerName := fmt.Sprintf("channels-%s", account.Currency)
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
			`, account.Currency, channelAmount, channelAccount)
			cParams := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
				Input: ledgercontroller.CreateTransaction{
					RunScript: vm.RunScript{
						Script:    vm.Script{Plain: channelScript},
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
			if volumes, ok := cTx.Transaction.PostCommitVolumes[channelAccount]; ok {
				asset := fmt.Sprintf("%s/2", account.Currency)
				if vol, ok := volumes[asset]; ok && vol.Balance().Sign() < 0 {
					warningMsg = fmt.Sprintf("Channel balance is negative: %s %s", vol.Balance().String(), account.Currency)
				}
			}

			revenue := amount - channelAmount
			if revenue > 0 {
				revenueLedgerName := fmt.Sprintf("revenue-%s", account.Currency)
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
				`, account.Currency, revenue)
				rParams := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
					Input: ledgercontroller.CreateTransaction{
						RunScript: vm.RunScript{
							Script:    vm.Script{Plain: revenueScript},
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

		if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
			handleAccountError(w, r, err)
			return
		}

		response := accountTransactionResponse(tx.Transaction, accountAvailable, account.Currency)
		response["metadata"] = respMetadata
		if warningMsg != "" {
			response["warning"] = warningMsg
		}
		api.Created(w, response)
	}
}

func transactionCursorForAccount(accountService services.AccountService, order bunpaginate.Order) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := accountService.Get(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		accountAvailable := walletAvailableAddress(account.WalletID, account.Currency)
		accountLien := walletLienAddress(account.WalletID, account.Currency)

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

		pq, err := getPaginatedQuery[any](r, common.PaginationConfig{
			DefaultPageSize: 15,
			MaxPageSize:     100,
		}, "id", order, func(q *storagecommon.ResourceQuery[any]) {
			if q.Builder == nil {
				q.Builder = qb
			} else {
				q.Builder = query.And(q.Builder, qb)
			}
			if order == bunpaginate.OrderDesc {
				q.Expand = []string{"volumes"}
			}
		})
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		l := common.LedgerFromContext(r.Context())
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

func lifecycleAccountHandler(fn func(context.Context, uuid.UUID) (*models.Account, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := fn(r.Context(), accountID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}
		api.Ok(w, account)
	}
}

func accountTransactionResponse(tx ledgerinternal.Transaction, trackedAccount string, currency string) map[string]any {
	var balanceBefore, balanceAfter int64
	assetName := fmt.Sprintf("%s/2", currency)
	if vol, ok := tx.PostCommitVolumes[trackedAccount]; ok {
		if v, ok := vol[assetName]; ok {
			bal := new(big.Int).Sub(v.Input, v.Output)
			balanceAfter = bal.Int64()
		}
	}
	netChange := int64(0)
	for _, posting := range tx.Postings {
		if posting.Asset != assetName {
			continue
		}
		if posting.Destination == trackedAccount {
			netChange += posting.Amount.Int64()
		}
		if posting.Source == trackedAccount {
			netChange -= posting.Amount.Int64()
		}
	}
	balanceBefore = balanceAfter - netChange

	return map[string]any{
		"txid":           tx.ID,
		"timestamp":      tx.Timestamp,
		"postings":       tx.Postings,
		"metadata":       accountTransactionMetadata(tx.Metadata),
		"balance_before": balanceBefore,
		"balance_after":  balanceAfter,
	}
}

func buildAccountMetadata(input map[string]string, account *models.Account, operation string) metadata.Metadata {
	ret := metadata.Metadata{
		"cba_operation": operation,
		"account_id":    account.ID.String(),
		"wallet_id":     account.WalletID,
	}
	for key, value := range input {
		ret[key] = value
	}
	return ret
}

func accountTransactionMetadata(input map[string]string) map[string]string {
	ret := map[string]string{}
	for key, value := range input {
		ret[key] = value
	}
	return ret
}

func creditOpeningDeposit(ctx context.Context, l ledgercontroller.Controller, account *models.Account, openingDeposit json.Number) error {
	amount, err := parseAmount(openingDeposit, account.Currency)
	if err != nil {
		return fmt.Errorf("%w: invalid opening_deposit: %v", services.ErrAccountValidation, err)
	}
	if amount == 0 {
		return nil
	}

	accountUser := walletAvailableAddress(account.WalletID, account.Currency)
	accountSystem := fmt.Sprintf("system:control:%s", account.Currency)
	script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, account.Currency, amount, accountSystem, accountUser)

	params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
		Input: ledgercontroller.CreateTransaction{
			RunScript: vm.RunScript{
				Script: vm.Script{
					Plain: script,
				},
				Reference: fmt.Sprintf("account-opening-deposit-%s", account.ID),
				Metadata: metadata.Metadata{
					"cba_operation": "opening_deposit",
					"account_id":    account.ID.String(),
					"wallet_id":     account.WalletID,
				},
			},
			Runtime: ledgerinternal.RuntimeMachine,
		},
	}

	_, _, _, err = l.CreateTransaction(ctx, params)
	return err
}

func readAvailableBalance(ctx context.Context, l ledgercontroller.Controller, walletID, currency string) (int64, error) {
	address := walletAvailableAddress(walletID, currency)
	asset := fmt.Sprintf("%s/2", currency)
	var order bunpaginate.Order = bunpaginate.OrderAsc
	rq := storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
		Column:   "account",
		Order:    &order,
		PageSize: 10,
		Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
			Opts:    ledgerstore.GetVolumesOptions{},
			Builder: query.Match("account", address),
		},
	}

	cursor, err := l.GetVolumesWithBalances(ctx, rq)
	if err != nil {
		return 0, err
	}
	for _, row := range cursor.Data {
		if row.Account != address {
			continue
		}
		if row.Asset != asset {
			continue
		}
		if row.Balance == nil {
			return 0, nil
		}
		return row.Balance.Int64(), nil
	}
	return 0, nil
}

func walletAvailableAddress(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:available", walletID, currency)
}

func walletLienAddress(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:lien", walletID, currency)
}

func systemControlAddress(seed, currency string) string {
	shards := systemControlShards()
	if shards <= 1 {
		return fmt.Sprintf("system:control:%s", currency)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	return fmt.Sprintf("system:control:%s:%d", currency, h.Sum32()%shards)
}

func systemControlShards() uint32 {
	v := strings.TrimSpace(os.Getenv("CBA_SYSTEM_CONTROL_SHARDS"))
	if v == "" {
		return 1
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return 1
	}
	return uint32(n)
}

func ledgertrackOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if chi.URLParam(r, "ledger") != "ledgertrack" {
			api.NotFound(w, errors.New("not found"))
			return
		}
		next.ServeHTTP(w, r)
	}
}

func getCBAAccountID(r *http.Request) (uuid.UUID, error) {
	accountID := chi.URLParam(r, "accountID")
	if accountID == "" {
		accountID = chi.URLParam(r, "address")
	}
	if accountID == "" {
		return uuid.Nil, fmt.Errorf("accountID is required")
	}
	ret, err := uuid.Parse(accountID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid accountID: %w", err)
	}
	return ret, nil
}

func handleAccountError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrAccountValidation),
		errors.Is(err, services.ErrAccountInvalidStateTransition),
		errors.Is(err, services.ErrClientKYCRequirement),
		errors.Is(err, services.ErrClientValidation),
		errors.Is(err, services.ErrProductValidation),
		errors.Is(err, services.ErrClientNotFound),
		errors.Is(err, services.ErrProductNotFound):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrAccountAlreadyExists):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case errors.Is(err, services.ErrAccountNotFound):
		api.NotFound(w, err)
	default:
		common.InternalServerError(w, r, err)
	}
}
