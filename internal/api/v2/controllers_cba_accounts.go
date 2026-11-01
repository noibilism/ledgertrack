package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
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
	currencyregistry "github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/hotrows"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/walletaddr"
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

func creditAccount(accountService services.AccountService, holds services.ReserveHoldService, charges services.ChargeService) http.HandlerFunc {
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
		if _, err := accountService.ValidateCredit(r.Context(), accountID, amount, currentBalance, usageAt); err != nil && !isIdempotentRetry(r.Context(), l, r.Header.Get("Idempotency-Key")) {
			handleAccountError(w, r, err)
			return
		}

		asset, err := currencyregistry.AssetFor(account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		accountUser := walletAvailableAddress(account.WalletID, account.Currency)
		accountSystem := systemControlAddress(account.WalletID, account.Currency)
		accountMetadata := buildAccountMetadata(req.Metadata, account, "credit")

		// With charge_fees the fee and tax come out of the credit: the wallet receives the amount less
		// the charge, and the control account pays the charge into the GL. The reserve then applies to
		// what the wallet actually receives.
		charge, err := planCharge(r, charges, req.ChargeFees, services.ChargeRequest{
			AccountID: accountID, Event: "credit", AmountMinor: amount, Ledger: chi.URLParam(r, "ledger"),
		})
		if err != nil {
			handleChargeError(w, r, err)
			return
		}
		received := amount
		if charge != nil {
			received = amount - charge.GrossMinor
			if received <= 0 {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("the fee and tax (%d) are not less than the credit (%d)", charge.GrossMinor, amount))
				return
			}
			accountMetadata = withChargeMetadata(accountMetadata, charge)
		}
		script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, asset, received, accountSystem, accountUser)

		// A reserve rule holds a share of the credit in the wallet lien (see creditWallet).
		var plan *services.CreditPlan
		if holds != nil {
			plan, err = holds.PlanCredit(r.Context(), services.PlanCreditRequest{
				Ledger: reserveLedgerName, WalletID: account.WalletID, Currency: account.Currency,
				Reference: req.Reference, AmountMinor: received, CreditType: req.CreditType,
			})
			if err != nil {
				handleReserveError(w, r, err)
				return
			}
		}
		if plan != nil {
			script = reserveCreditScript(asset, accountSystem, accountUser, walletaddr.Lien(account.WalletID, account.Currency), true, plan)
			accountMetadata = withReserveMetadata(accountMetadata, plan)
		}
		if charge != nil {
			script += chargeSends(asset, accountSystem, charge, true)
		}

		params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  accountMetadata,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		_, tx, replayed, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// A replayed request already counted: do not touch activity or usage twice.
		if !replayed {
			if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
				handleAccountError(w, r, err)
				return
			}
			if err := accountService.RecordCreditUsage(r.Context(), accountID, amount, req.Reference, usageAt); err != nil {
				handleAccountError(w, r, err)
				return
			}
		}

		response := accountTransactionResponse(tx.Transaction, accountUser, asset)
		if plan != nil {
			confirmReserveHold(r.Context(), holds, plan, tx)
			response["reserve"] = reserveResponse(plan)
		}
		if charge != nil {
			response["charges"] = chargeResponse(charge)
			response["received_minor"] = received
		}
		api.Created(w, response)
	}
}

func debitAccount(accountService services.AccountService, sys systemcontroller.Controller, holds services.ReserveHoldService, charges services.ChargeService) http.HandlerFunc {
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

		// With charge_fees the customer also pays the fee and tax of the account's schedule, posted to
		// the GL in the same transaction.
		if req.ChargeFees && req.ChannelID != "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("charge_fees cannot be combined with a channel"))
			return
		}
		charge, err := planCharge(r, charges, req.ChargeFees, services.ChargeRequest{
			AccountID: accountID, Event: "debit", AmountMinor: amount, Ledger: chi.URLParam(r, "ledger"),
		})
		if err != nil {
			handleChargeError(w, r, err)
			return
		}
		var chargeMinor int64
		if charge != nil {
			chargeMinor = charge.GrossMinor
		}

		// A chargeback asks to spend the held reserve first (see debitWallet).
		var drawPlan *services.DrawPlan
		if req.DrawFromReserve {
			if req.ChannelID != "" {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("draw_from_reserve cannot be combined with a channel"))
				return
			}
			if holds != nil && chi.URLParam(r, "ledger") == reserveLedgerName {
				drawPlan, err = holds.DrawReserve(r.Context(), services.DrawRequest{
					Ledger: reserveLedgerName, WalletID: account.WalletID, Reference: req.Reference, AmountMinor: amount,
				})
				if err != nil {
					handleReserveError(w, r, err)
					return
				}
			}
		}
		// Anything that stops the debit before it is posted gives the held shares back.
		giveBack := func() {
			if drawPlan != nil {
				_ = holds.RevertDraw(r.Context(), reserveLedgerName, account.WalletID, req.Reference)
			}
		}

		l := common.LedgerFromContext(r.Context())
		currentBalance, err := readAvailableBalance(r.Context(), l, account.WalletID, account.Currency)
		if err != nil {
			giveBack()
			common.HandleCommonErrors(w, r, err)
			return
		}
		if drawPlan != nil {
			// the held shares are funds this debit can spend
			currentBalance += drawPlan.DrawnMinor
		}
		usageAt := time.Now().UTC()
		if _, err := accountService.ValidateDebit(r.Context(), accountID, amount+chargeMinor, currentBalance, usageAt); err != nil && !isIdempotentRetry(r.Context(), l, r.Header.Get("Idempotency-Key")) {
			giveBack()
			handleAccountError(w, r, err)
			return
		}

		asset, err := currencyregistry.AssetFor(account.Currency)
		if err != nil {
			giveBack()
			common.HandleCommonErrors(w, r, err)
			return
		}
		accountUser := walletAvailableAddress(account.WalletID, account.Currency)
		accountSystem := systemControlAddress(account.WalletID, account.Currency)
		script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, asset, amount, accountUser, accountSystem)
		accountMetadata := buildAccountMetadata(req.Metadata, account, "debit")
		if drawPlan != nil {
			script = reserveDebitScript(asset, accountUser, walletaddr.Lien(account.WalletID, account.Currency), accountSystem, drawPlan)
			accountMetadata = withDrawMetadata(accountMetadata, drawPlan)
		}
		if charge != nil {
			script += chargeSends(asset, accountUser, charge, false)
			accountMetadata = withChargeMetadata(accountMetadata, charge)
		}

		params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  accountMetadata,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		}

		_, tx, replayed, err := l.CreateTransaction(r.Context(), params)
		if drawPlan != nil {
			settleReserveDraw(r.Context(), holds, reserveLedgerName, account.WalletID, req.Reference, err)
		}
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
				send [%s %d] (
					source = @%s allowing unbounded overdraft
					destination = @%s
				)
			`, asset, channelAmount, channelAccount, hotrows.ChannelSink(req.Reference))
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
					send [%s %d] (
						source = %s
						destination = @%s
					)
				`, asset, revenue, hotrows.SourceClause(hotrows.RevenueSource(req.Reference)), hotrows.RevenueDestination(hotrows.AccumulatedBase, req.Reference))
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

		// A replayed request already counted: do not touch activity or usage twice.
		if !replayed {
			if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
				handleAccountError(w, r, err)
				return
			}
			if err := accountService.RecordDebitUsage(r.Context(), accountID, amount, req.Reference, usageAt); err != nil {
				handleAccountError(w, r, err)
				return
			}
		}

		response := accountTransactionResponse(tx.Transaction, accountUser, asset)
		response["metadata"] = respMetadata
		if drawPlan != nil {
			response["reserve"] = drawResponse(drawPlan)
		}
		if charge != nil {
			response["charges"] = chargeResponse(charge)
		}
		if warningMsg != "" {
			response["warning"] = warningMsg
		}
		api.Created(w, response)
	}
}

// AccountTransferRequest moves money between two accounts of the same currency in one ledger
// transaction.
type AccountTransferRequest struct {
	DestinationAccountID string      `json:"destinationAccountID"`
	Amount               json.Number `json:"amount"`
	Reference            string      `json:"reference"`
	// CreditType is how the destination sees the credit for reserve rules. Default "transfer".
	CreditType string `json:"credit_type,omitempty"`
	// ChargeFees makes the source also pay the fee and tax of its schedule for event "transfer".
	ChargeFees bool              `json:"charge_fees,omitempty"`
	Metadata   map[string]string `json:"metadata"`
}

// transferAccount debits {accountID} and credits the destination account atomically. Both
// sides go through the same checks as a separate debit and credit, and a single posting moves
// the money, so a failure can never leave it half-way.
func transferAccount(accountService services.AccountService, holds services.ReserveHoldService, charges services.ChargeService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sourceID, err := getCBAAccountID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		source, err := accountService.Get(r.Context(), sourceID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}

		var req AccountTransferRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		destinationID, err := uuid.Parse(strings.TrimSpace(req.DestinationAccountID))
		if err != nil {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("destinationAccountID must be an account id"))
			return
		}
		if destinationID == sourceID {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("source and destination accounts must differ"))
			return
		}
		destination, err := accountService.Get(r.Context(), destinationID)
		if err != nil {
			handleAccountError(w, r, err)
			return
		}
		if destination.Currency != source.Currency {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("accounts must have the same currency (%s and %s)", source.Currency, destination.Currency))
			return
		}
		amount, err := parseAmount(req.Amount, source.Currency)
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

		l := common.LedgerFromContext(r.Context())
		idempotencyKey := r.Header.Get("Idempotency-Key")
		sourceBalance, err := readAvailableBalance(r.Context(), l, source.WalletID, source.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		destinationBalance, err := readAvailableBalance(r.Context(), l, destination.WalletID, destination.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		usageAt := time.Now().UTC()
		charge, err := planCharge(r, charges, req.ChargeFees, services.ChargeRequest{
			AccountID: sourceID, Event: "transfer", AmountMinor: amount, Ledger: chi.URLParam(r, "ledger"),
		})
		if err != nil {
			handleChargeError(w, r, err)
			return
		}
		var chargeMinor int64
		if charge != nil {
			chargeMinor = charge.GrossMinor
		}
		if _, err := accountService.ValidateDebit(r.Context(), sourceID, amount+chargeMinor, sourceBalance, usageAt); err != nil && !isIdempotentRetry(r.Context(), l, idempotencyKey) {
			handleAccountError(w, r, err)
			return
		}
		if _, err := accountService.ValidateCredit(r.Context(), destinationID, amount, destinationBalance, usageAt); err != nil && !isIdempotentRetry(r.Context(), l, idempotencyKey) {
			handleAccountError(w, r, err)
			return
		}

		asset, err := currencyregistry.AssetFor(source.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		sourceAddress := walletAvailableAddress(source.WalletID, source.Currency)
		destinationAddress := walletAvailableAddress(destination.WalletID, destination.Currency)
		script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, asset, amount, sourceAddress, destinationAddress)

		md := buildAccountMetadata(req.Metadata, source, "transfer")
		md["destination_account_id"] = destination.ID.String()
		md["destination_wallet_id"] = destination.WalletID

		// The destination is credited, so a reserve rule on it holds its share in the lien. The
		// source is debited in full either way and the money still moves in one transaction.
		var plan *services.CreditPlan
		if holds != nil && chi.URLParam(r, "ledger") == reserveLedgerName {
			creditType := strings.TrimSpace(req.CreditType)
			if creditType == "" {
				creditType = "transfer"
			}
			plan, err = holds.PlanCredit(r.Context(), services.PlanCreditRequest{
				Ledger: reserveLedgerName, WalletID: destination.WalletID, Currency: destination.Currency,
				Reference: req.Reference, AmountMinor: amount, CreditType: creditType,
			})
			if err != nil {
				handleReserveError(w, r, err)
				return
			}
		}
		if plan != nil {
			script = reserveCreditScript(asset, sourceAddress, destinationAddress,
				walletaddr.Lien(destination.WalletID, destination.Currency), false, plan)
			md = withReserveMetadata(md, plan)
		}
		if charge != nil {
			script += chargeSends(asset, sourceAddress, charge, false)
			md = withChargeMetadata(md, charge)
		}
		_, tx, replayed, err := l.CreateTransaction(r.Context(), ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
			IdempotencyKey: idempotencyKey,
			Input: ledgercontroller.CreateTransaction{
				RunScript: vm.RunScript{
					Script:    vm.Script{Plain: script},
					Reference: req.Reference,
					Metadata:  md,
				},
				Runtime: ledgerinternal.RuntimeMachine,
			},
		})
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

		if !replayed {
			at := tx.Transaction.Timestamp.Time
			if _, err := accountService.TouchActivity(r.Context(), sourceID, at); err != nil {
				handleAccountError(w, r, err)
				return
			}
			if _, err := accountService.TouchActivity(r.Context(), destinationID, at); err != nil {
				handleAccountError(w, r, err)
				return
			}
			if err := accountService.RecordDebitUsage(r.Context(), sourceID, amount, req.Reference, usageAt); err != nil {
				handleAccountError(w, r, err)
				return
			}
			if err := accountService.RecordCreditUsage(r.Context(), destinationID, amount, req.Reference, usageAt); err != nil {
				handleAccountError(w, r, err)
				return
			}
		}

		if plan != nil {
			confirmReserveHold(r.Context(), holds, plan, tx)
		}

		response := accountTransactionResponse(tx.Transaction, sourceAddress, asset)
		response["destination_account_id"] = destination.ID.String()
		if plan != nil {
			response["reserve"] = reserveResponse(plan)
		}
		if charge != nil {
			response["charges"] = chargeResponse(charge)
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
		if _, err := accountService.ValidateLien(r.Context(), accountID, amount, currentBalance, usageAt); err != nil && !isIdempotentRetry(r.Context(), l, r.Header.Get("Idempotency-Key")) {
			handleAccountError(w, r, err)
			return
		}

		asset, err := currencyregistry.AssetFor(account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		accountAvailable := walletAvailableAddress(account.WalletID, account.Currency)
		accountLien := walletLienAddress(account.WalletID, account.Currency)
		script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, asset, amount, accountAvailable, accountLien)

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

		_, tx, replayed, err := l.CreateTransaction(r.Context(), params)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") || strings.Contains(strings.ToLower(err.Error()), "duplicate reference") {
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
				return
			}
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// A replayed request already counted: do not touch activity or usage twice.
		if !replayed {
			if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
				handleAccountError(w, r, err)
				return
			}
			if err := accountService.RecordDebitUsage(r.Context(), accountID, amount, req.Reference, usageAt); err != nil {
				handleAccountError(w, r, err)
				return
			}
		}
		api.Created(w, accountTransactionResponse(tx.Transaction, accountAvailable, asset))
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

		req.Mode = normalizeReleaseMode(req.Mode)
		l := common.LedgerFromContext(r.Context())
		if _, err := accountService.ValidateRelease(r.Context(), accountID, req.Mode); err != nil && !isIdempotentRetry(r.Context(), l, r.Header.Get("Idempotency-Key")) {
			handleAccountError(w, r, err)
			return
		}

		asset, err := currencyregistry.AssetFor(account.Currency)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		accountAvailable := walletAvailableAddress(account.WalletID, account.Currency)
		accountLien := walletLienAddress(account.WalletID, account.Currency)

		var script string
		mode := strings.TrimSpace(req.Mode)
		if mode == "PAY" {
			script = fmt.Sprintf(`
				send [%s %d] (
					source = @%s
					destination = @world
				)
			`, asset, amount, accountLien)
		} else {
			script = fmt.Sprintf(`
				send [%s %d] (
					source = @%s
					destination = @%s
				)
			`, asset, amount, accountLien, accountAvailable)
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

		_, tx, replayed, err := l.CreateTransaction(r.Context(), params)
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
				send [%s %d] (
					source = @%s allowing unbounded overdraft
					destination = @%s
				)
			`, asset, channelAmount, channelAccount, hotrows.ChannelSink(req.Reference))
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
					send [%s %d] (
						source = %s
						destination = @%s
					)
				`, asset, revenue, hotrows.SourceClause(hotrows.RevenueSource(req.Reference)), hotrows.RevenueDestination(hotrows.AccumulatedBase, req.Reference))
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

		// A replayed request already counted: do not touch activity or usage twice.
		if !replayed {
			if _, err := accountService.TouchActivity(r.Context(), accountID, tx.Transaction.Timestamp.Time); err != nil {
				handleAccountError(w, r, err)
				return
			}
		}

		response := accountTransactionResponse(tx.Transaction, accountAvailable, asset)
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

func accountTransactionResponse(tx ledgerinternal.Transaction, trackedAccount string, assetName string) map[string]any {
	var balanceBefore, balanceAfter int64
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

	asset, err := currencyregistry.AssetFor(account.Currency)
	if err != nil {
		return err
	}

	accountUser := walletAvailableAddress(account.WalletID, account.Currency)
	accountSystem := fmt.Sprintf("system:control:%s", account.Currency)
	script := fmt.Sprintf(`
		send [%s %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, asset, amount, accountSystem, accountUser)

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
	asset, err := currencyregistry.AssetFor(currency)
	if err != nil {
		return 0, err
	}
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
	return walletaddr.Available(walletID, currency)
}

func walletLienAddress(walletID, currency string) string {
	return walletaddr.Lien(walletID, currency)
}

// systemControlAddress is shared with the atomic transfer executor through hotrows, so the handler
// and the transfer user leg always agree on the control shard of a wallet.
func systemControlAddress(seed, currency string) string {
	return hotrows.SystemControlAddress(seed, currency)
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

// isIdempotentRetry reports whether a request with this Idempotency-Key was already
// recorded in the ledger. Balance and limit checks must not reject such a retry: the
// ledger replays the original result, and a check against the post-request balance
// would turn a completed debit into a false "would overdraw" error.
func isIdempotentRetry(ctx context.Context, l ledgercontroller.Controller, key string) bool {
	if key == "" {
		return false
	}
	order := bunpaginate.Order(bunpaginate.OrderDesc)
	cursor, err := l.ListLogs(ctx, storagecommon.InitialPaginatedQuery[any]{
		Column:   "id",
		Order:    &order,
		PageSize: 1,
		Options:  storagecommon.ResourceQuery[any]{Builder: query.Match("idempotency_key", key)},
	})
	return err == nil && cursor != nil && len(cursor.Data) > 0
}

// normalizeReleaseMode accepts the wallet API spellings (release_only, release_and_debit)
// next to the account API ones (RELEASE, PAY), so one client works against both.
func normalizeReleaseMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "release_only":
		return "RELEASE"
	case "release_and_debit":
		return "PAY"
	default:
		return strings.TrimSpace(mode)
	}
}
