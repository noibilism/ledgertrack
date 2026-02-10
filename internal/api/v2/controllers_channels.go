package v2

import (
	"encoding/json"
	"fmt"
	"net/http"

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
	"github.com/google/uuid"
)

type CreateChannelRequest struct {
	Currency string            `json:"currency"`
	Metadata map[string]string `json:"metadata"`
}

type CreditChannelRequest struct {
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Reference string `json:"reference"`
}

func createChannel(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateChannelRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.Currency == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("currency is required"))
			return
		}

		channelID := uuid.New().String()
		ledgerName := fmt.Sprintf("channels-%s", req.Currency)

		// Ensure ledger exists
		_ = sys.CreateLedger(r.Context(), ledgerName, ledgerinternal.Configuration{})

		// Get ledger controller
		l, err := sys.GetLedgerController(r.Context(), ledgerName)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		// Create account (lazy via saving metadata)
		// We just need to ensure the account is "known"
		accountName := fmt.Sprintf("channel:%s", channelID)
		
		// To "create" it, we can save metadata if provided, or just return the ID.
		// Since user asked for metadata storage:
		if req.Metadata != nil {
			_, _, err := l.SaveAccountMetadata(r.Context(), ledger.Parameters[ledger.SaveAccountMetadata]{
				Input: ledger.SaveAccountMetadata{
					Address:  accountName,
					Metadata: metadata.Metadata(req.Metadata),
				},
			})
			if err != nil {
				common.HandleCommonWriteErrors(w, r, err)
				return
			}
		}

		api.Created(w, map[string]interface{}{
			"data": map[string]string{
				"channel_id": channelID,
				"currency":   req.Currency,
				"ledger":     ledgerName,
			},
		})
	}
}

func creditChannel(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelID := chi.URLParam(r, "channelID")
		var req CreditChannelRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if req.Currency == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("currency is required"))
			return
		}
		if req.Amount <= 0 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("amount must be positive"))
			return
		}

		ledgerName := fmt.Sprintf("channels-%s", req.Currency)
		l, err := sys.GetLedgerController(r.Context(), ledgerName)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		accountName := fmt.Sprintf("channel:%s", channelID)
		
		script := fmt.Sprintf(`
			send [%s/2 %d] (
				source = @world
				destination = @%s
			)
		`, req.Currency, req.Amount, accountName)

		params := ledger.Parameters[ledger.CreateTransaction]{
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

		api.Created(w, map[string]interface{}{
			"data": tx,
		})
	}
}

func readChannel(sys systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelID := chi.URLParam(r, "channelID")
		// We need currency to find the ledger. 
		// Since endpoints like GET /channels/{id} don't imply currency in path, 
		// we should require it in query param, or iterate?
		// User requirement says "Channel and Revenue Ledger is per currency".
		// We'll require ?currency=USD
		
		currency := r.URL.Query().Get("currency")
		if currency == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("currency query param required"))
			return
		}

		ledgerName := fmt.Sprintf("channels-%s", currency)
		l, err := sys.GetLedgerController(r.Context(), ledgerName)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		accountName := fmt.Sprintf("channel:%s", channelID)
		acc, err := l.GetAccount(r.Context(), storagecommon.ResourceQuery[any]{
			Builder: query.Match("address", accountName),
			Expand:  []string{"volumes"},
		})
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err) // Reusing common error handler
			return
		}

		api.Ok(w, acc)
	}
}

func getChannelHistory(sys systemcontroller.Controller, paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelID := chi.URLParam(r, "channelID")
		currency := r.URL.Query().Get("currency")
		if currency == "" {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("currency query param required"))
			return
		}

		ledgerName := fmt.Sprintf("channels-%s", currency)
		l, err := sys.GetLedgerController(r.Context(), ledgerName)
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		accountName := fmt.Sprintf("channel:%s", channelID)
		var qb query.Builder = query.Match("account", accountName)

		if startTime := r.URL.Query().Get("startTime"); startTime != "" {
			qb = query.And(qb, query.Gte("timestamp", startTime))
		}
		if endTime := r.URL.Query().Get("endTime"); endTime != "" {
			qb = query.And(qb, query.Lte("timestamp", endTime))
		}

		pq, err := getPaginatedQuery[any](r, paginationConfig, "id", bunpaginate.OrderDesc, func(q *storagecommon.ResourceQuery[any]) {
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
