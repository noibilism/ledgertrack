package v2

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

func createClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[services.CreateClientInput](w, r, func(req services.CreateClientInput) {
			client, err := clientService.Create(r.Context(), req)
			if err != nil {
				handleClientError(w, r, err)
				return
			}
			api.Created(w, client)
		})
	}
}

func listClients(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter := repositories.ClientFilter{}
		if clientType := strings.TrimSpace(r.URL.Query().Get("type")); clientType != "" {
			filter.Type = &clientType
		}
		if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
			filter.Status = &status
		}
		if kycStatus := strings.TrimSpace(r.URL.Query().Get("kyc_status")); kycStatus != "" {
			filter.KYCStatus = &kycStatus
		}

		clients, err := clientService.List(r.Context(), filter)
		if err != nil {
			handleClientError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"clients": clients,
		})
	}
}

func readClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		client, err := clientService.Get(r.Context(), clientID)
		if err != nil {
			handleClientError(w, r, err)
			return
		}
		api.Ok(w, client)
	}
}

func listClientAccounts(clientService services.ClientService, accountService services.AccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if _, err := clientService.Get(r.Context(), clientID); err != nil {
			handleClientError(w, r, err)
			return
		}

		accounts, err := accountService.List(r.Context(), repositories.AccountFilter{
			ClientID: &clientID,
		})
		if err != nil {
			handleAccountError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"accounts": accounts,
		})
	}
}

func patchClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		common.WithBody[services.PatchClientInput](w, r, func(req services.PatchClientInput) {
			client, err := clientService.Patch(r.Context(), clientID, req)
			if err != nil {
				handleClientError(w, r, err)
				return
			}
			api.Ok(w, client)
		})
	}
}

func activateClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		client, err := clientService.Activate(r.Context(), clientID)
		if err != nil {
			handleClientError(w, r, err)
			return
		}
		api.Ok(w, client)
	}
}

func suspendClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		common.WithBody[services.SuspendClientInput](w, r, func(req services.SuspendClientInput) {
			client, err := clientService.Suspend(r.Context(), clientID, req)
			if err != nil {
				handleClientError(w, r, err)
				return
			}
			api.Ok(w, client)
		})
	}
}

func reactivateClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		client, err := clientService.Reactivate(r.Context(), clientID)
		if err != nil {
			handleClientError(w, r, err)
			return
		}
		api.Ok(w, client)
	}
}

func closeClient(clientService services.ClientService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		client, err := clientService.Close(r.Context(), clientID)
		if err != nil {
			handleClientError(w, r, err)
			return
		}
		api.Ok(w, client)
	}
}

func submitKYC(kycService services.KYCService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		common.WithBody[services.SubmitKYCInput](w, r, func(req services.SubmitKYCInput) {
			record, err := kycService.Submit(r.Context(), clientID, req)
			if err != nil {
				handleClientError(w, r, err)
				return
			}
			api.Created(w, record)
		})
	}
}

func verifyKYC(kycService services.KYCService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, kycID, err := getClientAndKYCIDs(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		common.WithBody[services.VerifyKYCInput](w, r, func(req services.VerifyKYCInput) {
			record, err := kycService.Verify(r.Context(), clientID, kycID, req)
			if err != nil {
				handleClientError(w, r, err)
				return
			}
			api.Ok(w, record)
		})
	}
}

func rejectKYC(kycService services.KYCService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, kycID, err := getClientAndKYCIDs(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		common.WithBody[services.RejectKYCInput](w, r, func(req services.RejectKYCInput) {
			record, err := kycService.Reject(r.Context(), clientID, kycID, req)
			if err != nil {
				handleClientError(w, r, err)
				return
			}
			api.Ok(w, record)
		})
	}
}

func listClientKYC(kycService services.KYCService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID, err := getClientID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		records, err := kycService.History(r.Context(), clientID)
		if err != nil {
			handleClientError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"records": records,
		})
	}
}

func getClientID(r *http.Request) (uuid.UUID, error) {
	clientID := chi.URLParam(r, "clientID")
	if clientID == "" {
		return uuid.Nil, fmt.Errorf("clientID is required")
	}
	ret, err := uuid.Parse(clientID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid clientID: %w", err)
	}
	return ret, nil
}

func getClientAndKYCIDs(r *http.Request) (uuid.UUID, uuid.UUID, error) {
	clientID, err := getClientID(r)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	kycID := chi.URLParam(r, "kycID")
	if kycID == "" {
		return uuid.Nil, uuid.Nil, fmt.Errorf("kycID is required")
	}
	ret, err := uuid.Parse(kycID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid kycID: %w", err)
	}
	return clientID, ret, nil
}

func handleClientError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrClientValidation),
		errors.Is(err, services.ErrClientInvalidStateTransition),
		errors.Is(err, services.ErrClientKYCRequirement),
		errors.Is(err, services.ErrClientHasAccounts),
		errors.Is(err, services.ErrKYCValidation):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrClientAlreadyExists):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case errors.Is(err, services.ErrClientNotFound), errors.Is(err, services.ErrKYCNotFound):
		api.NotFound(w, err)
	default:
		common.InternalServerError(w, r, err)
	}
}
