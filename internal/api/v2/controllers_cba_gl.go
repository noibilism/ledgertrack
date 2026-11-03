package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

// glImportMaxBodyBytes bounds an import request body.
const glImportMaxBodyBytes = 8 << 20

func createGLAccount(glAccountService services.GLAccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input services.CreateGLAccountInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		account, err := glAccountService.Create(r.Context(), input)
		if err != nil {
			handleGLAccountError(w, r, err)
			return
		}
		api.Created(w, account)
	}
}

func listGLAccounts(glAccountService services.GLAccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter := repositories.GLAccountFilter{}
		q := r.URL.Query()

		if value := strings.ToLower(strings.TrimSpace(q.Get("type"))); value != "" {
			if !isGLAccountType(value) {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid type %q", value))
				return
			}
			filter.Type = &value
		}
		if value := strings.ToUpper(strings.TrimSpace(q.Get("currency"))); value != "" {
			filter.Currency = &value
		}
		if value := strings.TrimSpace(q.Get("parent")); value != "" {
			filter.ParentCode = &value
		}
		if value := strings.ToLower(strings.TrimSpace(q.Get("status"))); value != "" {
			if value != models.GLAccountStatusActive && value != models.GLAccountStatusClosed {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid status %q", value))
				return
			}
			filter.Status = &value
		}

		accounts, err := glAccountService.List(r.Context(), filter)
		if err != nil {
			handleGLAccountError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"accounts": accounts,
		})
	}
}

func readGLAccount(glAccountService services.GLAccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code, err := getGLAccountCode(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		account, err := glAccountService.GetByCode(r.Context(), code)
		if err != nil {
			handleGLAccountError(w, r, err)
			return
		}
		api.Ok(w, account)
	}
}

func patchGLAccount(glAccountService services.GLAccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code, err := getGLAccountCode(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		// Unknown fields are rejected so that an attempt to change an immutable field (type, address, ...)
		// fails loudly instead of being silently ignored.
		var input services.PatchGLAccountInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		account, err := glAccountService.Patch(r.Context(), code, input)
		if err != nil {
			handleGLAccountError(w, r, err)
			return
		}
		api.Ok(w, account)
	}
}

func importGLAccounts(glAccountService services.GLAccountService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dryRun := false
		if raw := strings.TrimSpace(r.URL.Query().Get("dry_run")); raw != "" {
			// strict on purpose: a typo must never turn a dry run into a real import
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid dry_run %q, expected true or false", raw))
				return
			}
			dryRun = parsed
		}

		body := http.MaxBytesReader(w, r.Body, glImportMaxBodyBytes)

		var (
			rows []services.CreateGLAccountInput
			err  error
		)
		if isCSVContentType(r.Header.Get("Content-Type")) {
			rows, err = services.ParseGLAccountsCSV(body)
		} else {
			rows, err = decodeGLImportJSON(body)
		}
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				api.BadRequest(w, common.ErrValidation, fmt.Errorf("import body is larger than %d bytes", glImportMaxBodyBytes))
				return
			}
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		report, err := glAccountService.Import(r.Context(), rows, dryRun)
		switch {
		case errors.Is(err, services.ErrGLImportRejected):
			writeGLImportRejection(w, r, report)
		case err != nil:
			handleGLAccountError(w, r, err)
		default:
			api.Ok(w, report)
		}
	}
}

func getGLAccountBalance(sys systemcontroller.Controller, glReportingService services.GLReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code, err := getGLAccountCode(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		pit, err := getPIT(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		balance, err := glReportingService.AccountBalance(r.Context(), sys, code, pit)
		if err != nil {
			handleGLAccountError(w, r, err)
			return
		}
		api.Ok(w, balance)
	}
}

func getGLTrialBalanceReport(sys systemcontroller.Controller, glReportingService services.GLReportingService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		asOf, err := getDate(r, "asOf")
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		report, err := glReportingService.TrialBalance(r.Context(), sys, services.GLTrialBalanceFilter{
			Currency: r.URL.Query().Get("currency"),
			AsOf:     asOf,
		})
		if err != nil {
			handleGLAccountError(w, r, err)
			return
		}
		api.Ok(w, report)
	}
}

// writeGLImportRejection answers a rejected import with the full per-row report so the caller can fix it.
func writeGLImportRejection(w http.ResponseWriter, r *http.Request, report *services.GLImportReport) {
	status := http.StatusConflict
	code := common.ErrConflict
	message := "import rejected, nothing was applied: some rows conflict with existing accounts"
	if report.Invalid > 0 {
		status = http.StatusBadRequest
		code = common.ErrValidation
		message = "import rejected, nothing was applied: some rows are invalid"
	}

	body, err := json.Marshal(map[string]any{
		"errorCode":    code,
		"errorMessage": message,
		"details":      fmt.Sprintf("%d invalid, %d conflicting, %d unchanged, %d to create", report.Invalid, report.Conflicts, report.Unchanged, report.ToCreate),
		"report":       report,
	})
	if err != nil {
		common.InternalServerError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	api.WriteResponse(w, status, body)
}

func decodeGLImportJSON(body io.Reader) ([]services.CreateGLAccountInput, error) {
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var rows []services.CreateGLAccountInput
	if err := decoder.Decode(&rows); err != nil {
		return nil, fmt.Errorf("%w: body must be a JSON array of accounts: %w", services.ErrGLAccountValidation, err)
	}
	return rows, nil
}

func decodeStrictJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return false
	}
	return true
}

func isCSVContentType(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	switch mediaType {
	case "text/csv", "application/csv", "application/vnd.ms-excel":
		return true
	default:
		return false
	}
}

func isGLAccountType(value string) bool {
	for _, accountType := range models.GLAccountTypes {
		if accountType == value {
			return true
		}
	}
	return false
}

func getGLAccountCode(r *http.Request) (string, error) {
	raw := chi.URLParam(r, "code")
	code, err := url.PathUnescape(raw)
	if err != nil {
		return "", fmt.Errorf("invalid code: %w", err)
	}
	code = strings.TrimSpace(code)
	if code == "" {
		return "", fmt.Errorf("code is required")
	}
	return code, nil
}

func handleGLAccountError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrGLAccountValidation),
		errors.Is(err, services.ErrGLReportingValidation):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrGLAccountAlreadyExists):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case errors.Is(err, services.ErrGLAccountNotFound):
		api.NotFound(w, err)
	default:
		common.HandleCommonErrors(w, r, err)
	}
}
