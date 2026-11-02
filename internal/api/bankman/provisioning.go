package bankman

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
)

// shadowIDHeader carries the id Bankman gave a resource when the shadow gateway mirrors a creation, so the
// same id is returned to callers after cutover.
const shadowIDHeader = "X-Bankman-Resource-Id"

type addressBody struct {
	Street     string `json:"street"`
	City       string `json:"city"`
	State      string `json:"state"`
	Country    string `json:"country"`
	PostalCode string `json:"postal_code"`
}

type clientBody struct {
	ClientType                 string       `json:"client_type"`
	ExternalID                 string       `json:"external_id"`
	Email                      string       `json:"email"`
	PhoneNumber                string       `json:"phone_number"`
	Address                    *addressBody `json:"address"`
	FirstName                  string       `json:"first_name"`
	LastName                   string       `json:"last_name"`
	DateOfBirth                string       `json:"date_of_birth"`
	Gender                     string       `json:"gender"`
	Nationality                string       `json:"nationality"`
	OwnerClientID              string       `json:"owner_client_id"`
	BusinessName               string       `json:"business_name"`
	BusinessRegistrationNumber string       `json:"business_registration_number"`
	BusinessRegistrationDate   string       `json:"business_registration_date"`
	TaxIdentificationNumber    string       `json:"tax_identification_number"`
	IndustrySector             string       `json:"industry_sector"`
}

func bankmanClientID(c *models.Client) string {
	if v, ok := c.KYCData["legacy_source_id"].(string); ok && v != "" {
		return v
	}
	return c.ID.String()
}

func displayName(c *models.Client) string {
	switch {
	case c.CorporateData != nil:
		return c.CorporateData.LegalName
	case c.IndividualData != nil:
		return strings.TrimSpace(c.IndividualData.FirstName + " " + c.IndividualData.LastName)
	}
	return ""
}

func (h *Handler) provisioningError(w http.ResponseWriter, err error, fallback string) {
	switch {
	case errors.Is(err, services.ErrProvisioningValidation):
		fail(w, http.StatusUnprocessableEntity, strings.TrimPrefix(err.Error(), services.ErrProvisioningValidation.Error()+": "))
	case errors.Is(err, services.ErrClientAlreadyExists):
		fail(w, http.StatusUnprocessableEntity, "The client has already been taken")
	default:
		fail(w, http.StatusInternalServerError, fallback)
	}
}

// createClient answers POST /clients.
func (h *Handler) createClient(w http.ResponseWriter, r *http.Request) {
	var b clientBody
	if err := readJSON(r, &b); err != nil {
		fail(w, http.StatusUnprocessableEntity, "invalid JSON body")
		return
	}
	in := services.BankmanClientInput{
		ClientType: b.ClientType, ExternalID: b.ExternalID, Email: b.Email, PhoneNumber: b.PhoneNumber,
		FirstName: b.FirstName, LastName: b.LastName, DateOfBirth: b.DateOfBirth, Gender: b.Gender, Nationality: b.Nationality,
		OwnerClientID: b.OwnerClientID, BusinessName: b.BusinessName, BusinessRegistrationNumber: b.BusinessRegistrationNumber,
		BusinessRegistrationDate: b.BusinessRegistrationDate, TaxIdentificationNumber: b.TaxIdentificationNumber,
		IndustrySector: b.IndustrySector, BankmanID: strings.TrimSpace(r.Header.Get(shadowIDHeader)),
	}
	if a := b.Address; a != nil {
		in.Address = &models.Address{Line1: a.Street, City: a.City, State: a.State, Country: a.Country, Postal: a.PostalCode}
	}
	client, owner, err := h.prov.CreateClient(r.Context(), in)
	if err != nil {
		h.provisioningError(w, err, "Failed to create client")
		return
	}
	var ownerView any
	var ownerID any
	if owner != nil {
		ownerID = bankmanClientID(owner)
		ownerView = map[string]any{"id": bankmanClientID(owner), "name": displayName(owner), "email": owner.Contact.Email}
	}
	respond(w, http.StatusCreated, "Client created successfully", map[string]any{
		"id": bankmanClientID(client), "external_id": client.KYCData["legacy_external_id"], "client_type": b.ClientType,
		"owner_client_id": ownerID, "owner": ownerView, "display_name": displayName(client),
		"email": nilIfEmpty(client.Contact.Email), "phone_number": nilIfEmpty(client.Contact.Phone),
		"kyc_status": client.KYCStatus, "created_at": client.CreatedAt.UTC().Format(time.RFC3339),
	})
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

type accountBody struct {
	ID          string  `json:"id"`
	ExternalID  string  `json:"external_id"`
	ProductID   string  `json:"product_id"`
	AccountName string  `json:"account_name"`
	AccountType string  `json:"account_type"`
	CountryCode *string `json:"country_code"`
	Status      string  `json:"status"`
}

// createAccount answers POST /accounts. A matching wallet that already exists is returned, as in Bankman.
func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var b accountBody
	if err := readJSON(r, &b); err != nil {
		fail(w, http.StatusUnprocessableEntity, "invalid JSON body")
		return
	}
	account, client, replayed, err := h.prov.CreateAccount(r.Context(), services.BankmanAccountInput{
		ID: b.ID, ExternalID: b.ExternalID, ProductID: b.ProductID, AccountName: b.AccountName,
		AccountType: b.AccountType, CountryCode: b.CountryCode, Status: b.Status,
		BankmanID: strings.TrimSpace(r.Header.Get(shadowIDHeader)),
	})
	if err != nil {
		h.provisioningError(w, err, "Failed to create account")
		return
	}
	available, actual := "0.0000", "0.0000"
	if replayed {
		av, lien, berr := h.balances(r, account)
		if berr != nil {
			fail(w, http.StatusInternalServerError, "Failed to create account")
			return
		}
		available, actual = fourDecimals(av, account.Currency), fourDecimals(av+lien, account.Currency)
	}
	status := account.Status
	if status == models.AccountStatusPending {
		status = "inactive"
	}
	data := map[string]any{
		"id": bankmanID(account), "external_id": client.KYCData["legacy_external_id"], "account_name": account.AccountName,
		"country_code": account.CountryCode, "available_balance": available, "actual_balance": actual, "status": status,
	}
	if replayed {
		respond(w, http.StatusOK, "Wallet account retrieved", data)
		return
	}
	respond(w, http.StatusCreated, "Account created successfully", data)
}
