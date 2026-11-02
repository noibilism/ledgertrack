package bankman

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
)

type fakeProvisioning struct {
	client   *models.Client
	owner    *models.Client
	account  *models.Account
	replayed bool
	err      error
	gotC     services.BankmanClientInput
	gotA     services.BankmanAccountInput
}

func (f *fakeProvisioning) CreateClient(_ context.Context, in services.BankmanClientInput) (*models.Client, *models.Client, error) {
	f.gotC = in
	return f.client, f.owner, f.err
}

func (f *fakeProvisioning) CreateAccount(_ context.Context, in services.BankmanAccountInput) (*models.Account, *models.Client, bool, error) {
	f.gotA = in
	return f.account, f.client, f.replayed, f.err
}

func TestCreateClientShapeAndShadowID(t *testing.T) {
	ext := uuid.NewString()
	owner := &models.Client{ID: uuid.New(), Contact: models.ClientContact{Email: "o@x.com"}, IndividualData: &models.IndividualData{FirstName: "Ada", LastName: "Obi"},
		KYCData: map[string]any{"legacy_source_id": "bm-owner"}}
	client := &models.Client{ID: uuid.New(), KYCStatus: "pending", Contact: models.ClientContact{Email: "c@x.com", Phone: "+234"},
		CorporateData: &models.CorporateData{LegalName: "Acme Ltd"}, KYCData: map[string]any{"legacy_external_id": ext, "legacy_source_id": "bm-client"},
		CreatedAt: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)}
	prov := &fakeProvisioning{client: client, owner: owner}
	h, _ := setupAll(t, &backend{}, &fakeRoles{}, prov)

	rec, out := doHeader(h, http.MethodPost, "/bankman/api/v1/clients",
		`{"client_type":"organization","email":"c@x.com","phone_number":"+234","owner_client_id":"bm-owner","business_name":"Acme Ltd","address":{"street":"1 Main","postal_code":"100001"}}`,
		map[string]string{shadowIDHeader: "bm-client"})
	if rec.Code != http.StatusCreated || out["message"] != "Client created successfully" {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	d := out["data"].(map[string]any)
	if d["id"] != "bm-client" || d["external_id"] != ext || d["display_name"] != "Acme Ltd" || d["owner_client_id"] != "bm-owner" || d["kyc_status"] != "pending" {
		t.Errorf("data = %v", d)
	}
	if d["owner"].(map[string]any)["name"] != "Ada Obi" {
		t.Errorf("owner = %v", d["owner"])
	}
	if prov.gotC.BankmanID != "bm-client" || prov.gotC.Address == nil || prov.gotC.Address.Line1 != "1 Main" || prov.gotC.Address.Postal != "100001" {
		t.Errorf("input = %+v", prov.gotC)
	}

	prov.err = fmt.Errorf("%w: the email has already been taken", services.ErrProvisioningValidation)
	rec, out = do(h, http.MethodPost, "/bankman/api/v1/clients", `{}`)
	if rec.Code != http.StatusUnprocessableEntity || out["message"] != "the email has already been taken" {
		t.Errorf("validation: %d %v", rec.Code, out["message"])
	}
}

func TestCreateAccountCreatedAndRetrieved(t *testing.T) {
	ext := uuid.NewString()
	client := &models.Client{ID: uuid.New(), KYCData: map[string]any{"legacy_external_id": ext}}
	name, typ, cc := "Main", "main", "ng"
	acct := &models.Account{ID: uuid.New(), Currency: "NGN", Status: "active", AccountName: &name, AccountType: &typ, CountryCode: &cc, WalletID: "w1"}
	prov := &fakeProvisioning{client: client, account: acct}
	h, _ := setupAll(t, &backend{avail: 250000}, &fakeRoles{}, prov)

	body := `{"id":"` + acct.ID.String() + `","external_id":"` + ext + `","product_id":"p","account_name":"Main","account_type":"main","country_code":"ng"}`
	rec, out := do(h, http.MethodPost, "/bankman/api/v1/accounts", body)
	if rec.Code != http.StatusCreated || out["message"] != "Account created successfully" {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	d := out["data"].(map[string]any)
	if d["id"] != acct.ID.String() || d["available_balance"] != "0.0000" || d["status"] != "active" || d["country_code"] != "ng" || d["external_id"] != ext {
		t.Errorf("data = %v", d)
	}
	if prov.gotA.ID != acct.ID.String() || prov.gotA.AccountType != "main" {
		t.Errorf("input = %+v", prov.gotA)
	}

	prov.replayed = true
	rec, out = do(h, http.MethodPost, "/bankman/api/v1/accounts", body)
	if rec.Code != http.StatusOK || out["message"] != "Wallet account retrieved" {
		t.Fatalf("replay: %d %s", rec.Code, rec.Body.String())
	}
	if out["data"].(map[string]any)["available_balance"] != "2500.0000" {
		t.Errorf("replay balance = %v", out["data"])
	}

	acct.Status = models.AccountStatusPending
	prov.replayed = false
	_, out = do(h, http.MethodPost, "/bankman/api/v1/accounts", body)
	if out["data"].(map[string]any)["status"] != "inactive" {
		t.Errorf("a pending account is reported as inactive: %v", out["data"])
	}
}
