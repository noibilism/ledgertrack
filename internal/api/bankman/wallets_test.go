package bankman

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
)

type fakeRoles struct {
	account  *models.Account
	accounts []models.Account
	replayed bool
	err      error
	classify services.ClassifyWalletInput
	create   services.CreateR2SubaccountInput
}

func (f *fakeRoles) ResolveMainWallet(context.Context, string, string) (*models.Account, error) {
	return f.account, f.err
}

func (f *fakeRoles) ResolveR2Subaccount(context.Context, string, string, string) (*models.Account, error) {
	return f.account, f.err
}

func (f *fakeRoles) List(context.Context, string) ([]models.Account, error) {
	return f.accounts, f.err
}

func (f *fakeRoles) Show(context.Context, string, string) (*models.Account, error) {
	return f.account, f.err
}

func (f *fakeRoles) Classify(_ context.Context, _ string, in services.ClassifyWalletInput) (*models.Account, bool, error) {
	f.classify = in
	return f.account, f.replayed, f.err
}

func (f *fakeRoles) CreateR2Subaccount(_ context.Context, in services.CreateR2SubaccountInput) (*models.Account, bool, error) {
	f.create = in
	return f.account, f.replayed, f.err
}

func str(s string) *string { return &s }

func TestResolveMainWalletShapeAndErrors(t *testing.T) {
	acct := &models.Account{ID: uuid.New(), Currency: "NGN", WalletRole: str(models.WalletRoleMerchantMain),
		Metadata: map[string]any{"legacy_source_id": "bm-acct-9"}}
	h, _ := setupRoles(t, &backend{}, &fakeRoles{account: acct})
	rec, out := do(h, http.MethodGet, "/bankman/api/v1/internal/accounts/resolved-main-wallet?external_id=e&currency=NGN", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	data := out["data"].(map[string]any)
	if data["id"] != "bm-acct-9" || data["currency"] != "NGN" || data["wallet_role"] != "merchant_main" {
		t.Errorf("resolved wallet = %v", data)
	}
	if _, has := out["message"]; has {
		t.Errorf("the resolved-wallet response carries no message in Bankman: %v", out)
	}

	for err, want := range map[error]int{
		services.ErrWalletResolution:                             http.StatusConflict,
		fmt.Errorf("%w: currency", services.ErrWalletValidation): http.StatusUnprocessableEntity,
		services.ErrWalletNotFound:                               http.StatusNotFound,
	} {
		h, _ := setupRoles(t, &backend{}, &fakeRoles{err: err})
		rec, out := do(h, http.MethodGet, "/bankman/api/v1/internal/accounts/resolved-main-wallet?external_id=e&currency=NGN", "")
		if rec.Code != want {
			t.Errorf("%v: status %d, want %d", err, rec.Code, want)
		}
		if want == http.StatusConflict && out["message"] != "wallet_selection_ambiguous_or_missing" {
			t.Errorf("conflict message = %v", out["message"])
		}
	}
}

func TestUnclassifiedWalletReadsAsUnknown(t *testing.T) {
	acct := &models.Account{ID: uuid.New(), Currency: "NGN", Status: "active"}
	h, _ := setupRoles(t, &backend{}, &fakeRoles{account: acct})
	_, out := do(h, http.MethodGet, "/bankman/api/v1/internal/accounts/"+acct.ID.String()+"/wallet-classification?external_id=e", "")
	data := out["data"].(map[string]any)
	if data["wallet_role"] != "unknown" || data["status"] != "active" || data["r2_relationship_public_id"] != nil {
		t.Errorf("classification = %v", data)
	}
	if _, leaked := data["account_number"]; leaked {
		t.Errorf("the classification read must stay redacted: %v", data)
	}
}

func TestClassifyAndCreateStatusCodes(t *testing.T) {
	acct := &models.Account{ID: uuid.New(), Currency: "NGN", WalletRole: str(models.WalletRoleR2Subaccount), R2RelationshipPublicID: str("rel-1")}

	roles := &fakeRoles{account: acct}
	h, _ := setupRoles(t, &backend{}, roles)
	rec, out := do(h, http.MethodPut, "/bankman/api/v1/internal/accounts/"+acct.ID.String()+"/wallet-classification",
		`{"external_id":"e","wallet_role":"r2_subaccount","r2_relationship_public_id":"rel-1","evidence_reference":"t-1","correction":true}`)
	if rec.Code != http.StatusOK || out["message"] != "Wallet classification saved" {
		t.Fatalf("classify: %d %s", rec.Code, rec.Body.String())
	}
	if !roles.classify.Correction || roles.classify.WalletRole != "r2_subaccount" || roles.classify.Evidence != "t-1" {
		t.Errorf("classify input = %+v", roles.classify)
	}

	roles.replayed = true
	_, out = do(h, http.MethodPut, "/bankman/api/v1/internal/accounts/"+acct.ID.String()+"/wallet-classification", `{"external_id":"e","wallet_role":"r2_subaccount","r2_relationship_public_id":"rel-1","evidence_reference":"t-1"}`)
	if out["message"] != "Wallet classification retrieved" {
		t.Errorf("replay message = %v", out["message"])
	}

	body := `{"account_id":"` + acct.ID.String() + `","external_id":"e","product_id":"` + uuid.NewString() + `","account_name":"R2","account_type":"main","r2_relationship_public_id":"rel-1","evidence_reference":"t-1"}`
	roles.replayed = false
	rec, out = do(h, http.MethodPost, "/bankman/api/v1/internal/accounts/r2-subaccounts", body)
	if rec.Code != http.StatusCreated || out["message"] != "R2 subaccount wallet created" {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	roles.replayed = true
	rec, out = do(h, http.MethodPost, "/bankman/api/v1/internal/accounts/r2-subaccounts", body)
	if rec.Code != http.StatusOK || out["message"] != "R2 subaccount wallet retrieved" {
		t.Fatalf("replay: %d %s", rec.Code, rec.Body.String())
	}

	roles.err = fmt.Errorf("%w: role is immutable", services.ErrWalletRoleConflict)
	rec, out = do(h, http.MethodPut, "/bankman/api/v1/internal/accounts/"+acct.ID.String()+"/wallet-classification", `{}`)
	if rec.Code != http.StatusConflict || out["message"] != "role is immutable" {
		t.Errorf("conflict: %d %v", rec.Code, out["message"])
	}
	rec, _ = do(h, http.MethodPost, "/bankman/api/v1/internal/accounts/r2-subaccounts", `not json`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("bad json: %d", rec.Code)
	}
}

func TestWalletClassificationListIsOwnerScoped(t *testing.T) {
	roles := &fakeRoles{accounts: []models.Account{
		{ID: uuid.New(), Currency: "NGN", Status: "active", AccountType: str("main"), WalletRole: str(models.WalletRoleMerchantMain)},
		{ID: uuid.New(), Currency: "NGN", Status: "active"},
	}}
	h, _ := setupRoles(t, &backend{}, roles)
	_, out := do(h, http.MethodGet, "/bankman/api/v1/internal/accounts/wallet-classifications?external_id=e", "")
	rows := out["data"].([]any)
	if len(rows) != 2 || rows[1].(map[string]any)["wallet_role"] != "unknown" || rows[0].(map[string]any)["account_type"] != "main" {
		t.Errorf("rows = %v", rows)
	}
}
