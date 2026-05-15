//go:build it

package test_suite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/services"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

type cbaEnvelope[T any] struct {
	Data T `json:"data"`
}

type cbaCursorResponse[T any] struct {
	Cursor struct {
		Data []T `json:"data"`
	} `json:"cursor"`
}

type cbaCurrenciesResponse struct {
	Currencies []struct {
		Code      string `json:"code"`
		Precision int    `json:"precision"`
		Enabled   bool   `json:"enabled"`
	} `json:"currencies"`
}

type cbaProductsResponse struct {
	Products []models.Product `json:"products"`
}

type cbaClientsResponse struct {
	Clients []models.Client `json:"clients"`
}

type cbaKYCResponse struct {
	Records []models.KYCRecord `json:"records"`
}

type cbaAccountsResponse struct {
	Accounts []models.Account `json:"accounts"`
}

type cbaAccountDetailsResponse struct {
	models.Account
	WalletInfo struct {
		WalletID         string `json:"wallet_id"`
		Currency         string `json:"currency"`
		AvailableBalance int64  `json:"available_balance"`
		AvailableAddress string `json:"available_address"`
		LienAddress      string `json:"lien_address"`
	} `json:"wallet_info"`
}

type cbaAccountBalanceResponse struct {
	AccountID string `json:"account_id"`
	WalletID  string `json:"wallet_id"`
	Currency  string `json:"currency"`
	Balance   int64  `json:"balance"`
}

type cbaTransactionResponse struct {
	TxID          int64             `json:"txid"`
	Reference     string            `json:"reference"`
	Metadata      map[string]string `json:"metadata"`
	BalanceBefore int64             `json:"balance_before"`
	BalanceAfter  int64             `json:"balance_after"`
}

type cbaRenderedTransaction struct {
	ID        int64     `json:"id"`
	Reference string    `json:"reference"`
	Timestamp time.Time `json:"timestamp"`
}

type cbaAccountStatementReportResponse struct {
	TransactionCount int              `json:"transaction_count"`
	Transactions     []map[string]any `json:"transactions"`
}

func cbaRequest(specContext SpecContext, baseURL, method, path string, body any, expectedStatus int) []byte {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		Expect(err).To(BeNil())
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(specContext, method, baseURL+path, reader)
	Expect(err).To(BeNil())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rsp, err := http.DefaultClient.Do(req)
	Expect(err).To(BeNil())
	defer rsp.Body.Close()

	payload, err := io.ReadAll(rsp.Body)
	Expect(err).To(BeNil())
	Expect(rsp.StatusCode).To(Equal(expectedStatus), string(payload))
	return payload
}

func cbaDecodeData[T any](payload []byte) T {
	var envelope cbaEnvelope[T]
	Expect(json.Unmarshal(payload, &envelope)).To(Succeed())
	return envelope.Data
}

func cbaDecodeCursor[T any](payload []byte) []T {
	var envelope cbaCursorResponse[T]
	Expect(json.Unmarshal(payload, &envelope)).To(Succeed())
	return envelope.Cursor.Data
}

var _ = Context("CBA functional API tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(natsURL),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	It("should exercise the complete CBA endpoint surface", func(specContext SpecContext) {
		srv := Wait(specContext, testServer)
		baseURL := testservice.GetServerURL(srv).String()
		today := time.Now().UTC().Format("2006-01-02")
		pnlStart := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
		pnlEnd := time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339)
		futureExpiry := time.Now().UTC().Add(365 * 24 * time.Hour).Format(time.RFC3339)

		By("creating the ledgertrack ledger")
		_ = ctx
		cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack", map[string]any{}, http.StatusNoContent)
		cbaRequest(specContext, baseURL, http.MethodPost, "/v2/revenue-USD", map[string]any{}, http.StatusNoContent)

		By("listing currencies")
		currencies := cbaDecodeData[cbaCurrenciesResponse](
			cbaRequest(specContext, baseURL, http.MethodGet, "/v2/ledgertrack/currencies", nil, http.StatusOK),
		)
		Expect(currencies.Currencies).ToNot(BeEmpty())
		Expect(currencies.Currencies[0].Code).ToNot(BeEmpty())

		By("creating, reading, patching, listing, activating, and later retiring a product")
		product := cbaDecodeData[models.Product](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/products", map[string]any{
			"code":        "SAV-USD-E2E",
			"name":        "Functional Savings USD",
			"description": "Functional test savings product",
			"category":    "savings",
			"currency":    "USD",
			"rules": map[string]any{
				"min_opening_balance":   "0",
				"min_balance":           "0",
				"allow_debits":          true,
				"allow_credits":         true,
				"requires_kyc_level":    1,
				"eligible_client_types": []string{"individual"},
			},
			"interest_config": map[string]any{
				"type":              "simple",
				"rate":              "5.0",
				"accrual_frequency": "daily",
				"posting_frequency": "monthly",
			},
			"fee_schedule": map[string]any{
				"maintenance_fee": map[string]any{
					"amount":    "1.00",
					"frequency": "monthly",
					"currency":  "USD",
				},
			},
		}, http.StatusCreated))
		Expect(product.Status).To(Equal(models.ProductStatusDraft))

		product = cbaDecodeData[models.Product](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/products/"+product.ID.String(),
			nil,
			http.StatusOK,
		))
		Expect(product.Code).To(Equal("SAV-USD-E2E"))

		product = cbaDecodeData[models.Product](cbaRequest(specContext, baseURL, http.MethodPatch, "/v2/ledgertrack/products/"+product.ID.String(), map[string]any{
			"description": "Functional savings product updated",
		}, http.StatusOK))
		Expect(product.Description).To(Equal("Functional savings product updated"))

		product = cbaDecodeData[models.Product](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/products/"+product.ID.String()+"/activate",
			nil,
			http.StatusOK,
		))
		Expect(product.Status).To(Equal(models.ProductStatusActive))

		products := cbaDecodeData[cbaProductsResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/products?category=savings&currency=usd&status=active",
			nil,
			http.StatusOK,
		))
		Expect(products.Products).To(HaveLen(1))
		Expect(products.Products[0].ID).To(Equal(product.ID))

		By("creating, patching, listing, reading, verifying KYC for, and activating the primary client")
		client := cbaDecodeData[models.Client](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients", map[string]any{
			"type": "individual",
			"contact": map[string]any{
				"email": "ada@example.com",
				"phone": "+15550001111",
				"address": map[string]any{
					"line1":   "1 Main St",
					"city":    "Lagos",
					"state":   "LA",
					"country": "NG",
					"postal":  "100001",
				},
			},
			"individual_data": map[string]any{
				"first_name":         "Ada",
				"last_name":          "Lovelace",
				"national_id_type":   "passport",
				"national_id_number": "P1234567",
				"nationality":        "NG",
			},
		}, http.StatusCreated))
		Expect(client.Status).To(Equal(models.ClientStatusPending))

		client = cbaDecodeData[models.Client](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/clients/"+client.ID.String(),
			nil,
			http.StatusOK,
		))
		Expect(client.Type).To(Equal(models.ClientTypeIndividual))

		client = cbaDecodeData[models.Client](cbaRequest(specContext, baseURL, http.MethodPatch, "/v2/ledgertrack/clients/"+client.ID.String(), map[string]any{
			"contact": map[string]any{
				"email": "ada.updated@example.com",
				"phone": "+15550002222",
				"address": map[string]any{
					"line1":   "2 Updated St",
					"city":    "Abuja",
					"state":   "FC",
					"country": "NG",
					"postal":  "900001",
				},
			},
		}, http.StatusOK))
		Expect(client.Contact.Email).To(Equal("ada.updated@example.com"))

		kycRecord := cbaDecodeData[models.KYCRecord](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients/"+client.ID.String()+"/kyc", map[string]any{
			"level": 1,
			"documents": []map[string]any{
				{
					"type":      "national_id",
					"reference": "doc-client-primary-1",
					"provider":  "s3",
				},
			},
			"payload": map[string]any{
				"national_id_number": "P1234567",
			},
		}, http.StatusCreated))
		Expect(kycRecord.Status).To(Equal(models.KYCStatusPending))

		kycHistory := cbaDecodeData[cbaKYCResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/clients/"+client.ID.String()+"/kyc",
			nil,
			http.StatusOK,
		))
		Expect(kycHistory.Records).To(HaveLen(1))

		kycRecord = cbaDecodeData[models.KYCRecord](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients/"+client.ID.String()+"/kyc/"+kycRecord.ID.String()+"/verify", map[string]any{
			"verifier":   "ops-functional",
			"expires_at": futureExpiry,
		}, http.StatusOK))
		Expect(kycRecord.Status).To(Equal(models.KYCStatusVerified))

		client = cbaDecodeData[models.Client](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/clients/"+client.ID.String()+"/activate",
			nil,
			http.StatusOK,
		))
		Expect(client.Status).To(Equal(models.ClientStatusActive))

		By("creating a secondary client to cover KYC rejection")
		rejectedClient := cbaDecodeData[models.Client](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients", map[string]any{
			"type": "individual",
			"contact": map[string]any{
				"phone": "+15550003333",
			},
			"individual_data": map[string]any{
				"first_name": "Grace",
				"last_name":  "Hopper",
			},
		}, http.StatusCreated))

		rejectedKYC := cbaDecodeData[models.KYCRecord](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients/"+rejectedClient.ID.String()+"/kyc", map[string]any{
			"level": 1,
			"payload": map[string]any{
				"national_id_number": "BAD-ID-001",
			},
		}, http.StatusCreated))

		rejectedKYC = cbaDecodeData[models.KYCRecord](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients/"+rejectedClient.ID.String()+"/kyc/"+rejectedKYC.ID.String()+"/reject", map[string]any{
			"reason": "document mismatch",
		}, http.StatusOK))
		Expect(rejectedKYC.Status).To(Equal(models.KYCStatusRejected))

		clients := cbaDecodeData[cbaClientsResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/clients?type=individual&status=active&kyc_status=verified",
			nil,
			http.StatusOK,
		))
		Expect(clients.Clients).To(HaveLen(1))
		Expect(clients.Clients[0].ID).To(Equal(client.ID))

		client = cbaDecodeData[models.Client](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/clients/"+client.ID.String()+"/suspend", map[string]any{
			"reason": "manual review",
		}, http.StatusOK))
		Expect(client.Status).To(Equal(models.ClientStatusSuspended))
		Expect(client.KYCData["suspension_reason"]).To(Equal("manual review"))

		client = cbaDecodeData[models.Client](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/clients/"+client.ID.String()+"/reactivate",
			nil,
			http.StatusOK,
		))
		Expect(client.Status).To(Equal(models.ClientStatusActive))

		By("opening, listing, reading, activating, and transacting on an account")
		account := cbaDecodeData[models.Account](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/accounts", map[string]any{
			"client_id":       client.ID.String(),
			"product_id":      product.ID.String(),
			"opening_deposit": 0,
			"metadata": map[string]any{
				"source": "functional-e2e",
			},
		}, http.StatusCreated))
		Expect(account.Status).To(Equal(models.AccountStatusPending))

		accountList := cbaDecodeData[cbaAccountsResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/accounts?client_id="+url.QueryEscape(client.ID.String()),
			nil,
			http.StatusOK,
		))
		Expect(accountList.Accounts).To(HaveLen(1))
		Expect(accountList.Accounts[0].ID).To(Equal(account.ID))

		clientAccounts := cbaDecodeData[cbaAccountsResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/clients/"+client.ID.String()+"/accounts",
			nil,
			http.StatusOK,
		))
		Expect(clientAccounts.Accounts).To(HaveLen(1))
		Expect(clientAccounts.Accounts[0].ID).To(Equal(account.ID))

		accountDetails := cbaDecodeData[cbaAccountDetailsResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/accounts/"+account.ID.String(),
			nil,
			http.StatusOK,
		))
		Expect(accountDetails.ID).To(Equal(account.ID))
		Expect(accountDetails.WalletInfo.WalletID).To(Equal(account.WalletID))
		Expect(accountDetails.WalletInfo.Currency).To(Equal("USD"))

		account = cbaDecodeData[models.Account](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/activate",
			nil,
			http.StatusOK,
		))
		Expect(account.Status).To(Equal(models.AccountStatusActive))

		accountBalance := cbaDecodeData[cbaAccountBalanceResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/balance",
			nil,
			http.StatusOK,
		))
		Expect(accountBalance.Balance).To(Equal(int64(0)))

		creditResponse := cbaDecodeData[cbaTransactionResponse](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/accounts/"+account.ID.String()+"/credit", map[string]any{
			"amount":    100,
			"reference": "e2e-credit-001",
			"metadata": map[string]any{
				"type": "topup",
			},
		}, http.StatusCreated))
		Expect(creditResponse.BalanceBefore).To(Equal(int64(0)))
		Expect(creditResponse.BalanceAfter).To(Equal(int64(100)))

		lienResponse := cbaDecodeData[cbaTransactionResponse](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/accounts/"+account.ID.String()+"/lien", map[string]any{
			"amount":    30,
			"reference": "e2e-lien-001",
		}, http.StatusCreated))
		Expect(lienResponse.BalanceAfter).To(Equal(int64(70)))

		releaseResponse := cbaDecodeData[cbaTransactionResponse](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/accounts/"+account.ID.String()+"/lien/release", map[string]any{
			"amount":    30,
			"reference": "e2e-release-001",
			"mode":      "RELEASE",
		}, http.StatusCreated))
		Expect(releaseResponse.BalanceAfter).To(Equal(int64(100)))

		debitResponse := cbaDecodeData[cbaTransactionResponse](cbaRequest(specContext, baseURL, http.MethodPost, "/v2/ledgertrack/accounts/"+account.ID.String()+"/debit", map[string]any{
			"amount":    100,
			"reference": "e2e-debit-001",
		}, http.StatusCreated))
		Expect(debitResponse.BalanceBefore).To(Equal(int64(100)))
		Expect(debitResponse.BalanceAfter).To(Equal(int64(0)))

		By("reading account history and statement")
		history := cbaDecodeCursor[cbaRenderedTransaction](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/history",
			nil,
			http.StatusOK,
		))
		Expect(history).To(HaveLen(4))

		statement := cbaDecodeCursor[cbaRenderedTransaction](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/statement",
			nil,
			http.StatusOK,
		))
		Expect(statement).To(HaveLen(4))

		By("reading the reporting endpoints")
		portfolio := cbaDecodeData[services.ClientPortfolioReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/reports/clients/"+client.ID.String()+"/portfolio",
			nil,
			http.StatusOK,
		))
		Expect(portfolio.TotalAccounts).To(Equal(1))
		Expect(portfolio.Accounts[0].ID).To(Equal(account.ID))

		statementReport := cbaDecodeData[cbaAccountStatementReportResponse](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/reports/accounts/"+account.ID.String()+"/statement",
			nil,
			http.StatusOK,
		))
		Expect(statementReport.TransactionCount).To(Equal(4))
		Expect(statementReport.Transactions).To(HaveLen(4))

		dailyReport := cbaDecodeData[services.DailyTransactionSummaryReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			fmt.Sprintf("/v2/ledgertrack/reports/transactions/daily?date=%s&client_id=%s&account_id=%s", url.QueryEscape(today), url.QueryEscape(client.ID.String()), url.QueryEscape(account.ID.String())),
			nil,
			http.StatusOK,
		))
		Expect(dailyReport.ReportDate).To(Equal(today))
		Expect(dailyReport.TransactionCount).To(Equal(int64(4)))

		interestFeeReport := cbaDecodeData[services.InterestFeeReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			fmt.Sprintf("/v2/ledgertrack/reports/interest-fees?client_id=%s&account_id=%s", url.QueryEscape(client.ID.String()), url.QueryEscape(account.ID.String())),
			nil,
			http.StatusOK,
		))
		Expect(interestFeeReport.Accounts).To(HaveLen(1))
		Expect(interestFeeReport.Accounts[0].AccountID).To(Equal(account.ID))

		By("reading the finance reporting endpoints")
		trialBalance := cbaDecodeData[services.FinanceTrialBalanceReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/reports/finance/trial-balance?currency=USD",
			nil,
			http.StatusOK,
		))
		Expect(trialBalance.Currency).To(Equal("USD"))
		Expect(trialBalance.Lines).ToNot(BeEmpty())

		balanceSheet := cbaDecodeData[services.FinanceBalanceSheetReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			"/v2/ledgertrack/reports/finance/balance-sheet?currency=USD",
			nil,
			http.StatusOK,
		))
		Expect(balanceSheet.Currency).To(Equal("USD"))
		Expect(balanceSheet.Assets).ToNot(BeEmpty())
		Expect(balanceSheet.Liabilities).ToNot(BeEmpty())

		pnl := cbaDecodeData[services.FinanceProfitAndLossReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			fmt.Sprintf("/v2/ledgertrack/reports/finance/pnl?currency=USD&startTime=%s&endTime=%s", url.QueryEscape(pnlStart), url.QueryEscape(pnlEnd)),
			nil,
			http.StatusOK,
		))
		Expect(pnl.Currency).To(Equal("USD"))
		Expect(pnl.Income).ToNot(BeEmpty())
		Expect(pnl.Expenses).ToNot(BeEmpty())

		cashFlow := cbaDecodeData[services.FinanceCashFlowReport](cbaRequest(
			specContext,
			baseURL,
			http.MethodGet,
			fmt.Sprintf("/v2/ledgertrack/reports/finance/cash-flow?currency=USD&startTime=%s&endTime=%s", url.QueryEscape(pnlStart), url.QueryEscape(pnlEnd)),
			nil,
			http.StatusOK,
		))
		Expect(cashFlow.Currency).To(Equal("USD"))
		Expect(cashFlow.Lines).ToNot(BeEmpty())

		By("exercising account lifecycle endpoints")
		account = cbaDecodeData[models.Account](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/suspend",
			nil,
			http.StatusOK,
		))
		Expect(account.Status).To(Equal(models.AccountStatusSuspended))

		account = cbaDecodeData[models.Account](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/reactivate",
			nil,
			http.StatusOK,
		))
		Expect(account.Status).To(Equal(models.AccountStatusActive))

		account = cbaDecodeData[models.Account](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/freeze",
			nil,
			http.StatusOK,
		))
		Expect(account.FreezeDebits).To(BeTrue())

		account = cbaDecodeData[models.Account](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/dormant",
			nil,
			http.StatusOK,
		))
		Expect(account.Status).To(Equal(models.AccountStatusDormant))

		account = cbaDecodeData[models.Account](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/accounts/"+account.ID.String()+"/close",
			nil,
			http.StatusOK,
		))
		Expect(account.Status).To(Equal(models.AccountStatusClosed))

		By("closing the client and retiring the product")
		client = cbaDecodeData[models.Client](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/clients/"+client.ID.String()+"/close",
			nil,
			http.StatusOK,
		))
		Expect(client.Status).To(Equal(models.ClientStatusClosed))

		product = cbaDecodeData[models.Product](cbaRequest(
			specContext,
			baseURL,
			http.MethodPost,
			"/v2/ledgertrack/products/"+product.ID.String()+"/retire",
			nil,
			http.StatusOK,
		))
		Expect(product.Status).To(Equal(models.ProductStatusRetired))
	})
})
