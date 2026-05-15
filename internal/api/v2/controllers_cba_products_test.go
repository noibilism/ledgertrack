package v2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	"github.com/formancehq/ledger/internal/cba/services"
)

type productResponse struct {
	Products []models.Product `json:"products"`
}

type productRepositoryForHTTPTests struct {
	products    map[uuid.UUID]*models.Product
	lastFilter  repositories.ProductFilter
	returnEmpty bool
}

func newProductRepositoryForHTTPTests() *productRepositoryForHTTPTests {
	return &productRepositoryForHTTPTests{
		products: map[uuid.UUID]*models.Product{},
	}
}

func (s *productRepositoryForHTTPTests) Create(_ context.Context, product *models.Product) error {
	for _, existing := range s.products {
		if existing.Code == product.Code {
			return postgres.ErrConstraintsFailed{}
		}
	}
	if product.ID == uuid.Nil {
		product.ID = uuid.New()
	}
	copied := *product
	s.products[product.ID] = &copied
	return nil
}

func (s *productRepositoryForHTTPTests) Update(_ context.Context, product *models.Product) error {
	if _, ok := s.products[product.ID]; !ok {
		return postgres.ErrNotFound
	}
	copied := *product
	s.products[product.ID] = &copied
	return nil
}

func (s *productRepositoryForHTTPTests) Get(_ context.Context, id uuid.UUID) (*models.Product, error) {
	product, ok := s.products[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	copied := *product
	return &copied, nil
}

func (s *productRepositoryForHTTPTests) GetByCode(_ context.Context, code string) (*models.Product, error) {
	for _, product := range s.products {
		if product.Code == code {
			copied := *product
			return &copied, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (s *productRepositoryForHTTPTests) List(_ context.Context, filter repositories.ProductFilter) ([]models.Product, error) {
	s.lastFilter = filter
	if s.returnEmpty {
		return []models.Product{}, nil
	}

	ret := make([]models.Product, 0, len(s.products))
	for _, product := range s.products {
		if filter.Category != nil && product.Category != *filter.Category {
			continue
		}
		if filter.Currency != nil && product.Currency != *filter.Currency {
			continue
		}
		if filter.Status != nil && product.Status != *filter.Status {
			continue
		}
		ret = append(ret, *product)
	}
	return ret, nil
}

func newProductServiceForHTTPTests() (services.ProductService, *productRepositoryForHTTPTests) {
	repo := newProductRepositoryForHTTPTests()
	return services.NewProductService(repo), repo
}

func TestCreateProduct(t *testing.T) {
	productService, _ := newProductServiceForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithProductService(productService))

	req := httptest.NewRequest(http.MethodPost, "/test/products", api.Buffer(t, services.CreateProductInput{
		Code:     "SAV-NGN-001",
		Name:     "Personal Savings NGN",
		Category: "savings",
		Currency: "ngn",
	}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	product, ok := api.DecodeSingleResponse[models.Product](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "SAV-NGN-001", product.Code)
	require.Equal(t, "NGN", product.Currency)
	require.Equal(t, models.ProductStatusDraft, product.Status)
	require.Equal(t, "0", product.Rules.MinOpeningBalance)
}

func TestCreateProductValidationError(t *testing.T) {
	productService, _ := newProductServiceForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithProductService(productService))

	req := httptest.NewRequest(http.MethodPost, "/test/products", api.Buffer(t, services.CreateProductInput{
		Code:     "SAV-XYZ-001",
		Name:     "Unsupported",
		Category: "savings",
		Currency: "XYZ",
	}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	err := api.ErrorResponse{}
	api.Decode(t, rec.Body, &err)
	require.EqualValues(t, common.ErrValidation, err.ErrorCode)
}

func TestListProductsAppliesFilters(t *testing.T) {
	productService, repo := newProductServiceForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithProductService(productService))

	product, err := productService.Create(context.Background(), services.CreateProductInput{
		Code:     "SAV-NGN-001",
		Name:     "Personal Savings NGN",
		Category: "savings",
		Currency: "NGN",
	})
	require.NoError(t, err)
	_, err = productService.Activate(context.Background(), product.ID)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/test/products?category=savings&currency=ngn&status=active", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, ok := api.DecodeSingleResponse[productResponse](t, rec.Body)
	require.True(t, ok)
	require.Len(t, response.Products, 1)
	require.Equal(t, "SAV-NGN-001", response.Products[0].Code)

	require.NotNil(t, repo.lastFilter.Category)
	require.Equal(t, "savings", *repo.lastFilter.Category)
	require.NotNil(t, repo.lastFilter.Currency)
	require.Equal(t, "NGN", *repo.lastFilter.Currency)
	require.NotNil(t, repo.lastFilter.Status)
	require.Equal(t, models.ProductStatusActive, *repo.lastFilter.Status)
}

func TestActivateProduct(t *testing.T) {
	productService, _ := newProductServiceForHTTPTests()
	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithProductService(productService))

	product, err := productService.Create(context.Background(), services.CreateProductInput{
		Code:     "CUR-USD-001",
		Name:     "Corporate Current USD",
		Category: "current",
		Currency: "USD",
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/test/products/"+product.ID.String()+"/activate", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	activated, ok := api.DecodeSingleResponse[models.Product](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, product.ID, activated.ID)
	require.Equal(t, models.ProductStatusActive, activated.Status)
}
