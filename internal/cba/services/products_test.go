package services

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

type productRepositoryStub struct {
	products map[uuid.UUID]*models.Product
}

func newProductRepositoryStub() *productRepositoryStub {
	return &productRepositoryStub{
		products: map[uuid.UUID]*models.Product{},
	}
}

func (s *productRepositoryStub) Create(_ context.Context, product *models.Product) error {
	if product.ID == uuid.Nil {
		product.ID = uuid.New()
	}
	copied := *product
	s.products[product.ID] = &copied
	return nil
}

func (s *productRepositoryStub) Update(_ context.Context, product *models.Product) error {
	copied := *product
	s.products[product.ID] = &copied
	return nil
}

func (s *productRepositoryStub) Get(_ context.Context, id uuid.UUID) (*models.Product, error) {
	product, ok := s.products[id]
	if !ok {
		return nil, ErrProductNotFound
	}
	copied := *product
	return &copied, nil
}

func (s *productRepositoryStub) GetByCode(_ context.Context, code string) (*models.Product, error) {
	for _, product := range s.products {
		if product.Code == code {
			copied := *product
			return &copied, nil
		}
	}
	return nil, ErrProductNotFound
}

func (s *productRepositoryStub) List(_ context.Context, _ repositories.ProductFilter) ([]models.Product, error) {
	ret := make([]models.Product, 0, len(s.products))
	for _, product := range s.products {
		ret = append(ret, *product)
	}
	return ret, nil
}

func TestProductServiceCreateDefaults(t *testing.T) {
	t.Parallel()

	service := NewProductService(newProductRepositoryStub())
	product, err := service.Create(context.Background(), CreateProductInput{
		Code:     "SAV-NGN-001",
		Name:     "Personal Savings NGN",
		Category: "savings",
		Currency: "ngn",
	})
	require.NoError(t, err)
	require.Equal(t, models.ProductStatusDraft, product.Status)
	require.Equal(t, "NGN", product.Currency)
	require.Equal(t, "0", product.Rules.MinOpeningBalance)
	require.Equal(t, "0", product.Rules.MinBalance)
	require.True(t, product.Rules.AllowDebits)
	require.True(t, product.Rules.AllowCredits)
}

func TestProductServiceRejectsDisabledCurrency(t *testing.T) {
	t.Parallel()

	service := NewProductService(newProductRepositoryStub())
	_, err := service.Create(context.Background(), CreateProductInput{
		Code:     "SAV-XYZ-001",
		Name:     "Unsupported",
		Category: "savings",
		Currency: "XYZ",
	})
	require.ErrorIs(t, err, ErrProductValidation)
}

func TestProductServiceRejectsRestrictedActivePatch(t *testing.T) {
	t.Parallel()

	repo := newProductRepositoryStub()
	service := NewProductService(repo)
	product, err := service.Create(context.Background(), CreateProductInput{
		Code:     "SAV-USD-001",
		Name:     "Personal Savings USD",
		Category: "savings",
		Currency: "USD",
	})
	require.NoError(t, err)

	product.Status = models.ProductStatusActive
	require.NoError(t, repo.Update(context.Background(), product))

	nextCode := "NEW-CODE"
	_, err = service.Patch(context.Background(), product.ID, PatchProductInput{
		Code: &nextCode,
	})
	require.ErrorIs(t, err, ErrProductActivePatchRestricted)
}

func TestProductServiceActivate(t *testing.T) {
	t.Parallel()

	service := NewProductService(newProductRepositoryStub())
	product, err := service.Create(context.Background(), CreateProductInput{
		Code:     "CUR-USD-001",
		Name:     "Corporate Current USD",
		Category: "current",
		Currency: "USD",
	})
	require.NoError(t, err)

	activated, err := service.Activate(context.Background(), product.ID)
	require.NoError(t, err)
	require.Equal(t, models.ProductStatusActive, activated.Status)
}
