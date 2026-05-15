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

func createProduct(productService services.ProductService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[services.CreateProductInput](w, r, func(req services.CreateProductInput) {
			product, err := productService.Create(r.Context(), req)
			if err != nil {
				handleProductError(w, r, err)
				return
			}
			api.Created(w, product)
		})
	}
}

func listProducts(productService services.ProductService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter := repositories.ProductFilter{}
		if category := strings.TrimSpace(r.URL.Query().Get("category")); category != "" {
			filter.Category = &category
		}
		if currency := strings.TrimSpace(r.URL.Query().Get("currency")); currency != "" {
			currency = strings.ToUpper(currency)
			filter.Currency = &currency
		}
		if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
			filter.Status = &status
		}

		products, err := productService.List(r.Context(), filter)
		if err != nil {
			handleProductError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{
			"products": products,
		})
	}
}

func readProduct(productService services.ProductService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID, err := getProductID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		product, err := productService.Get(r.Context(), productID)
		if err != nil {
			handleProductError(w, r, err)
			return
		}
		api.Ok(w, product)
	}
}

func patchProduct(productService services.ProductService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID, err := getProductID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		common.WithBody[services.PatchProductInput](w, r, func(req services.PatchProductInput) {
			product, err := productService.Patch(r.Context(), productID, req)
			if err != nil {
				handleProductError(w, r, err)
				return
			}
			api.Ok(w, product)
		})
	}
}

func activateProduct(productService services.ProductService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID, err := getProductID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		product, err := productService.Activate(r.Context(), productID)
		if err != nil {
			handleProductError(w, r, err)
			return
		}
		api.Ok(w, product)
	}
}

func retireProduct(productService services.ProductService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		productID, err := getProductID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		product, err := productService.Retire(r.Context(), productID)
		if err != nil {
			handleProductError(w, r, err)
			return
		}
		api.Ok(w, product)
	}
}

func getProductID(r *http.Request) (uuid.UUID, error) {
	productID := chi.URLParam(r, "productID")
	if productID == "" {
		return uuid.Nil, fmt.Errorf("productID is required")
	}
	ret, err := uuid.Parse(productID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid productID: %w", err)
	}
	return ret, nil
}

func handleProductError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, services.ErrProductValidation),
		errors.Is(err, services.ErrProductInvalidStateTransition),
		errors.Is(err, services.ErrProductActivePatchRestricted):
		api.BadRequest(w, common.ErrValidation, err)
	case errors.Is(err, services.ErrProductAlreadyExists):
		api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
	case errors.Is(err, services.ErrProductNotFound):
		api.NotFound(w, err)
	default:
		common.InternalServerError(w, r, err)
	}
}
