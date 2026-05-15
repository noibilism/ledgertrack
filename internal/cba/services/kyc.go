package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var (
	ErrKYCValidation = errors.New("kyc validation failed")
	ErrKYCNotFound   = errors.New("kyc record not found")
)

type KYCService interface {
	Submit(context.Context, uuid.UUID, SubmitKYCInput) (*models.KYCRecord, error)
	Verify(context.Context, uuid.UUID, uuid.UUID, VerifyKYCInput) (*models.KYCRecord, error)
	Reject(context.Context, uuid.UUID, uuid.UUID, RejectKYCInput) (*models.KYCRecord, error)
	History(context.Context, uuid.UUID) ([]models.KYCRecord, error)
}

type SubmitKYCInput struct {
	Level     int                  `json:"level"`
	Documents []models.KYCDocument `json:"documents,omitempty"`
	Payload   map[string]any       `json:"payload,omitempty"`
}

type VerifyKYCInput struct {
	Verifier  string  `json:"verifier"`
	ExpiresAt *string `json:"expires_at,omitempty"`
}

type RejectKYCInput struct {
	Reason string `json:"reason"`
}

type DefaultKYCService struct {
	clientRepository repositories.ClientRepository
	kycRepository    repositories.KYCRepository
}

func NewKYCService(
	clientRepository repositories.ClientRepository,
	kycRepository repositories.KYCRepository,
) KYCService {
	return &DefaultKYCService{
		clientRepository: clientRepository,
		kycRepository:    kycRepository,
	}
}

func (s *DefaultKYCService) Submit(ctx context.Context, clientID uuid.UUID, input SubmitKYCInput) (*models.KYCRecord, error) {
	client, err := s.clientRepository.Get(ctx, clientID)
	if err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	record := &models.KYCRecord{
		ClientID:    clientID,
		Level:       input.Level,
		Status:      models.KYCStatusPending,
		SubmittedAt: time.Now().UTC(),
		Documents:   normalizeKYCDocuments(input.Documents),
		Payload:     normalizeMap(input.Payload),
	}

	if err := validateKYCSubmission(client, record); err != nil {
		return nil, err
	}

	if err := s.kycRepository.Create(ctx, record); err != nil {
		return nil, resolveKYCRepositoryError(err)
	}

	client.KYCStatus = models.KYCStatusPending
	client.KYCData = mergeMaps(client.KYCData, record.Payload)
	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return record, nil
}

func (s *DefaultKYCService) Verify(ctx context.Context, clientID, kycID uuid.UUID, input VerifyKYCInput) (*models.KYCRecord, error) {
	client, record, err := s.loadClientAndRecord(ctx, clientID, kycID)
	if err != nil {
		return nil, err
	}

	if record.Status == models.KYCStatusVerified {
		return record, nil
	}
	if record.Status != models.KYCStatusPending {
		return nil, fmt.Errorf("%w: only pending KYC records can be verified", ErrKYCValidation)
	}

	expiresAt, err := parseOptionalTimestamp(input.ExpiresAt)
	if err != nil {
		return nil, err
	}
	if expiresAt != nil && expiresAt.Before(time.Now().UTC()) {
		return nil, fmt.Errorf("%w: expires_at must be in the future", ErrKYCValidation)
	}

	record.Status = models.KYCStatusVerified
	now := time.Now().UTC()
	record.VerifiedAt = &now
	record.Verifier = strings.TrimSpace(input.Verifier)
	record.ExpiresAt = expiresAt

	if err := s.kycRepository.Update(ctx, record); err != nil {
		return nil, resolveKYCRepositoryError(err)
	}

	if record.Level > client.KYCLevel {
		client.KYCLevel = record.Level
	}
	client.KYCStatus = models.KYCStatusVerified
	client.KYCData = mergeMaps(client.KYCData, record.Payload)
	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return record, nil
}

func (s *DefaultKYCService) Reject(ctx context.Context, clientID, kycID uuid.UUID, input RejectKYCInput) (*models.KYCRecord, error) {
	client, record, err := s.loadClientAndRecord(ctx, clientID, kycID)
	if err != nil {
		return nil, err
	}

	if record.Status == models.KYCStatusRejected {
		return record, nil
	}
	if record.Status != models.KYCStatusPending {
		return nil, fmt.Errorf("%w: only pending KYC records can be rejected", ErrKYCValidation)
	}

	reason := strings.TrimSpace(input.Reason)
	if reason == "" {
		return nil, fmt.Errorf("%w: reason is required", ErrKYCValidation)
	}

	record.Status = models.KYCStatusRejected
	record.Reason = reason
	if err := s.kycRepository.Update(ctx, record); err != nil {
		return nil, resolveKYCRepositoryError(err)
	}

	client.KYCStatus = models.KYCStatusRejected
	if client.KYCData == nil {
		client.KYCData = map[string]any{}
	}
	client.KYCData["rejection_reason"] = reason
	if err := s.clientRepository.Update(ctx, client); err != nil {
		return nil, resolveClientRepositoryError(err)
	}

	return record, nil
}

func (s *DefaultKYCService) History(ctx context.Context, clientID uuid.UUID) ([]models.KYCRecord, error) {
	if _, err := s.clientRepository.Get(ctx, clientID); err != nil {
		return nil, resolveClientRepositoryError(err)
	}
	return s.kycRepository.ListByClient(ctx, clientID)
}

func (s *DefaultKYCService) loadClientAndRecord(ctx context.Context, clientID, kycID uuid.UUID) (*models.Client, *models.KYCRecord, error) {
	client, err := s.clientRepository.Get(ctx, clientID)
	if err != nil {
		return nil, nil, resolveClientRepositoryError(err)
	}
	record, err := s.kycRepository.Get(ctx, kycID)
	if err != nil {
		return nil, nil, resolveKYCRepositoryError(err)
	}
	if record.ClientID != clientID {
		return nil, nil, ErrKYCNotFound
	}
	return client, record, nil
}

func validateKYCSubmission(client *models.Client, record *models.KYCRecord) error {
	if record.Level < 0 || record.Level > 3 {
		return fmt.Errorf("%w: level must be between 0 and 3", ErrKYCValidation)
	}

	if !hasKYCName(client, record.Payload) {
		return fmt.Errorf("%w: level 0 requires a client name", ErrKYCValidation)
	}
	if !hasPayloadOrClientValue(record.Payload, "phone", client.Contact.Phone) {
		return fmt.Errorf("%w: level 0 requires phone", ErrKYCValidation)
	}

	if record.Level >= 1 && !hasNationalID(client, record.Payload) {
		return fmt.Errorf("%w: level 1 requires national_id_number", ErrKYCValidation)
	}
	if record.Level >= 2 {
		if !hasDocumentType(record.Documents, "government_id") {
			return fmt.Errorf("%w: level 2 requires a government_id document", ErrKYCValidation)
		}
		if !hasDocumentType(record.Documents, "proof_of_address") {
			return fmt.Errorf("%w: level 2 requires a proof_of_address document", ErrKYCValidation)
		}
	}
	if record.Level >= 3 && !hasLevelThreeEvidence(record.Payload) {
		return fmt.Errorf("%w: level 3 requires biometric or in-person verification evidence", ErrKYCValidation)
	}

	return nil
}

func normalizeKYCDocuments(documents []models.KYCDocument) []models.KYCDocument {
	ret := make([]models.KYCDocument, 0, len(documents))
	for _, document := range documents {
		ret = append(ret, models.KYCDocument{
			Type:      strings.TrimSpace(document.Type),
			Reference: strings.TrimSpace(document.Reference),
			Provider:  strings.TrimSpace(document.Provider),
			Metadata:  normalizeMap(document.Metadata),
		})
	}
	return ret
}

func normalizeMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	ret := make(map[string]any, len(input))
	for key, value := range input {
		ret[strings.TrimSpace(key)] = value
	}
	return ret
}

func mergeMaps(dst, src map[string]any) map[string]any {
	ret := normalizeMap(dst)
	for key, value := range normalizeMap(src) {
		ret[key] = value
	}
	return ret
}

func hasKYCName(client *models.Client, payload map[string]any) bool {
	switch client.Type {
	case models.ClientTypeIndividual:
		if client.IndividualData != nil && strings.TrimSpace(client.IndividualData.FirstName) != "" && strings.TrimSpace(client.IndividualData.LastName) != "" {
			return true
		}
	case models.ClientTypeCorporate:
		if client.CorporateData != nil && strings.TrimSpace(client.CorporateData.LegalName) != "" {
			return true
		}
	}

	firstName, _ := payload["first_name"].(string)
	lastName, _ := payload["last_name"].(string)
	legalName, _ := payload["legal_name"].(string)
	return (strings.TrimSpace(firstName) != "" && strings.TrimSpace(lastName) != "") || strings.TrimSpace(legalName) != ""
}

func hasNationalID(client *models.Client, payload map[string]any) bool {
	if client.IndividualData != nil && strings.TrimSpace(client.IndividualData.NationalIDNumber) != "" {
		return true
	}
	if value, ok := payload["national_id_number"].(string); ok && strings.TrimSpace(value) != "" {
		return true
	}
	if client.KYCData != nil {
		if value, ok := client.KYCData["national_id_number"].(string); ok && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func hasPayloadOrClientValue(payload map[string]any, key, fallback string) bool {
	if strings.TrimSpace(fallback) != "" {
		return true
	}
	if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
		return true
	}
	return false
}

func hasDocumentType(documents []models.KYCDocument, docType string) bool {
	for _, document := range documents {
		if strings.EqualFold(strings.TrimSpace(document.Type), docType) {
			return true
		}
	}
	return false
}

func hasLevelThreeEvidence(payload map[string]any) bool {
	if value, ok := payload["biometric_reference"].(string); ok && strings.TrimSpace(value) != "" {
		return true
	}
	if value, ok := payload["in_person_verification"].(bool); ok && value {
		return true
	}
	return false
}

func parseOptionalTimestamp(value *string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: expires_at must be RFC3339", ErrKYCValidation)
	}
	parsed = parsed.UTC()
	return &parsed, nil
}

func resolveKYCRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrKYCNotFound
	default:
		return err
	}
}
