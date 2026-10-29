package legacyimport

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

// expectedTx is the single posting every import transaction must contain.
type expectedTx struct {
	Reference   string
	Source      string
	Destination string
	Asset       string
	Amount      *big.Int
	LegacyID    string
}

// openingTx builds the opening balance transaction for one account:
//
//	gl:migration:opening:{ccy} (overdraft allowed) -> users:{wallet}:wallets:{ccy}:available
//
// It never touches system:control, so legacy and new trial balances agree.
func buildOpening(opt Options, prec int, b *BalanceRec, ra *RemoteAccount) (TxRequest, expectedTx) {
	asset := opt.asset(b.Currency, prec)
	src := opt.source(b.Currency)
	dst := availableAddress(ra.WalletID, ra.Currency)
	ref := "import:opening:" + b.SourceID
	md := map[string]string{
		metaLegacySourceID:  b.SourceID,
		metaLegacyImport:    "true",
		metaLegacyAcctSrc:   b.AccountSourceID,
		"cba_operation":     "legacy_opening_balance",
		"account_id":        ra.ID,
		"wallet_id":         ra.WalletID,
		metaLegacyAsOf:      b.AsOf,
		"source_account":    src,
		"opening_total":     b.Amount,
		"opening_available": b.Available,
		"opening_lien":      b.Lien,
	}
	if b.AsOf == "" {
		delete(md, metaLegacyAsOf)
	}
	req := TxRequest{
		Script:    TxScript{Plain: moveScript(asset, b.total, src, dst, true), Vars: map[string]any{}},
		Reference: ref,
		Metadata:  md,
		Runtime:   "machine",
	}
	return req, expectedTx{Reference: ref, Source: src, Destination: dst, Asset: asset, Amount: b.total, LegacyID: b.SourceID}
}

// buildLien moves part of the opening balance from available to the lien
// address. The transaction reference is the original legacy reference.
func buildLien(opt Options, prec int, ccy string, l *LienRec, ra *RemoteAccount) (TxRequest, expectedTx) {
	asset := opt.asset(ccy, prec)
	src := availableAddress(ra.WalletID, ra.Currency)
	dst := lienAddress(ra.WalletID, ra.Currency)
	md := map[string]string{
		metaLegacySourceID:  l.SourceID,
		metaLegacyImport:    "true",
		metaLegacyAcctSrc:   l.AccountSourceID,
		metaLegacyReference: l.Reference,
		metaLegacyCreatedAt: l.CreatedAt,
		"cba_operation":     "legacy_lien",
		"account_id":        ra.ID,
		"wallet_id":         ra.WalletID,
	}
	if l.Description != "" {
		md["description"] = l.Description
	}
	req := TxRequest{
		Script:    TxScript{Plain: moveScript(asset, l.amount, src, dst, false), Vars: map[string]any{}},
		Reference: l.Reference,
		Metadata:  md,
		Runtime:   "machine",
	}
	return req, expectedTx{Reference: l.Reference, Source: src, Destination: dst, Asset: asset, Amount: l.amount, LegacyID: l.SourceID}
}

// matchTx compares an existing transaction with what the import would post.
func matchTx(tx *Tx, want expectedTx) error {
	if got := tx.Metadata[metaLegacySourceID]; got != want.LegacyID {
		return fmt.Errorf("transaction %s with reference %q exists but carries legacy_source_id %q, expected %q", tx.ID, want.Reference, got, want.LegacyID)
	}
	if len(tx.Postings) != 1 {
		return fmt.Errorf("transaction %s has %d postings, expected 1", tx.ID, len(tx.Postings))
	}
	p := tx.Postings[0]
	if p.Source != want.Source || p.Destination != want.Destination || p.Asset != want.Asset {
		return fmt.Errorf("transaction %s moves %s %s -> %s, expected %s %s -> %s", tx.ID, p.Asset, p.Source, p.Destination, want.Asset, want.Source, want.Destination)
	}
	if p.Amount.Int == nil || p.Amount.Int.Cmp(want.Amount) != 0 {
		return fmt.Errorf("transaction %s amount %v differs from expected %s", tx.ID, p.Amount.Int, want.Amount)
	}
	return nil
}

func productBody(p *ProductRec) map[string]any {
	desc := strings.TrimSpace(p.Description)
	marker := productMarkerPrefix + p.SourceID + "]"
	if desc != "" {
		desc += " "
	}
	body := map[string]any{
		"code":        p.Code,
		"name":        p.Name,
		"description": desc + marker,
		"category":    p.Category,
		"currency":    p.Currency,
	}
	for k, raw := range map[string]json.RawMessage{"rules": p.Rules, "interest_config": p.InterestConfig, "fee_schedule": p.FeeSchedule} {
		if len(raw) > 0 && string(raw) != "null" {
			body[k] = raw
		}
	}
	return body
}

func clientBody(c *ClientRec) map[string]any {
	contact := map[string]any{"phone": strings.TrimSpace(c.Phone)}
	if c.Email != "" {
		contact["email"] = strings.TrimSpace(c.Email)
	}
	if c.Address != nil {
		contact["address"] = c.Address
	}
	corp := map[string]any{"legal_name": strings.TrimSpace(c.LegalName)}
	for k, v := range map[string]string{
		"trading_name":          c.TradingName,
		"registration_number":   c.RegistrationNumber,
		"tax_id":                c.TaxID,
		"incorporation_country": c.IncorporationCountry,
		"industry":              c.Industry,
	} {
		if strings.TrimSpace(v) != "" {
			corp[k] = strings.TrimSpace(v)
		}
	}
	return map[string]any{"type": "corporate", "contact": contact, "corporate_data": corp}
}

// kycBody builds the KYC submission. The payload is merged into the client's
// kyc_data by LedgerTrack, which is how the legacy source_id lands on the
// client (clients have no metadata field).
func kycBody(c *ClientRec) map[string]any {
	payload := map[string]any{
		metaLegacySourceID: c.SourceID,
		metaLegacyImport:   "true",
		metaKYCSource:      c.KYCSource,
	}
	if c.LegacyCreatedAt != "" {
		payload[metaLegacyCreatedAt] = c.LegacyCreatedAt
	}
	docs := []map[string]any{}
	if c.KYCLevel >= 1 {
		payload["national_id_number"] = c.kycIdentifier()
	}
	if c.KYCLevel >= 2 {
		for _, t := range []string{"government_id", "proof_of_address"} {
			docs = append(docs, map[string]any{
				"type":      t,
				"reference": "legacy-import:" + c.SourceID,
				"provider":  KYCVerifier,
				"metadata":  map[string]any{"synthesized": true, "kyc_source": c.KYCSource},
			})
		}
	}
	if c.KYCLevel >= 3 {
		payload["biometric_reference"] = "legacy-import:" + c.SourceID
	}
	return map[string]any{"level": c.KYCLevel, "documents": docs, "payload": payload}
}

func accountMetadata(a *AccountRec) map[string]any {
	md := map[string]any{
		metaLegacySourceID:   a.SourceID,
		metaLegacyImport:     "true",
		metaLegacyAccountNum: a.LegacyAccountNumber,
		metaLegacyClientSrc:  a.ClientSourceID,
	}
	if a.OpenedAt != "" {
		md[metaLegacyOpenedAt] = a.OpenedAt
	}
	return md
}

func walletIDFor(clientNumber, productCode string) string {
	return fmt.Sprintf("client-%s-%s", strings.TrimSpace(clientNumber), strings.TrimSpace(productCode))
}

// Status ranks used to move objects forward only.
func clientRank(s string) int {
	switch s {
	case "pending":
		return 0
	case "active":
		return 1
	case "suspended":
		return 2
	case "closed":
		return 3
	}
	return -1
}

func accountRank(s string) int {
	switch s {
	case "pending":
		return 0
	case "active", "frozen":
		return 1
	case "dormant", "suspended":
		return 2
	case "closed":
		return 3
	}
	return -1
}
