package legacyimport

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// Options configure plan, apply and verify.
type Options struct {
	Concurrency int
	FailFast    bool
	// AssetTemplate renders the ledger asset. {CCY} and {PREC} are replaced.
	// The default "{CCY}/2" mirrors what the LedgerTrack wallet layer hard-codes:
	// the asset suffix is always /2 while amounts use the registry precision.
	AssetTemplate string
	// SourceTemplate renders the migration source account, one per currency.
	SourceTemplate string
	// AdoptUnmarkedClients lets a client that already exists on the server
	// without an import marker (same legal name and registration number, still
	// pending) be adopted instead of reported as a conflict. It is the recovery
	// path when a crash left an orphan and the state file is gone.
	AdoptUnmarkedClients bool
	Logf                 func(format string, args ...any)
}

const (
	DefaultAssetTemplate  = "{CCY}/2"
	DefaultSourceTemplate = "gl:migration:opening:{CCY}"
)

var (
	assetRe   = regexp.MustCompile(`^[A-Z][A-Z0-9]{0,16}(/[0-9]{1,6})?$`)
	addressRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+(:[a-zA-Z0-9_-]+)*$`)
)

func (o *Options) defaults() {
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if o.AssetTemplate == "" {
		o.AssetTemplate = DefaultAssetTemplate
	}
	if o.SourceTemplate == "" {
		o.SourceTemplate = DefaultSourceTemplate
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
}

// Validate checks that the templates render valid names for a sample currency.
func (o Options) Validate() error {
	o.defaults()
	if a := o.asset("USD", 2); !assetRe.MatchString(a) {
		return fmt.Errorf("asset template %q renders %q which is not a valid asset", o.AssetTemplate, a)
	}
	s := o.source("USD")
	if !addressRe.MatchString(s) || s == "world" {
		return fmt.Errorf("source template %q renders %q which is not a valid, dedicated account address", o.SourceTemplate, s)
	}
	if !strings.Contains(o.SourceTemplate, "{CCY}") {
		return fmt.Errorf("source template %q must contain {CCY}: one migration source account per currency", o.SourceTemplate)
	}
	if strings.HasPrefix(s, "system:control") {
		return fmt.Errorf("source %q is under system:control, which the trial balance reconciliation requires us to avoid", s)
	}
	return nil
}

func (o Options) asset(ccy string, prec int) string {
	return strings.NewReplacer("{CCY}", ccy, "{PREC}", strconv.Itoa(prec)).Replace(o.AssetTemplate)
}

func (o Options) source(ccy string) string {
	return strings.NewReplacer("{CCY}", ccy).Replace(o.SourceTemplate)
}

// Wallet addresses, identical to the ones used by the CBA / wallet layer.
func availableAddress(walletID, ccy string) string {
	return fmt.Sprintf("users:%s:wallets:%s:available", walletID, ccy)
}

func lienAddress(walletID, ccy string) string {
	return fmt.Sprintf("users:%s:wallets:%s:lien", walletID, ccy)
}

func moveScript(asset string, amount *big.Int, source, destination string, overdraft bool) string {
	od := ""
	if overdraft {
		od = " allowing unbounded overdraft"
	}
	return fmt.Sprintf("send [%s %s] (\n  source = @%s%s\n  destination = @%s\n)\n", asset, amount.String(), source, od, destination)
}

// Snapshot is an in-memory index of what already exists on the server.
type Snapshot struct {
	mu         sync.RWMutex
	Currencies map[string]CurrencyInfo

	productsByCode   map[string]*RemoteProduct
	clientsByLegacy  map[string][]*RemoteClient
	clientsUnmarked  []*RemoteClient
	accountsByLegacy map[string][]*RemoteAccount
	accountsByWallet map[string]*RemoteAccount
}

// LoadSnapshot reads currencies, products, clients and accounts.
func LoadSnapshot(ctx context.Context, c *Client) (*Snapshot, error) {
	s := &Snapshot{Currencies: map[string]CurrencyInfo{}}
	curs, err := c.Currencies(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing currencies: %w", err)
	}
	for _, cur := range curs {
		if cur.Enabled {
			s.Currencies[strings.ToUpper(cur.Code)] = cur
		}
	}
	if err := s.refreshProducts(ctx, c); err != nil {
		return nil, err
	}
	if err := s.RefreshClients(ctx, c); err != nil {
		return nil, err
	}
	if err := s.RefreshAccounts(ctx, c); err != nil {
		return nil, err
	}
	return s, nil
}

// PrecisionMap returns currency -> precision for enabled currencies.
func (s *Snapshot) PrecisionMap() map[string]int {
	out := map[string]int{}
	for k, v := range s.Currencies {
		out[k] = v.Precision
	}
	return out
}

func (s *Snapshot) refreshProducts(ctx context.Context, c *Client) error {
	ps, err := c.ListProducts(ctx)
	if err != nil {
		return fmt.Errorf("listing products: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.productsByCode = map[string]*RemoteProduct{}
	for i := range ps {
		s.productsByCode[strings.ToUpper(ps[i].Code)] = &ps[i]
	}
	return nil
}

// RefreshClients reloads the client index.
func (s *Snapshot) RefreshClients(ctx context.Context, c *Client) error {
	cs, err := c.ListClients(ctx)
	if err != nil {
		return fmt.Errorf("listing clients: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientsByLegacy = map[string][]*RemoteClient{}
	s.clientsUnmarked = nil
	for i := range cs {
		if id := cs[i].LegacyID(); id != "" {
			s.clientsByLegacy[id] = append(s.clientsByLegacy[id], &cs[i])
		} else {
			s.clientsUnmarked = append(s.clientsUnmarked, &cs[i])
		}
	}
	return nil
}

// RefreshAccounts reloads the account index.
func (s *Snapshot) RefreshAccounts(ctx context.Context, c *Client) error {
	as, err := c.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accountsByLegacy = map[string][]*RemoteAccount{}
	s.accountsByWallet = map[string]*RemoteAccount{}
	for i := range as {
		s.accountsByWallet[as[i].WalletID] = &as[i]
		if id := as[i].LegacyID(); id != "" {
			s.accountsByLegacy[id] = append(s.accountsByLegacy[id], &as[i])
		}
	}
	return nil
}

func (s *Snapshot) product(code string) *RemoteProduct {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.productsByCode[strings.ToUpper(code)]; p != nil {
		cp := *p
		return &cp
	}
	return nil
}

func (s *Snapshot) putProduct(p *RemoteProduct) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *p
	s.productsByCode[strings.ToUpper(p.Code)] = &cp
}

// client returns the single client carrying the legacy marker. dup is true if
// several remote clients carry it, which is a data integrity problem.
func (s *Snapshot) client(sourceID string) (rc *RemoteClient, dup bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l := s.clientsByLegacy[sourceID]
	switch len(l) {
	case 0:
		return nil, false
	case 1:
		cp := *l[0]
		return &cp, false
	default:
		cp := *l[0]
		return &cp, true
	}
}

func (s *Snapshot) putClient(rc *RemoteClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *rc
	if id := cp.LegacyID(); id != "" {
		l := s.clientsByLegacy[id]
		for i := range l {
			if l[i].ID == cp.ID {
				l[i] = &cp
				return
			}
		}
		s.clientsByLegacy[id] = append(l, &cp)
		// drop from unmarked
		for i := range s.clientsUnmarked {
			if s.clientsUnmarked[i].ID == cp.ID {
				s.clientsUnmarked = append(s.clientsUnmarked[:i], s.clientsUnmarked[i+1:]...)
				break
			}
		}
		return
	}
	for i := range s.clientsUnmarked {
		if s.clientsUnmarked[i].ID == cp.ID {
			s.clientsUnmarked[i] = &cp
			return
		}
	}
	s.clientsUnmarked = append(s.clientsUnmarked, &cp)
}

// orphanCandidates returns unmarked pending clients that look like the record.
func (s *Snapshot) orphanCandidates(cl *ClientRec) []*RemoteClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*RemoteClient
	for _, rc := range s.clientsUnmarked {
		if rc.Status != "pending" || rc.Type != "corporate" || rc.legalName() != strings.TrimSpace(cl.LegalName) {
			continue
		}
		if rc.Contact.Phone != strings.TrimSpace(cl.Phone) {
			continue
		}
		if rc.CorporateData != nil && rc.CorporateData.RegistrationNumber != strings.TrimSpace(cl.RegistrationNumber) {
			continue
		}
		cp := *rc
		out = append(out, &cp)
	}
	return out
}

// lookalikes returns unmarked clients with the same legal name and registration number.
func (s *Snapshot) lookalikes(cl *ClientRec) []*RemoteClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*RemoteClient
	for _, rc := range s.clientsUnmarked {
		if rc.legalName() == strings.TrimSpace(cl.LegalName) && rc.CorporateData != nil &&
			rc.CorporateData.RegistrationNumber == strings.TrimSpace(cl.RegistrationNumber) {
			cp := *rc
			out = append(out, &cp)
		}
	}
	return out
}

func (s *Snapshot) account(sourceID string) (ra *RemoteAccount, dup bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l := s.accountsByLegacy[sourceID]
	switch len(l) {
	case 0:
		return nil, false
	case 1:
		cp := *l[0]
		return &cp, false
	default:
		cp := *l[0]
		return &cp, true
	}
}

func (s *Snapshot) accountByWallet(walletID string) *RemoteAccount {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if a := s.accountsByWallet[walletID]; a != nil {
		cp := *a
		return &cp
	}
	return nil
}

func (s *Snapshot) putAccount(ra *RemoteAccount) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *ra
	s.accountsByWallet[cp.WalletID] = &cp
	if id := cp.LegacyID(); id != "" {
		l := s.accountsByLegacy[id]
		for i := range l {
			if l[i].ID == cp.ID {
				l[i] = &cp
				return
			}
		}
		s.accountsByLegacy[id] = append(l, &cp)
	}
}
