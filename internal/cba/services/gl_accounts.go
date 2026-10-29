package services

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
)

const (
	// GLImportMaxRows bounds a single import request.
	GLImportMaxRows = 5000

	GLImportStatusCreate    = "create"
	GLImportStatusUnchanged = "unchanged"
	GLImportStatusConflict  = "conflict"
	GLImportStatusInvalid   = "invalid"
)

var (
	ErrGLAccountValidation    = errors.New("gl account validation failed")
	ErrGLAccountNotFound      = errors.New("gl account not found")
	ErrGLAccountAlreadyExists = errors.New("gl account already exists")
	// ErrGLImportRejected is returned (together with the report) when a non dry-run import was not applied
	// because at least one row is invalid or conflicts with an existing account.
	ErrGLImportRejected = errors.New("gl import rejected, nothing was applied")
)

var (
	glCodeFormat    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	glLedgerFormat  = regexp.MustCompile(`^[0-9a-zA-Z_-]{1,63}$`)
	glSegmentFormat = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

type GLAccountService interface {
	Create(context.Context, CreateGLAccountInput) (*models.GLAccount, error)
	List(context.Context, repositories.GLAccountFilter) ([]models.GLAccount, error)
	GetByCode(context.Context, string) (*models.GLAccount, error)
	Patch(context.Context, string, PatchGLAccountInput) (*models.GLAccount, error)
	// Import applies (or, with dryRun, only validates) a whole chart of accounts. It is all-or-nothing and
	// idempotent. When a non dry-run import is rejected, both the report and ErrGLImportRejected are returned.
	Import(ctx context.Context, rows []CreateGLAccountInput, dryRun bool) (*GLImportReport, error)
}

// CreateGLAccountInput is also the shape of one import row.
type CreateGLAccountInput struct {
	Code       string         `json:"code"`
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	NormalSide string         `json:"normal_side,omitempty"`
	Currency   string         `json:"currency"`
	ParentCode string         `json:"parent_code,omitempty"`
	Ledger     string         `json:"ledger"`
	Address    string         `json:"address"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// PatchGLAccountInput lists the only mutable fields. Metadata, when present, replaces the whole object.
type PatchGLAccountInput struct {
	Name     *string        `json:"name,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Status   *string        `json:"status,omitempty"`
}

type GLImportRowReport struct {
	Row       int      `json:"row"`
	Code      string   `json:"code"`
	Status    string   `json:"status"`
	Errors    []string `json:"errors,omitempty"`
	Conflicts []string `json:"conflicts,omitempty"`
}

type GLImportReport struct {
	DryRun    bool                `json:"dry_run"`
	Committed bool                `json:"committed"`
	CanCommit bool                `json:"can_commit"`
	Total     int                 `json:"total"`
	ToCreate  int                 `json:"to_create"`
	Unchanged int                 `json:"unchanged"`
	Conflicts int                 `json:"conflicts"`
	Invalid   int                 `json:"invalid"`
	Rows      []GLImportRowReport `json:"rows"`
}

type DefaultGLAccountService struct {
	repository repositories.GLAccountRepository
}

func NewGLAccountService(repository repositories.GLAccountRepository) GLAccountService {
	return &DefaultGLAccountService{repository: repository}
}

func (s *DefaultGLAccountService) Create(ctx context.Context, input CreateGLAccountInput) (*models.GLAccount, error) {
	plan, err := planGLAccounts(ctx, s.repository, []CreateGLAccountInput{input})
	if err != nil {
		return nil, err
	}

	row := plan.rows[0]
	switch row.Status {
	case GLImportStatusInvalid:
		return nil, fmt.Errorf("%w: %s", ErrGLAccountValidation, strings.Join(row.Errors, "; "))
	case GLImportStatusUnchanged:
		return nil, fmt.Errorf("%w: code %s", ErrGLAccountAlreadyExists, row.Code)
	case GLImportStatusConflict:
		return nil, fmt.Errorf("%w: %s", ErrGLAccountAlreadyExists, strings.Join(row.Conflicts, "; "))
	}

	account := plan.creates[0]
	if err := s.repository.Create(ctx, account); err != nil {
		return nil, resolveGLRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultGLAccountService) List(ctx context.Context, filter repositories.GLAccountFilter) ([]models.GLAccount, error) {
	return s.repository.List(ctx, filter)
}

func (s *DefaultGLAccountService) GetByCode(ctx context.Context, code string) (*models.GLAccount, error) {
	account, err := s.repository.GetByCode(ctx, strings.TrimSpace(code))
	if err != nil {
		return nil, resolveGLRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultGLAccountService) Patch(ctx context.Context, code string, input PatchGLAccountInput) (*models.GLAccount, error) {
	account, err := s.GetByCode(ctx, code)
	if err != nil {
		return nil, err
	}

	if input.Name != nil {
		name := strings.TrimSpace(*input.Name)
		if name == "" || len(name) > 255 {
			return nil, fmt.Errorf("%w: name is required and must be at most 255 characters", ErrGLAccountValidation)
		}
		account.Name = name
	}
	if input.Metadata != nil {
		account.Metadata = input.Metadata
	}
	if input.Status != nil {
		status := strings.ToLower(strings.TrimSpace(*input.Status))
		if status != models.GLAccountStatusActive && status != models.GLAccountStatusClosed {
			return nil, fmt.Errorf("%w: status must be one of active, closed", ErrGLAccountValidation)
		}
		if status != account.Status {
			if err := s.checkStatusTransition(ctx, account, status); err != nil {
				return nil, err
			}
			account.Status = status
		}
	}

	if err := s.repository.Update(ctx, account); err != nil {
		return nil, resolveGLRepositoryError(err)
	}
	return account, nil
}

func (s *DefaultGLAccountService) checkStatusTransition(ctx context.Context, account *models.GLAccount, target string) error {
	switch target {
	case models.GLAccountStatusClosed:
		activeStatus := models.GLAccountStatusActive
		children, err := s.repository.List(ctx, repositories.GLAccountFilter{ParentCode: &account.Code, Status: &activeStatus})
		if err != nil {
			return err
		}
		if len(children) > 0 {
			return fmt.Errorf("%w: cannot close %s, it still has %d active child account(s), starting with %s",
				ErrGLAccountValidation, account.Code, len(children), children[0].Code)
		}
	case models.GLAccountStatusActive:
		if account.ParentCode != nil {
			parent, err := s.repository.GetByCode(ctx, *account.ParentCode)
			if err != nil {
				return resolveGLRepositoryError(err)
			}
			if parent.Status != models.GLAccountStatusActive {
				return fmt.Errorf("%w: cannot reopen %s, its parent %s is closed", ErrGLAccountValidation, account.Code, parent.Code)
			}
		}
	}
	return nil
}

func (s *DefaultGLAccountService) Import(ctx context.Context, rows []CreateGLAccountInput, dryRun bool) (*GLImportReport, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: import contains no rows", ErrGLAccountValidation)
	}
	if len(rows) > GLImportMaxRows {
		return nil, fmt.Errorf("%w: import contains %d rows, the maximum is %d", ErrGLAccountValidation, len(rows), GLImportMaxRows)
	}

	if dryRun {
		plan, err := planGLAccounts(ctx, s.repository, rows)
		if err != nil {
			return nil, err
		}
		report := plan.report(true)
		return report, nil
	}

	var report *GLImportReport
	err := s.repository.RunExclusive(ctx, func(ctx context.Context, repo repositories.GLAccountRepository) error {
		// Planned again under the lock so the decision and the writes see the same state.
		plan, err := planGLAccounts(ctx, repo, rows)
		if err != nil {
			return err
		}
		report = plan.report(false)
		if !report.CanCommit {
			return nil
		}
		for _, account := range plan.creates {
			if err := repo.Create(ctx, account); err != nil {
				return resolveGLRepositoryError(err)
			}
		}
		report.Committed = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !report.Committed {
		return report, ErrGLImportRejected
	}
	return report, nil
}

// glPlan is the outcome of validating a batch of rows against the registry.
type glPlan struct {
	rows []GLImportRowReport
	// creates holds the accounts to insert, parents before children.
	creates []*models.GLAccount
}

func (p *glPlan) report(dryRun bool) *GLImportReport {
	report := &GLImportReport{DryRun: dryRun, Total: len(p.rows), Rows: p.rows}
	for _, row := range p.rows {
		switch row.Status {
		case GLImportStatusCreate:
			report.ToCreate++
		case GLImportStatusUnchanged:
			report.Unchanged++
		case GLImportStatusConflict:
			report.Conflicts++
		case GLImportStatusInvalid:
			report.Invalid++
		}
	}
	report.CanCommit = report.Conflicts == 0 && report.Invalid == 0
	return report
}

type glCandidate struct {
	index   int
	account *models.GLAccount
}

// glNode is what the relation checks need to know about an account, whether it exists already or is part of the batch.
type glNode struct {
	currency string
	typ      string
	status   string
	parent   string
}

func planGLAccounts(ctx context.Context, repo repositories.GLAccountRepository, inputs []CreateGLAccountInput) (*glPlan, error) {
	existing, err := repo.List(ctx, repositories.GLAccountFilter{})
	if err != nil {
		return nil, err
	}
	existingByCode := make(map[string]*models.GLAccount, len(existing))
	existingByAddress := make(map[string]*models.GLAccount, len(existing))
	nodes := make(map[string]glNode, len(existing)+len(inputs))
	for i := range existing {
		account := &existing[i]
		existingByCode[account.Code] = account
		existingByAddress[addressKey(account.Ledger, account.Address)] = account
		nodes[account.Code] = glNode{
			currency: account.Currency,
			typ:      account.Type,
			status:   account.Status,
			parent:   derefString(account.ParentCode),
		}
	}

	rows := make([]GLImportRowReport, len(inputs))
	candidates := make([]*glCandidate, 0, len(inputs))
	seenCode := map[string]int{}
	seenAddress := map[string]int{}

	for i, input := range inputs {
		row := &rows[i]
		row.Row = i + 1
		row.Code = strings.TrimSpace(input.Code)

		account, problems := normalizeGLInput(input)
		if row.Code != "" {
			if first, dup := seenCode[row.Code]; dup {
				problems = append(problems, fmt.Sprintf("duplicate code in this import, first seen at row %d", first))
			} else {
				seenCode[row.Code] = row.Row
			}
		}
		if len(problems) == 0 {
			key := addressKey(account.Ledger, account.Address)
			if first, dup := seenAddress[key]; dup {
				problems = append(problems, fmt.Sprintf("address %s in ledger %s is already used by row %d of this import", account.Address, account.Ledger, first))
			} else {
				seenAddress[key] = row.Row
			}
		}
		if len(problems) > 0 {
			row.Status = GLImportStatusInvalid
			row.Errors = problems
			continue
		}

		if current, ok := existingByCode[account.Code]; ok {
			if diffs := diffGLAccount(current, account, input.Metadata != nil); len(diffs) > 0 {
				row.Status = GLImportStatusConflict
				row.Conflicts = diffs
			} else {
				row.Status = GLImportStatusUnchanged
			}
			continue
		}
		if other, ok := existingByAddress[addressKey(account.Ledger, account.Address)]; ok {
			row.Status = GLImportStatusConflict
			row.Conflicts = []string{fmt.Sprintf("address: %s in ledger %s is already used by account %s", account.Address, account.Ledger, other.Code)}
			continue
		}

		row.Status = GLImportStatusCreate
		candidates = append(candidates, &glCandidate{index: i, account: account})
		nodes[account.Code] = glNode{
			currency: account.Currency,
			typ:      account.Type,
			status:   account.Status,
			parent:   derefString(account.ParentCode),
		}
	}

	rejectCandidate := func(c *glCandidate, problem string) {
		rows[c.index].Status = GLImportStatusInvalid
		rows[c.index].Errors = append(rows[c.index].Errors, problem)
		delete(nodes, c.account.Code)
	}

	// Cycles can only be created by a batch (the parent of a stored account never changes), so only
	// candidates are walked.
	for _, c := range candidates {
		if path, cyclic := findGLCycle(c.account.Code, nodes); cyclic {
			rows[c.index].Status = GLImportStatusInvalid
			rows[c.index].Errors = append(rows[c.index].Errors, "parent_code creates a cycle: "+strings.Join(path, " -> "))
		}
	}
	for _, c := range candidates {
		if rows[c.index].Status == GLImportStatusInvalid {
			delete(nodes, c.account.Code)
		}
	}

	// Parent checks, repeated until stable because rejecting a parent invalidates its children.
	for changed := true; changed; {
		changed = false
		for _, c := range candidates {
			if rows[c.index].Status != GLImportStatusCreate || c.account.ParentCode == nil {
				continue
			}
			parentCode := *c.account.ParentCode
			parent, ok := nodes[parentCode]
			switch {
			case !ok:
				rejectCandidate(c, fmt.Sprintf("parent_code %s does not exist (or is rejected in this import)", parentCode))
			case parent.currency != c.account.Currency:
				rejectCandidate(c, fmt.Sprintf("parent %s has currency %s, expected %s", parentCode, parent.currency, c.account.Currency))
			case parent.typ != c.account.Type:
				rejectCandidate(c, fmt.Sprintf("parent %s has type %s, expected %s", parentCode, parent.typ, c.account.Type))
			case parent.status != models.GLAccountStatusActive:
				rejectCandidate(c, fmt.Sprintf("parent %s is closed", parentCode))
			default:
				continue
			}
			changed = true
		}
	}

	plan := &glPlan{rows: rows}
	for _, c := range candidates {
		if rows[c.index].Status == GLImportStatusCreate {
			plan.creates = append(plan.creates, c.account)
		}
	}
	orderGLCreates(plan.creates, nodes)
	return plan, nil
}

// findGLCycle follows the parent chain starting at code and reports a cycle that goes back to code.
func findGLCycle(code string, nodes map[string]glNode) ([]string, bool) {
	path := []string{code}
	visited := map[string]struct{}{code: {}}
	current := nodes[code].parent
	for current != "" {
		path = append(path, current)
		if current == code {
			return path, true
		}
		if _, seen := visited[current]; seen {
			// walked into a cycle that does not include code, it is reported on its own members
			return nil, false
		}
		visited[current] = struct{}{}
		node, ok := nodes[current]
		if !ok {
			return nil, false
		}
		current = node.parent
	}
	return nil, false
}

// orderGLCreates sorts accounts so that every parent is inserted before its children.
func orderGLCreates(creates []*models.GLAccount, nodes map[string]glNode) {
	depth := func(code string) int {
		d := 0
		for current := nodes[code].parent; current != ""; current = nodes[current].parent {
			d++
			if d > len(nodes) {
				break
			}
		}
		return d
	}
	sort.SliceStable(creates, func(i, j int) bool {
		return depth(creates[i].Code) < depth(creates[j].Code)
	})
}

// normalizeGLInput builds the account to store and returns every field level problem found.
func normalizeGLInput(input CreateGLAccountInput) (*models.GLAccount, []string) {
	account := &models.GLAccount{
		Code:       strings.TrimSpace(input.Code),
		Name:       strings.TrimSpace(input.Name),
		Type:       strings.ToLower(strings.TrimSpace(input.Type)),
		NormalSide: strings.ToLower(strings.TrimSpace(input.NormalSide)),
		Currency:   strings.ToUpper(strings.TrimSpace(input.Currency)),
		Ledger:     strings.TrimSpace(input.Ledger),
		Address:    strings.TrimSpace(input.Address),
		Status:     models.GLAccountStatusActive,
		Metadata:   input.Metadata,
	}
	if parent := strings.TrimSpace(input.ParentCode); parent != "" {
		account.ParentCode = &parent
	}
	if account.Metadata == nil {
		account.Metadata = map[string]any{}
	}

	var problems []string

	if !glCodeFormat.MatchString(account.Code) {
		problems = append(problems, "code is required and must match "+glCodeFormat.String())
	}
	if account.Name == "" || len(account.Name) > 255 {
		problems = append(problems, "name is required and must be at most 255 characters")
	}

	switch account.Type {
	case models.GLAccountTypeAsset, models.GLAccountTypeLiability, models.GLAccountTypeEquity, models.GLAccountTypeIncome, models.GLAccountTypeExpense:
		if account.NormalSide == "" {
			account.NormalSide = models.DefaultGLNormalSide(account.Type)
		}
	default:
		problems = append(problems, "type must be one of asset, liability, equity, income, expense")
	}
	if account.NormalSide != "" && account.NormalSide != models.GLSideDebit && account.NormalSide != models.GLSideCredit {
		problems = append(problems, "normal_side must be one of debit, credit")
	}

	if account.Currency == "" {
		problems = append(problems, "currency is required")
	} else if definition, ok := currencyregistry.Lookup(account.Currency); !ok || !definition.Enabled {
		problems = append(problems, fmt.Sprintf("currency %s is not enabled in the currency registry", account.Currency))
	}

	if account.ParentCode != nil {
		switch {
		case *account.ParentCode == account.Code:
			problems = append(problems, "parent_code cannot be the account itself")
		case !glCodeFormat.MatchString(*account.ParentCode):
			problems = append(problems, "parent_code must match "+glCodeFormat.String())
		}
	}

	if !glLedgerFormat.MatchString(account.Ledger) {
		problems = append(problems, "ledger is required and must match "+glLedgerFormat.String())
	}
	if problem := validateGLAddress(account.Address); problem != "" {
		problems = append(problems, problem)
	}

	return account, problems
}

func validateGLAddress(address string) string {
	if !strings.HasPrefix(address, models.GLAddressPrefix) {
		return "address must start with " + models.GLAddressPrefix
	}
	if len(address) > 512 {
		return "address must be at most 512 characters"
	}
	for _, segment := range strings.Split(address, ":") {
		if !glSegmentFormat.MatchString(segment) {
			return fmt.Sprintf("address %q is not a valid ledger address: every ':' separated segment must match %s", address, glSegmentFormat.String())
		}
	}
	return ""
}

// diffGLAccount lists the fields that differ between the stored account and the incoming definition.
func diffGLAccount(current, incoming *models.GLAccount, compareMetadata bool) []string {
	var diffs []string
	add := func(field, have, want string) {
		if have != want {
			diffs = append(diffs, fmt.Sprintf("%s: existing %q, import %q", field, have, want))
		}
	}
	add("name", current.Name, incoming.Name)
	add("type", current.Type, incoming.Type)
	add("normal_side", current.NormalSide, incoming.NormalSide)
	add("currency", current.Currency, incoming.Currency)
	add("parent_code", derefString(current.ParentCode), derefString(incoming.ParentCode))
	add("ledger", current.Ledger, incoming.Ledger)
	add("address", current.Address, incoming.Address)
	if compareMetadata && !metadataEqual(current.Metadata, incoming.Metadata) {
		diffs = append(diffs, "metadata: differs from the existing account")
	}
	return diffs
}

func metadataEqual(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	left, errLeft := json.Marshal(a)
	right, errRight := json.Marshal(b)
	if errLeft != nil || errRight != nil {
		return false
	}
	return bytes.Equal(left, right)
}

// ParseGLAccountsCSV reads a chart of accounts. The header is required; the columns are
// code,name,type,normal_side,currency,parent_code,ledger,address (normal_side and parent_code may be omitted
// and left blank).
func ParseGLAccountsCSV(reader io.Reader) ([]CreateGLAccountInput, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))

	csvReader := csv.NewReader(bytes.NewReader(data))
	csvReader.TrimLeadingSpace = true

	header, err := csvReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: csv is empty, a header row is required", ErrGLAccountValidation)
		}
		return nil, fmt.Errorf("%w: %s", ErrGLAccountValidation, err)
	}

	known := map[string]struct{}{
		"code": {}, "name": {}, "type": {}, "normal_side": {}, "currency": {}, "parent_code": {}, "ledger": {}, "address": {},
	}
	columns := make(map[string]int, len(header))
	for i, name := range header {
		name = strings.ToLower(strings.TrimSpace(name))
		if _, ok := known[name]; !ok {
			return nil, fmt.Errorf("%w: unknown csv column %q", ErrGLAccountValidation, name)
		}
		if _, dup := columns[name]; dup {
			return nil, fmt.Errorf("%w: duplicate csv column %q", ErrGLAccountValidation, name)
		}
		columns[name] = i
	}
	for _, required := range []string{"code", "name", "type", "currency", "ledger", "address"} {
		if _, ok := columns[required]; !ok {
			return nil, fmt.Errorf("%w: csv column %q is required", ErrGLAccountValidation, required)
		}
	}

	field := func(record []string, name string) string {
		if idx, ok := columns[name]; ok && idx < len(record) {
			return record[idx]
		}
		return ""
	}

	rows := make([]CreateGLAccountInput, 0)
	for {
		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrGLAccountValidation, err)
		}
		rows = append(rows, CreateGLAccountInput{
			Code:       field(record, "code"),
			Name:       field(record, "name"),
			Type:       field(record, "type"),
			NormalSide: field(record, "normal_side"),
			Currency:   field(record, "currency"),
			ParentCode: field(record, "parent_code"),
			Ledger:     field(record, "ledger"),
			Address:    field(record, "address"),
		})
	}
	return rows, nil
}

func resolveGLRepositoryError(err error) error {
	switch {
	case postgres.IsNotFoundError(err), errors.Is(err, postgres.ErrNotFound):
		return ErrGLAccountNotFound
	case errors.Is(err, postgres.ErrConstraintsFailed{}):
		return ErrGLAccountAlreadyExists
	default:
		return err
	}
}

func addressKey(ledger, address string) string {
	return ledger + "\x00" + address
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
