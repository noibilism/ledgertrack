package currency

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

type Definition struct {
	Precision int
	Enabled   bool
}

type Currency struct {
	Code      string `json:"code"`
	Precision int    `json:"precision"`
	Enabled   bool   `json:"enabled"`
}

type Record struct {
	bun.BaseModel `bun:"_system.currencies,alias:currencies"`

	Code      string    `bun:"code,type:varchar(16),pk"`
	Precision int       `bun:"precision,type:int,notnull"`
	Enabled   bool      `bun:"enabled,type:boolean,notnull"`
	CreatedAt time.Time `bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt time.Time `bun:"updated_at,type:timestamp without time zone,nullzero"`
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Definition{}
)

func Load(ctx context.Context, db bun.IDB) error {
	records := make([]Record, 0)
	if err := db.NewSelect().
		Model(&records).
		OrderExpr("code asc").
		Scan(ctx); err != nil {
		return err
	}

	definitions := make(map[string]Definition, len(records))
	for _, record := range records {
		code := normalizeCode(record.Code)
		if code == "" {
			continue
		}
		definitions[code] = Definition{
			Precision: record.Precision,
			Enabled:   record.Enabled,
		}
	}

	SetDefinitions(definitions)
	return nil
}

func SetDefinitions(definitions map[string]Definition) {
	ret := make(map[string]Definition, len(definitions))
	for code, definition := range definitions {
		normalized := normalizeCode(code)
		if normalized == "" {
			continue
		}
		ret[normalized] = definition
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	registry = ret
}

func Lookup(code string) (Definition, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	definition, ok := registry[normalizeCode(code)]
	return definition, ok
}

func EnabledCodes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	ret := make([]string, 0, len(registry))
	for code, definition := range registry {
		if definition.Enabled {
			ret = append(ret, code)
		}
	}
	sort.Strings(ret)
	return ret
}

func List() []Currency {
	registryMu.RLock()
	defer registryMu.RUnlock()

	ret := make([]Currency, 0, len(registry))
	for code, definition := range registry {
		ret = append(ret, Currency{
			Code:      code,
			Precision: definition.Precision,
			Enabled:   definition.Enabled,
		})
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Code < ret[j].Code
	})
	return ret
}

func normalizeCode(code string) string {
	return strings.ToUpper(strings.TrimSpace(code))
}
