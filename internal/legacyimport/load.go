package legacyimport

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

//go:embed schema/*.json
var schemaFS embed.FS

// File names of the canonical input format.
const (
	FileProducts = "products.jsonl"
	FileClients  = "clients.jsonl"
	FileAccounts = "accounts.jsonl"
	FileBalances = "balances.jsonl"
	FileLiens    = "liens.jsonl"
)

// Issue is a validation finding.
type Issue struct {
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	Severity string `json:"severity"` // "error" or "warning"
	Message  string `json:"message"`
}

func (i Issue) String() string {
	loc := i.File
	if i.Line > 0 {
		loc = fmt.Sprintf("%s:%d", i.File, i.Line)
	}
	if loc != "" {
		return fmt.Sprintf("%s: %s: %s", i.Severity, loc, i.Message)
	}
	return fmt.Sprintf("%s: %s", i.Severity, i.Message)
}

// HasErrors reports whether any issue is an error.
func HasErrors(issues []Issue) bool {
	for _, i := range issues {
		if i.Severity == "error" {
			return true
		}
	}
	return false
}

func errIssue(file string, line int, format string, args ...any) Issue {
	return Issue{File: file, Line: line, Severity: "error", Message: fmt.Sprintf(format, args...)}
}

func warnIssue(file string, line int, format string, args ...any) Issue {
	return Issue{File: file, Line: line, Severity: "warning", Message: fmt.Sprintf(format, args...)}
}

// SchemaJSON returns the embedded JSON schema for an input file name
// (for example "products.jsonl").
func SchemaJSON(file string) ([]byte, error) {
	return schemaFS.ReadFile("schema/" + strings.TrimSuffix(file, ".jsonl") + ".schema.json")
}

func loadSchema(file string) (*gojsonschema.Schema, error) {
	raw, err := SchemaJSON(file)
	if err != nil {
		return nil, err
	}
	return gojsonschema.NewSchema(gojsonschema.NewBytesLoader(raw))
}

// LoadDataset reads the five JSONL files from dir. Each line is validated
// against its JSON schema before being decoded. products, clients and accounts
// are required; balances and liens are optional (a missing file is a warning).
func LoadDataset(dir string) (*Dataset, []Issue) {
	var issues []Issue
	ds := &Dataset{}

	var is []Issue
	ds.Products, is = loadFile(dir, FileProducts, true, func(r *ProductRec, l int) { r.Line = l })
	issues = append(issues, is...)
	ds.Clients, is = loadFile(dir, FileClients, true, func(r *ClientRec, l int) { r.Line = l })
	issues = append(issues, is...)
	ds.Accounts, is = loadFile(dir, FileAccounts, true, func(r *AccountRec, l int) { r.Line = l })
	issues = append(issues, is...)
	ds.Balances, is = loadFile(dir, FileBalances, false, func(r *BalanceRec, l int) { r.Line = l })
	issues = append(issues, is...)
	ds.Liens, is = loadFile(dir, FileLiens, false, func(r *LienRec, l int) { r.Line = l })
	issues = append(issues, is...)
	return ds, issues
}

func loadFile[T any](dir, name string, required bool, setLine func(*T, int)) ([]*T, []Issue) {
	path := filepath.Join(dir, name)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if required {
				return nil, []Issue{errIssue(name, 0, "required file not found in %s", dir)}
			}
			return nil, []Issue{warnIssue(name, 0, "file not found in %s, treated as empty", dir)}
		}
		return nil, []Issue{errIssue(name, 0, "cannot open: %v", err)}
	}
	defer f.Close()

	schema, err := loadSchema(name)
	if err != nil {
		return nil, []Issue{errIssue(name, 0, "cannot load schema: %v", err)}
	}

	var (
		out    []*T
		issues []Issue
		lineNo int
		r      = bufio.NewReaderSize(f, 1<<20)
	)
	for {
		line, rerr := r.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			if lineNo == 1 {
				line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
			}
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				rec, is := parseLine[T](name, lineNo, trimmed, schema)
				issues = append(issues, is...)
				if rec != nil {
					setLine(rec, lineNo)
					out = append(out, rec)
				}
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				issues = append(issues, errIssue(name, lineNo, "read error: %v", rerr))
			}
			break
		}
	}
	return out, issues
}

func parseLine[T any](name string, lineNo int, raw []byte, schema *gojsonschema.Schema) (*T, []Issue) {
	if !json.Valid(raw) {
		return nil, []Issue{errIssue(name, lineNo, "invalid JSON")}
	}
	res, err := schema.Validate(gojsonschema.NewBytesLoader(raw))
	if err != nil {
		return nil, []Issue{errIssue(name, lineNo, "schema validation failed to run: %v", err)}
	}
	if !res.Valid() {
		var issues []Issue
		for _, e := range res.Errors() {
			issues = append(issues, errIssue(name, lineNo, "%s", e.String()))
		}
		return nil, issues
	}
	var rec T
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, []Issue{errIssue(name, lineNo, "decode: %v", err)}
	}
	return &rec, nil
}
