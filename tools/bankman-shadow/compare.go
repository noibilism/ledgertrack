package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/shopspring/decimal"
)

// diff is one difference between Bankman's reply and LedgerTrack's.
type diff struct {
	Path     string `json:"path"`
	Bankman  string `json:"bankman"`
	LedgerTr string `json:"ledgertrack"`
}

// normalize parses a JSON body, drops ignored keys and rewrites numeric strings and numbers to one canonical
// decimal form, so "1500.0000", 1500 and "1500" compare equal.
func normalize(raw []byte, ignore map[string]bool) (any, bool) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	return clean(v, ignore), true
}

func clean(v any, ignore map[string]bool) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			if ignore[k] {
				continue
			}
			out[k] = clean(x, ignore)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = clean(x, ignore)
		}
		return out
	case json.Number:
		if d, err := decimal.NewFromString(t.String()); err == nil {
			return d.String()
		}
		return t.String()
	case string:
		if d, err := decimal.NewFromString(t); err == nil && strings.ContainsAny(t, "0123456789") && !strings.ContainsAny(t, "eE-:/ ") {
			return d.String()
		}
		return t
	default:
		return v
	}
}

// compareReplies reports the differences between two reply bodies. Non-JSON bodies are compared as text.
func compareReplies(bankman, lt []byte, ignore map[string]bool) []diff {
	bv, bok := normalize(bankman, ignore)
	lv, lok := normalize(lt, ignore)
	if !bok || !lok {
		if bytes.Equal(bytes.TrimSpace(bankman), bytes.TrimSpace(lt)) {
			return nil
		}
		return []diff{{Path: "$", Bankman: clip(string(bankman)), LedgerTr: clip(string(lt))}}
	}
	var out []diff
	walk("$", bv, lv, &out)
	return out
}

func walk(path string, b, l any, out *[]diff) {
	switch bt := b.(type) {
	case map[string]any:
		lt, ok := l.(map[string]any)
		if !ok {
			*out = append(*out, diff{path, show(b), show(l)})
			return
		}
		keys := map[string]bool{}
		for k := range bt {
			keys[k] = true
		}
		for k := range lt {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			bv, bin := bt[k]
			lv, lin := lt[k]
			switch {
			case !bin:
				*out = append(*out, diff{path + "." + k, "<absent>", show(lv)})
			case !lin:
				*out = append(*out, diff{path + "." + k, show(bv), "<absent>"})
			default:
				walk(path+"."+k, bv, lv, out)
			}
		}
	case []any:
		lt, ok := l.([]any)
		if !ok || len(lt) != len(bt) {
			*out = append(*out, diff{path + "[]", fmt.Sprintf("%d items", len(bt)), show(l)})
			if !ok {
				return
			}
		}
		for i := 0; i < len(bt) && i < len(lt); i++ {
			walk(fmt.Sprintf("%s[%d]", path, i), bt[i], lt[i], out)
		}
	default:
		if show(b) != show(l) {
			*out = append(*out, diff{path, show(b), show(l)})
		}
	}
}

func show(v any) string {
	if v == nil {
		return "null"
	}
	raw, _ := json.Marshal(v)
	return clip(string(raw))
}

func clip(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
