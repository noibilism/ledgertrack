package legacyimport

import (
	"fmt"
	"sort"
	"strings"
)

// StatusReport renders the state file: counts per kind and status, then every
// item that is not done.
func StatusReport(st *State, limit int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "target:  %s\nruns:    %d\nupdated: %s\n", st.Target, st.Runs, st.Updated.Format("2006-01-02 15:04:05 MST"))
	counts := st.Counts()
	kinds := make([]string, 0, len(counts))
	for k := range counts {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	if len(kinds) == 0 {
		b.WriteString("no items recorded yet\n")
	}
	for _, k := range kinds {
		fmt.Fprintf(&b, "%-16s", k)
		sts := make([]string, 0, len(counts[k]))
		for s := range counts[k] {
			sts = append(sts, s)
		}
		sort.Strings(sts)
		for _, s := range sts {
			fmt.Fprintf(&b, " %s=%d", s, counts[k][s])
		}
		b.WriteString("\n")
	}
	problems := st.Problems()
	if len(problems) > 0 {
		fmt.Fprintf(&b, "\n%d item(s) not done:\n", len(problems))
	}
	for i, p := range problems {
		if limit > 0 && i >= limit {
			fmt.Fprintf(&b, "... %d more\n", len(problems)-limit)
			break
		}
		msg := p.Error
		if msg == "" {
			msg = p.Detail
		}
		fmt.Fprintf(&b, "  %-9s %-16s %s: %s\n", p.Status, p.Kind, p.SourceID, msg)
	}
	return b.String()
}
