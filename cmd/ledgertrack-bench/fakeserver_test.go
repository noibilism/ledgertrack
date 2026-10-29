package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeServer is a tiny in-memory imitation of the wallet API surface the
// harness uses. It mirrors the real status codes and response shapes (data
// envelope, 402 on debit, 400 INSUFFICIENT_FUND on lien, 500 for an untouched
// channel, system:control sharding) so the whole pipeline can be unit tested
// without a database. It is test code only.
type fakeServer struct {
	mu       sync.Mutex
	ledgers  map[string]map[string]int64 // ledger -> address -> balance
	refs     map[string]bool             // ledger|reference
	fees     map[string][2]int64         // channel -> user, processing
	shards   int
	requests int

	// fault injection
	dropEveryNthCredit int // acknowledge but do not apply
	creditCount        int
	failEveryNthDebit  int // return 500 without applying
	debitCount         int

	srv *httptest.Server
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{
		ledgers: map[string]map[string]int64{},
		refs:    map[string]bool{},
		fees:    map[string][2]int64{},
		shards:  4,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/{ledger}", f.getLedger)
	mux.HandleFunc("POST /v2/{ledger}", f.createLedger)
	mux.HandleFunc("POST /v2/{ledger}/wallets", f.createWallet)
	mux.HandleFunc("POST /v2/{ledger}/wallets/{id}/{op}", f.walletOp)
	mux.HandleFunc("POST /v2/{ledger}/wallets/{id}/lien/release", f.walletOp)
	mux.HandleFunc("POST /v2/{ledger}/channels", f.createChannel)
	mux.HandleFunc("PUT /v2/{ledger}/channels/{id}/fees/config", f.putFees)
	mux.HandleFunc("GET /v2/{ledger}/channels/{id}", f.getChannel)
	mux.HandleFunc("GET /v2/{ledger}/accounts/{addr}", f.getAccount)
	mux.HandleFunc("GET /v2/{ledger}/aggregate/balances", f.aggregate)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) url() string { return f.srv.URL + "/v2" }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"errorCode": code, "errorMessage": msg})
}

func (f *fakeServer) ledger(name string) map[string]int64 {
	l, ok := f.ledgers[name]
	if !ok {
		l = map[string]int64{}
		f.ledgers[name] = l
	}
	return l
}

func (f *fakeServer) getLedger(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.ledgers[r.PathValue("ledger")]; !ok {
		apiErr(w, 404, "NOT_FOUND", "ledger not found")
		return
	}
	writeJSON(w, 200, map[string]any{"data": map[string]any{"name": r.PathValue("ledger")}})
}

func (f *fakeServer) createLedger(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ledger(r.PathValue("ledger"))
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeServer) createWallet(w http.ResponseWriter, r *http.Request) {
	var req struct{ UserID, Currency string }
	_ = json.NewDecoder(r.Body).Decode(&req)
	writeJSON(w, 201, map[string]any{"data": map[string]string{"walletID": req.UserID + "-" + req.Currency}})
}

func (f *fakeServer) createChannel(w http.ResponseWriter, r *http.Request) {
	var req struct{ Currency string }
	_ = json.NewDecoder(r.Body).Decode(&req)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ledger("channels-" + req.Currency)
	id := fmt.Sprintf("chan-%d", len(f.fees)+len(f.ledgers)*1000+f.requests)
	f.requests++
	f.fees[id] = [2]int64{}
	// the real handler double wraps in "data"
	writeJSON(w, 201, map[string]any{"data": map[string]any{"data": map[string]string{"channel_id": id, "currency": req.Currency}}})
}

func parseMinor(s string) int64 {
	whole, frac, _ := strings.Cut(s, ".")
	a, _ := strconv.ParseInt(whole, 10, 64)
	b, _ := strconv.ParseInt((frac + "00")[:2], 10, 64)
	return a*100 + b
}

func (f *fakeServer) putFees(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserFee       struct{ Flat string } `json:"user_fee"`
		ProcessingFee struct{ Flat string } `json:"processing_fee"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	f.mu.Lock()
	f.fees[r.PathValue("id")] = [2]int64{parseMinor(req.UserFee.Flat), parseMinor(req.ProcessingFee.Flat)}
	f.mu.Unlock()
	writeJSON(w, 200, map[string]any{"data": map[string]any{}})
}

func volumes(asset string, bal int64) map[string]any {
	in, out := bal, int64(0)
	if bal < 0 {
		in, out = 0, -bal
	}
	return map[string]any{asset: map[string]any{"input": in, "output": out, "balance": bal}}
}

func (f *fakeServer) getAccount(w http.ResponseWriter, r *http.Request) {
	addr := r.PathValue("addr")
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.ledger(r.PathValue("ledger"))
	if addr == "world" {
		vols := map[string]any{}
		for k, bal := range l {
			if ccy, ok := strings.CutPrefix(k, "world@"); ok {
				vols[ccy+"/2"] = volumes(ccy+"/2", bal)[ccy+"/2"]
			}
		}
		if len(vols) == 0 {
			apiErr(w, 404, "NOT_FOUND", "account not found")
			return
		}
		writeJSON(w, 200, map[string]any{"data": map[string]any{"address": addr, "volumes": vols}})
		return
	}
	bal, ok := l[addr]
	if !ok {
		apiErr(w, 404, "NOT_FOUND", "account not found")
		return
	}
	ccy := addr[strings.LastIndex(addr, ":wallets:")+len(":wallets:"):]
	ccy, _, _ = strings.Cut(ccy, ":")
	writeJSON(w, 200, map[string]any{"data": map[string]any{"address": addr, "volumes": volumes(ccy+"/2", bal)}})
}

func (f *fakeServer) getChannel(w http.ResponseWriter, r *http.Request) {
	ccy := r.URL.Query().Get("currency")
	f.mu.Lock()
	defer f.mu.Unlock()
	bal, ok := f.ledger("channels-" + ccy)["channel:"+r.PathValue("id")]
	if !ok {
		apiErr(w, 500, "INTERNAL", "account not found") // matches the real handler
		return
	}
	writeJSON(w, 200, map[string]any{"data": map[string]any{"volumes": volumes(ccy+"/2", bal)}})
}

// matches mirrors storage filterAccountAddress (internal/storage/ledger/utils.go)
// for a single address pattern: no empty segment means exact match; otherwise
// every non-empty segment must match by position, and the segment count must
// match unless the pattern ends with an empty segment (prefix match).
func matches(pattern, address string) bool {
	src := strings.Split(pattern, ":")
	partial := false
	for i, s := range src {
		if s == "" || (s == "..." && i == len(src)-1) {
			partial = true
		}
	}
	if !partial {
		return pattern == address
	}
	acc := strings.Split(address, ":")
	if src[len(src)-1] != "" && len(acc) != len(src) {
		return false
	}
	for i, s := range src {
		if s == "" || s == "..." {
			continue
		}
		if i >= len(acc) || acc[i] != s {
			return false
		}
	}
	return true
}

func (f *fakeServer) aggregate(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Match map[string]string `json:"$match"`
	}
	_ = json.Unmarshal([]byte(r.URL.Query().Get("query")), &q)
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.ledgers[r.PathValue("ledger")]
	if !ok {
		apiErr(w, 404, "NOT_FOUND", "ledger not found")
		return
	}
	var total int64
	for addr, bal := range l {
		if matches(q.Match["address"], addr) {
			total += bal
		}
	}
	// the harness asks for one currency at a time; return every plausible asset
	out := map[string]int64{}
	for _, ccy := range []string{"USD", "NGN", "KES"} {
		out[ccy+"/2"] = total
	}
	// A pattern is currency specific, so only the matching asset is meaningful.
	writeJSON(w, 200, map[string]any{"data": out})
}

func (f *fakeServer) controlAddr(walletID, ccy string) string {
	sum := 0
	for _, c := range walletID {
		sum += int(c)
	}
	return fmt.Sprintf("system:control:%s:%d", ccy, sum%f.shards)
}

type walletReq struct {
	Amount        int64  `json:"amount"`
	Reference     string `json:"reference"`
	Mode          string `json:"mode"`
	ChannelID     string `json:"channelID"`
	ChannelAmount int64  `json:"channelAmount"`
}

func (f *fakeServer) walletOp(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	op := r.PathValue("op")
	if strings.HasSuffix(r.URL.Path, "/lien/release") {
		op = "release"
	}
	var req walletReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	dash := strings.LastIndex(id, "-")
	user, ccy := id[:dash], id[dash+1:]
	availA := fmt.Sprintf("users:%s:wallets:%s:available", user, ccy)
	lienA := fmt.Sprintf("users:%s:wallets:%s:lien", user, ccy)
	ctrl := f.controlAddr(id, ccy)
	ledgerName := r.PathValue("ledger")

	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.ledger(ledgerName)
	key := ledgerName + "|" + req.Reference
	if f.refs[key] {
		apiErr(w, 409, "CONFLICT", "duplicate reference")
		return
	}
	touch := func(a string) {
		if _, ok := l[a]; !ok {
			l[a] = 0
		}
	}
	touch(availA)
	touch(lienA)

	resp := map[string]any{"txid": 1, "metadata": map[string]string{}}
	finish := func() {
		f.refs[key] = true
		writeJSON(w, 201, map[string]any{"data": resp})
	}
	channelLegs := func(total int64) {
		cl := f.ledger("channels-" + ccy)
		cl["channel:"+req.ChannelID] -= req.ChannelAmount
		fees := f.fees[req.ChannelID]
		user, proc := fees[0], fees[1]
		if user == 0 && proc == 0 {
			user = total - req.ChannelAmount
		}
		rl := f.ledger("revenue-" + ccy)
		rl["revenue:accumulated"] += user
		rl["revenue:channel_processing_cost"] += proc
		resp["metadata"] = map[string]string{
			"channel_user_fee_amount":       strconv.FormatInt(user, 10),
			"channel_processing_fee_amount": strconv.FormatInt(proc, 10),
		}
	}

	switch op {
	case "credit":
		f.creditCount++
		if f.dropEveryNthCredit > 0 && f.creditCount%f.dropEveryNthCredit == 0 {
			finish() // acknowledged but lost
			return
		}
		l[ctrl] -= req.Amount
		l[availA] += req.Amount
		finish()
	case "debit":
		f.debitCount++
		if f.failEveryNthDebit > 0 && f.debitCount%f.failEveryNthDebit == 0 {
			apiErr(w, 500, "INTERNAL", "boom")
			return
		}
		if l[availA] < req.Amount {
			apiErr(w, 402, "INSUFFICIENT_FUND", "insufficient fund")
			return
		}
		l[availA] -= req.Amount
		l[ctrl] += req.Amount
		if req.ChannelID != "" {
			channelLegs(req.Amount)
		}
		finish()
	case "lien":
		if l[availA] < req.Amount {
			apiErr(w, 400, "INSUFFICIENT_FUND", "insufficient fund")
			return
		}
		l[availA] -= req.Amount
		l[lienA] += req.Amount
		finish()
	case "release":
		if l[lienA] < req.Amount {
			apiErr(w, 400, "INSUFFICIENT_FUND", "insufficient fund")
			return
		}
		l[lienA] -= req.Amount
		if strings.EqualFold(req.Mode, "PAY") {
			l["world@"+ccy] += req.Amount
			if req.ChannelID != "" {
				channelLegs(req.Amount)
			}
		} else {
			l[availA] += req.Amount
		}
		finish()
	default:
		apiErr(w, 404, "NOT_FOUND", "no such op")
	}
}
