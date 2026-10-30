package legacyimport

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// fakeLT is an in-memory stand-in for the LedgerTrack HTTP API. It mirrors the
// behaviours of the real services that the importer depends on (lifecycle
// guards, deterministic wallet ids, unique references, idempotency keys,
// response envelopes) as read from the Go source, so the tests exercise the
// importer against the same rules. It is NOT proof that a live server behaves
// the same way.
type fakeLT struct {
	mu    sync.Mutex
	token string

	currencies []CurrencyInfo
	products   map[string]map[string]any
	prodOrder  []string
	clients    map[string]map[string]any
	cliOrder   []string
	kyc        map[string]map[string]any
	accounts   map[string]map[string]any
	acctOrder  []string
	txByRef    map[string]map[string]any
	idem       map[string]string
	txCount    int
	volumes    map[string]map[string][2]*big.Int

	nextClient, nextAcct int

	// Fault injection.
	mutating     int
	crashAfter   int  // abort every request once this many mutating requests were processed
	crashed      bool // connection aborted for all requests while true
	dropResponse func(method, path string, n int) bool
	fail503      map[string]int // path suffix -> remaining 503 answers before processing
	log          []string
}

func newFake() *fakeLT {
	return &fakeLT{
		currencies: []CurrencyInfo{{Code: "NGN", Precision: 2, Enabled: true}, {Code: "USDT", Precision: 6, Enabled: true}},
		products:   map[string]map[string]any{}, clients: map[string]map[string]any{}, kyc: map[string]map[string]any{},
		accounts: map[string]map[string]any{}, txByRef: map[string]map[string]any{}, idem: map[string]string{},
		volumes: map[string]map[string][2]*big.Int{}, fail503: map[string]int{},
	}
}

func (f *fakeLT) start(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeLT) restart() {
	f.mu.Lock()
	f.crashed, f.crashAfter, f.dropResponse = false, 0, nil
	f.mu.Unlock()
}

type fakeErr struct {
	status int
	code   string
	msg    string
}

func bad(msg string) *fakeErr      { return &fakeErr{400, "VALIDATION", msg} }
func conflict(msg string) *fakeErr { return &fakeErr{409, "CONFLICT", msg} }
func notFound(msg string) *fakeErr { return &fakeErr{404, "NOT_FOUND", msg} }

func (f *fakeLT) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mutating := r.Method != http.MethodGet
	f.log = append(f.log, r.Method+" "+r.URL.Path)
	if f.token != "" && r.Header.Get("Authorization") != "Bearer "+f.token {
		writeJSON(w, 401, map[string]any{"errorCode": "UNAUTHORIZED", "errorMessage": "missing or wrong token"})
		return
	}
	if f.crashed {
		panic(http.ErrAbortHandler)
	}
	if mutating {
		f.mutating++
		if f.crashAfter > 0 && f.mutating > f.crashAfter {
			f.crashed = true
			panic(http.ErrAbortHandler)
		}
		for suffix, n := range f.fail503 {
			if n > 0 && strings.HasSuffix(r.URL.Path, suffix) {
				f.fail503[suffix]--
				writeJSON(w, 503, map[string]any{"errorCode": "INTERNAL", "errorMessage": "try later"})
				return
			}
		}
	}
	rec := httptest.NewRecorder()
	f.route(rec, r)
	if mutating && f.dropResponse != nil && f.dropResponse(r.Method, r.URL.Path, f.mutating) {
		// The request was committed but the answer never arrives.
		panic(http.ErrAbortHandler)
	}
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func ok(w http.ResponseWriter, status int, data any) {
	writeJSON(w, status, map[string]any{"data": data})
}

func (f *fakeLT) route(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/v2/ledgertrack")
	seg := strings.Split(strings.Trim(p, "/"), "/")
	var body map[string]any
	if r.Body != nil && r.Method != http.MethodGet {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	var (
		data   any
		status = 200
		err    *fakeErr
	)
	switch {
	case r.Method == "GET" && p == "/currencies":
		data = map[string]any{"currencies": f.currencies}
	case p == "/products" && r.Method == "GET":
		list := []any{}
		for _, id := range f.prodOrder {
			list = append(list, f.products[id])
		}
		data = map[string]any{"products": list}
	case p == "/products" && r.Method == "POST":
		data, err = f.createProduct(body)
		status = 201
	case len(seg) == 3 && seg[0] == "products" && r.Method == "POST":
		data, err = f.productAction(seg[1], seg[2])
	case p == "/clients" && r.Method == "GET":
		list := []any{}
		for _, id := range f.cliOrder {
			list = append(list, f.clients[id])
		}
		data = map[string]any{"clients": list}
	case p == "/clients" && r.Method == "POST":
		data, err = f.createClient(body)
		status = 201
	case len(seg) == 2 && seg[0] == "clients" && r.Method == "GET":
		if c := f.clients[seg[1]]; c != nil {
			data = c
		} else {
			err = notFound("client not found")
		}
	case len(seg) == 3 && seg[0] == "clients" && seg[2] == "kyc" && r.Method == "GET":
		list := []any{}
		for _, k := range f.kyc {
			if k["client_id"] == seg[1] {
				list = append(list, k)
			}
		}
		data = map[string]any{"records": list}
	case len(seg) == 3 && seg[0] == "clients" && seg[2] == "kyc" && r.Method == "POST":
		data, err = f.submitKYC(seg[1], body)
		status = 201
	case len(seg) == 5 && seg[0] == "clients" && seg[2] == "kyc" && seg[4] == "verify":
		data, err = f.verifyKYC(seg[1], seg[3], body)
	case len(seg) == 3 && seg[0] == "clients" && r.Method == "POST":
		data, err = f.clientAction(seg[1], seg[2])
	case p == "/accounts" && r.Method == "GET":
		list := []any{}
		for _, id := range f.acctOrder {
			list = append(list, f.accounts[id])
		}
		data = map[string]any{"accounts": list}
	case p == "/accounts" && r.Method == "POST":
		data, err = f.openAccount(body)
		status = 201
	case len(seg) == 3 && seg[0] == "accounts" && r.Method == "POST":
		data, err = f.accountAction(seg[1], seg[2])
	case len(seg) == 2 && seg[0] == "accounts" && r.Method == "GET":
		data, err = f.ledgerAccount(seg[1])
	case p == "/transactions" && r.Method == "POST":
		data, err = f.createTx(w, r, body)
	case p == "/transactions" && r.Method == "GET":
		writeJSON(w, 200, f.listTx(r))
		return
	default:
		err = notFound("no route " + r.Method + " " + p)
	}
	if err != nil {
		writeJSON(w, err.status, map[string]any{"errorCode": err.code, "errorMessage": err.msg})
		return
	}
	ok(w, status, data)
}

func str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

func (f *fakeLT) createProduct(b map[string]any) (any, *fakeErr) {
	code := strings.TrimSpace(str(b, "code"))
	cur := strings.ToUpper(str(b, "currency"))
	if code == "" || str(b, "name") == "" || str(b, "category") == "" {
		return nil, bad("code, name and category are required")
	}
	enabled := false
	for _, c := range f.currencies {
		if c.Code == cur && c.Enabled {
			enabled = true
		}
	}
	if !enabled {
		return nil, bad("currency " + cur + " is not enabled in wallet registry")
	}
	for _, p := range f.products {
		if p["code"] == code {
			return nil, conflict("product already exists")
		}
	}
	id := fmt.Sprintf("00000000-0000-4000-8000-%012d", len(f.products)+1)
	p := map[string]any{"id": id, "code": code, "name": b["name"], "description": b["description"], "category": b["category"], "currency": cur, "status": "draft"}
	f.products[id] = p
	f.prodOrder = append(f.prodOrder, id)
	return p, nil
}

func (f *fakeLT) productAction(id, action string) (any, *fakeErr) {
	p := f.products[id]
	if p == nil {
		return nil, notFound("product not found")
	}
	switch action {
	case "activate":
		if p["status"] == "draft" {
			p["status"] = "active"
		} else if p["status"] != "active" {
			return nil, bad("cannot activate product in status " + str(p, "status"))
		}
	case "retire":
		if p["status"] != "active" && p["status"] != "retired" {
			return nil, bad("only active products can be retired")
		}
		p["status"] = "retired"
	}
	return p, nil
}

func (f *fakeLT) createClient(b map[string]any) (any, *fakeErr) {
	if str(b, "type") != "corporate" {
		return nil, bad("invalid client type")
	}
	corp, _ := b["corporate_data"].(map[string]any)
	if corp == nil || str(corp, "legal_name") == "" {
		return nil, bad("corporate clients require legal_name")
	}
	f.nextClient++
	id := fmt.Sprintf("11111111-0000-4000-8000-%012d", f.nextClient)
	contact, _ := b["contact"].(map[string]any)
	if contact == nil {
		contact = map[string]any{}
	}
	c := map[string]any{"id": id, "client_number": fmt.Sprintf("CL-2026-%06d", 100000+f.nextClient), "type": "corporate",
		"status": "pending", "kyc_level": 0, "kyc_status": "pending", "kyc_data": map[string]any{},
		"contact": contact, "corporate_data": corp}
	f.clients[id] = c
	f.cliOrder = append(f.cliOrder, id)
	return c, nil
}

func (f *fakeLT) submitKYC(clientID string, b map[string]any) (any, *fakeErr) {
	c := f.clients[clientID]
	if c == nil {
		return nil, notFound("client not found")
	}
	level := int(b["level"].(float64))
	payload, _ := b["payload"].(map[string]any)
	if payload == nil {
		payload = map[string]any{}
	}
	contact := c["contact"].(map[string]any)
	if str(contact, "phone") == "" && str(payload, "phone") == "" {
		return nil, bad("level 0 requires phone")
	}
	kd := c["kyc_data"].(map[string]any)
	if level >= 1 && str(payload, "national_id_number") == "" && str(kd, "national_id_number") == "" {
		return nil, bad("level 1 requires national_id_number")
	}
	docs, _ := b["documents"].([]any)
	hasDoc := func(t string) bool {
		for _, d := range docs {
			if dm, ok := d.(map[string]any); ok && strings.EqualFold(str(dm, "type"), t) {
				return true
			}
		}
		return false
	}
	if level >= 2 && !(hasDoc("government_id") && hasDoc("proof_of_address")) {
		return nil, bad("level 2 requires government_id and proof_of_address documents")
	}
	if level >= 3 && str(payload, "biometric_reference") == "" {
		return nil, bad("level 3 requires biometric or in-person verification evidence")
	}
	id := fmt.Sprintf("22222222-0000-4000-8000-%012d", len(f.kyc)+1)
	rec := map[string]any{"id": id, "client_id": clientID, "level": level, "status": "pending", "payload": payload, "documents": docs}
	f.kyc[id] = rec
	c["kyc_status"] = "pending"
	for k, v := range payload {
		kd[k] = v
	}
	return rec, nil
}

func (f *fakeLT) verifyKYC(clientID, kycID string, b map[string]any) (any, *fakeErr) {
	c, rec := f.clients[clientID], f.kyc[kycID]
	if c == nil || rec == nil || rec["client_id"] != clientID {
		return nil, notFound("kyc record not found")
	}
	if rec["status"] == "verified" {
		return rec, nil
	}
	rec["status"], rec["verifier"] = "verified", b["verifier"]
	if lvl := int(rec["level"].(int)); lvl > int(c["kyc_level"].(int)) {
		c["kyc_level"] = lvl
	}
	c["kyc_status"] = "verified"
	kd := c["kyc_data"].(map[string]any)
	for k, v := range rec["payload"].(map[string]any) {
		kd[k] = v
	}
	return rec, nil
}

func (f *fakeLT) clientAction(id, action string) (any, *fakeErr) {
	c := f.clients[id]
	if c == nil {
		return nil, notFound("client not found")
	}
	switch action {
	case "activate":
		switch c["status"] {
		case "active":
		case "pending":
			if c["kyc_level"].(int) < 1 || c["kyc_status"] != "verified" {
				return nil, bad("client requires verified KYC level 1 or higher")
			}
			c["status"] = "active"
		default:
			return nil, bad("cannot activate client in status " + str(c, "status"))
		}
	case "suspend":
		if c["status"] == "closed" {
			return nil, bad("cannot suspend closed client")
		}
		c["status"] = "suspended"
	case "close":
		for _, a := range f.accounts {
			if a["client_id"] == id && a["status"] != "closed" {
				return nil, bad("account " + str(a, "account_number") + " must be closed before client closure")
			}
		}
		c["status"] = "closed"
	}
	return c, nil
}

func (f *fakeLT) openAccount(b map[string]any) (any, *fakeErr) {
	c, p := f.clients[str(b, "client_id")], f.products[str(b, "product_id")]
	if c == nil || p == nil {
		return nil, bad("client or product not found")
	}
	if c["status"] != "active" {
		return nil, bad("client must be active before opening an account")
	}
	if p["status"] != "active" {
		return nil, bad("product must be active before opening an account")
	}
	wallet := fmt.Sprintf("client-%s-%s", c["client_number"], p["code"])
	for _, a := range f.accounts {
		if a["wallet_id"] == wallet {
			return nil, conflict("account already exists")
		}
	}
	f.nextAcct++
	id := fmt.Sprintf("33333333-0000-4000-8000-%012d", f.nextAcct)
	md, _ := b["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
	}
	a := map[string]any{"id": id, "account_number": fmt.Sprintf("%010d", 1000000000+f.nextAcct), "client_id": c["id"], "product_id": p["id"],
		"currency": p["currency"], "status": "pending", "wallet_id": wallet, "freeze_debits": false, "metadata": md}
	f.accounts[id] = a
	f.acctOrder = append(f.acctOrder, id)
	return a, nil
}

func (f *fakeLT) availableBalance(a map[string]any) *big.Int {
	addr := fmt.Sprintf("users:%s:wallets:%s:available", a["wallet_id"], a["currency"])
	v := f.volumes[addr][fmt.Sprintf("%s/2", a["currency"])]
	if v[0] == nil {
		return new(big.Int)
	}
	return new(big.Int).Sub(v[0], v[1])
}

func (f *fakeLT) accountAction(id, action string) (any, *fakeErr) {
	a := f.accounts[id]
	if a == nil {
		return nil, notFound("account not found")
	}
	st := str(a, "status")
	switch action {
	case "activate":
		if st == "pending" || st == "dormant" {
			a["status"] = "active"
		} else if st != "active" {
			return nil, bad("cannot activate account in status " + st)
		}
	case "suspend":
		if st == "closed" {
			return nil, bad("cannot suspend closed account")
		}
		a["status"] = "suspended"
	case "freeze":
		if st != "active" && st != "dormant" {
			return nil, bad("only active or dormant accounts can be frozen")
		}
		a["freeze_debits"] = true
	case "dormant":
		if st != "active" && st != "dormant" {
			return nil, bad("only active accounts can become dormant")
		}
		a["status"] = "dormant"
	case "close":
		if f.availableBalance(a).Sign() != 0 {
			return nil, bad("account balance must be zero before close")
		}
		a["status"] = "closed"
	}
	return a, nil
}

var sendRe = regexp.MustCompile(`(?s)^send \[(\S+) (\d+)\] \(\s*source = @(\S+?)( allowing unbounded overdraft)?\s*destination = @(\S+)\s*\)\s*$`)

func (f *fakeLT) vol(addr, asset string) *[2]*big.Int {
	if f.volumes[addr] == nil {
		f.volumes[addr] = map[string][2]*big.Int{}
	}
	v, ok := f.volumes[addr][asset]
	if !ok {
		v = [2]*big.Int{new(big.Int), new(big.Int)}
		f.volumes[addr][asset] = v
	}
	return &v
}

func (f *fakeLT) txJSON(tx map[string]any) map[string]any { return tx }

func (f *fakeLT) createTx(w http.ResponseWriter, r *http.Request, b map[string]any) (any, *fakeErr) {
	script, _ := b["script"].(map[string]any)
	ref := str(b, "reference")
	key := r.Header.Get("Idempotency-Key")
	if key != "" {
		if prev, ok := f.idem[key]; ok {
			if prev != ref {
				return nil, conflict("idempotency key reused with different input")
			}
			w.Header().Set("Idempotency-Hit", "true")
			return f.txByRef[ref], nil
		}
	}
	m := sendRe.FindStringSubmatch(strings.TrimSpace(str(script, "plain")))
	if m == nil {
		return nil, &fakeErr{400, "COMPILATION_FAILED", "unsupported script in fake: " + str(script, "plain")}
	}
	asset, amtS, src, overdraft, dst := m[1], m[2], m[3], m[4] != "", m[5]
	if ref != "" {
		if _, exists := f.txByRef[ref]; exists {
			return nil, conflict("duplicate reference " + ref)
		}
	}
	amt, _ := new(big.Int).SetString(amtS, 10)
	sv := f.vol(src, asset)
	if !overdraft && src != "world" && new(big.Int).Sub(sv[0], sv[1]).Cmp(amt) < 0 {
		return nil, &fakeErr{400, "INSUFFICIENT_FUND", "insufficient funds in " + src}
	}
	sv[1].Add(sv[1], amt)
	f.vol(dst, asset)[0].Add(f.vol(dst, asset)[0], amt)
	f.txCount++
	md := map[string]string{}
	if mm, ok := b["metadata"].(map[string]any); ok {
		for k, v := range mm {
			md[k] = fmt.Sprint(v)
		}
	}
	tx := map[string]any{"id": f.txCount, "reference": ref, "metadata": md,
		"postings": []any{map[string]any{"source": src, "destination": dst, "asset": asset, "amount": amt.String()}}}
	if ref != "" {
		f.txByRef[ref] = tx
	}
	if key != "" {
		f.idem[key] = ref
	}
	return tx, nil
}

func (f *fakeLT) listTx(r *http.Request) any {
	var q struct {
		Match map[string]string `json:"$match"`
	}
	_ = json.Unmarshal([]byte(r.URL.Query().Get("query")), &q)
	data := []any{}
	if tx, ok := f.txByRef[q.Match["reference"]]; ok && q.Match["reference"] != "" {
		data = append(data, tx)
	}
	return map[string]any{"cursor": map[string]any{"data": data, "hasMore": false}}
}

func (f *fakeLT) ledgerAccount(addr string) (any, *fakeErr) {
	vols, ok := f.volumes[addr]
	if !ok {
		return nil, notFound("account not found")
	}
	out := map[string]any{}
	for asset, v := range vols {
		out[asset] = map[string]any{"input": v[0].String(), "output": v[1].String()}
	}
	return map[string]any{"address": addr, "volumes": out}, nil
}

// ---- helpers for assertions -------------------------------------------------

func (f *fakeLT) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, l := range f.log {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func (f *fakeLT) balance(addr, asset string) *big.Int {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.volumes[addr][asset]
	if v[0] == nil {
		return new(big.Int)
	}
	return new(big.Int).Sub(v[0], v[1])
}

func (f *fakeLT) sizes() (products, clients, accounts, txs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.products), len(f.clients), len(f.accounts), len(f.txByRef)
}
