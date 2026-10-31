package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type runner struct {
	cfg  *Config
	cl   *client
	uni  *universe
	book *book
	pool *lienPool
	rec  *recorder
	mix  *chooser
	refs refGen

	seq       atomic.Uint64
	okTotal   atomic.Uint64
	errTotal  atomic.Uint64
	shedTotal atomic.Uint64
	hardStop  atomic.Int64 // unix nanos; queued arrivals after this are dropped
	// unexpected409 counts 409 responses in the whole run (warmup included):
	// every reference is unique, so a conflict means the server saw a duplicate.
	unexpected409 atomic.Uint64
}

type runTimes struct {
	start, winStart, winEnd time.Time
	// effEnd is when the window actually ended (earlier than winEnd on interrupt).
	effEnd time.Time
}

type job struct{ intended time.Time }

func newRunner(cfg *Config, cl *client, uni *universe, bk *book, runID string) *runner {
	return &runner{
		cfg:  cfg,
		cl:   cl,
		uni:  uni,
		book: bk,
		pool: &lienPool{},
		mix:  newChooser(cfg.Mix),
		refs: refGen{runID: runID},
	}
}

// run drives the workload until warmup+duration has elapsed (or ctx ends).
func (r *runner) run(ctx context.Context) runTimes {
	start := time.Now()
	t := runTimes{
		start:    start,
		winStart: start.Add(r.cfg.Warmup),
		winEnd:   start.Add(r.cfg.Warmup + r.cfg.Duration),
	}
	r.rec = newRecorder(t.winStart, t.winEnd)
	r.hardStop.Store(t.winEnd.Add(r.cfg.Timeout).UnixNano())

	stopProgress := make(chan struct{})
	var progressWG sync.WaitGroup
	if !r.cfg.Quiet {
		progressWG.Add(1)
		go func() {
			defer progressWG.Done()
			r.progress(start, t.winStart, t.winEnd, stopProgress)
		}()
	}

	var wg sync.WaitGroup
	if r.cfg.openLoop() {
		queue := make(chan job, r.cfg.MaxBacklog)
		for i := 0; i < r.cfg.Concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				w := r.newWorker(id)
				for j := range queue {
					if time.Now().UnixNano() > r.hardStop.Load() {
						if r.rec.inWindow(j.intended) {
							r.rec.dropped.Add(1)
						}
						continue
					}
					w.process(j.intended)
				}
			}(i)
		}
		r.schedule(ctx, start, t.winEnd, queue)
		close(queue)
	} else {
		for i := 0; i < r.cfg.Concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				w := r.newWorker(id)
				for ctx.Err() == nil && time.Now().Before(t.winEnd) {
					w.process(time.Now())
				}
			}(i)
		}
	}
	wg.Wait()
	close(stopProgress)
	progressWG.Wait()

	t.effEnd = time.Now()
	if t.effEnd.After(t.winEnd) {
		t.effEnd = t.winEnd
	}
	return t
}

// schedule enqueues arrivals at their intended times. It never waits for the
// workers: when the queue is full the arrival is dropped and counted, so the
// offered load stays on schedule and latency is measured from the intended
// start (no coordinated omission).
func (r *runner) schedule(ctx context.Context, start, end time.Time, queue chan<- job) {
	rng := rand.New(rand.NewSource(r.cfg.Seed))
	gap := time.Duration(float64(time.Second) / r.cfg.Rate)
	next := start
	advance := func() {
		if r.cfg.Arrival == "poisson" {
			next = next.Add(time.Duration(rng.ExpFloat64() * float64(gap)))
		} else {
			next = next.Add(gap)
		}
	}
	for {
		now := time.Now()
		if ctx.Err() != nil {
			r.hardStop.Store(now.UnixNano())
			return
		}
		for !next.After(now) && next.Before(end) {
			select {
			case queue <- job{intended: next}:
			default:
				if r.rec.inWindow(next) {
					r.rec.dropped.Add(1)
				}
			}
			advance()
		}
		if !next.Before(end) {
			return
		}
		sleep := next.Sub(now)
		if sleep > time.Millisecond {
			sleep = time.Millisecond
		}
		select {
		case <-ctx.Done():
		case <-time.After(sleep):
		}
	}
}

func (r *runner) progress(start, winStart, winEnd time.Time, stop <-chan struct{}) {
	tk := time.NewTicker(5 * time.Second)
	defer tk.Stop()
	var lastOK, lastErr uint64
	last := start
	for {
		select {
		case <-stop:
			return
		case now := <-tk.C:
			ok, er := r.okTotal.Load(), r.errTotal.Load()
			dt := now.Sub(last).Seconds()
			phase := "run"
			if now.Before(winStart) {
				phase = "warmup"
			} else if now.After(winEnd) {
				phase = "drain"
			}
			_, _ = fmt.Fprintf(logOut, "[%6.0fs %-6s] ok %7.0f/s  err %6.1f/s  lien-pool=%d\n",
				now.Sub(start).Seconds(), phase, float64(ok-lastOK)/dt, float64(er-lastErr)/dt, r.pool.size())
			lastOK, lastErr, last = ok, er, now
		}
	}
}

type worker struct {
	r      *runner
	id     int
	rng    *rand.Rand
	picker *keyPicker
}

func (r *runner) newWorker(id int) *worker {
	rng := rand.New(rand.NewSource(r.cfg.Seed + int64(id)*7919 + 1))
	return &worker{r: r, id: id, rng: rng, picker: newKeyPicker(r.cfg, rng)}
}

// plan chooses wallet, amounts and channel for an operation. Release and pay
// consume a previously acknowledged lien; without one they degrade to a lien.
func (w *worker) plan(op Op) (effect, bool) {
	cfg := w.r.cfg
	e := effect{op: op, channel: -1}
	e.total = cfg.AmountMin + w.rng.Int63n(cfg.AmountMax-cfg.AmountMin+1)
	e.wallet = w.picker.pick()
	fallback := false
	if op == OpLienRelease || op == OpLienPay {
		if it, ok := w.r.pool.pop(); ok {
			e.wallet, e.total = it.wallet, it.amount
		} else {
			e.op = OpLien
			fallback = true
		}
	}
	e.ccy = w.r.uni.wallets[e.wallet].ccy
	if e.op.usesChannel() {
		e.channel = w.rng.Intn(cfg.Channels)
		e.userFee, e.procFee = cfg.FeeUser, cfg.FeeProc
		e.principal = e.total - e.userFee
	}
	return e, fallback
}

func (w *worker) process(intended time.Time) {
	r := w.r
	op := r.mix.pick(w.rng)
	e, fallback := w.plan(op)
	ref := r.refs.ref(e.op, r.seq.Add(1))

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	resp, err := w.send(ctx, &e, ref)
	cancel()
	done := time.Now()

	class := classify(resp.Status, resp.Body, err)
	r.book.sentOps.Add(1)
	switch {
	case class == ClassShed:
		r.book.shedOps.Add(1)
		r.shedTotal.Add(1)
		// Nothing committed: give a consumed lien back so it can be released or paid later.
		if (e.op == OpLienRelease || e.op == OpLienPay) && !fallback {
			r.pool.push(lienItem{wallet: e.wallet, amount: e.total})
		}
	case class == ClassOK:
		if e.op.usesChannel() {
			applyReportedFees(&e, resp.Body)
		}
		r.book.commit(e)
		if e.op == OpLien {
			r.pool.push(lienItem{wallet: e.wallet, amount: e.total})
		}
		r.okTotal.Add(1)
	case definitelyNotCommitted(class):
		if isError(e.op, class) {
			r.errTotal.Add(1)
		} else {
			r.okTotal.Add(1)
		}
	default:
		if class == ClassConflict {
			r.unexpected409.Add(1)
		}
		r.book.uncertain(e)
		r.errTotal.Add(1)
	}

	txs := 1
	if e.op.usesChannel() {
		txs = 2
		if e.userFee > 0 {
			txs++
		}
		if e.procFee > 0 {
			txs++
		}
	}
	r.rec.record(w.id, sample{
		op: e.op, intended: intended, started: started, done: done,
		class: class, ledgerTxs: txs, fallback: fallback,
	})
}

// send builds and issues the request for an effect.
func (w *worker) send(ctx context.Context, e *effect, ref string) (response, error) {
	r := w.r
	wallet := r.uni.wallets[e.wallet]
	base := r.cl.ledgerPath("/wallets/%s", wallet.id)
	idem := ""
	if r.cfg.Idempotency {
		idem = ref
	}
	var path string
	var body []byte
	switch e.op {
	case OpCredit:
		path = base + "/credit"
		body = fmt.Appendf(nil, `{"amount":%d,"reference":%q}`, e.total, ref)
	case OpDebit:
		path = base + "/debit"
		body = fmt.Appendf(nil, `{"amount":%d,"reference":%q}`, e.total, ref)
	case OpLien:
		path = base + "/lien"
		body = fmt.Appendf(nil, `{"amount":%d,"reference":%q}`, e.total, ref)
	case OpLienRelease:
		path = base + "/lien/release"
		body = fmt.Appendf(nil, `{"amount":%d,"reference":%q,"mode":"release_only"}`, e.total, ref)
	case OpLienPay:
		path = base + "/lien/release"
		body = fmt.Appendf(nil, `{"amount":%d,"reference":%q,"mode":"PAY","channelID":%q,"channelAmount":%d}`,
			e.total, ref, r.uni.channels[e.ccy][e.channel], e.principal)
	case OpChannelDebit:
		path = base + "/debit"
		body = fmt.Appendf(nil, `{"amount":%d,"reference":%q,"channelID":%q,"channelAmount":%d}`,
			e.total, ref, r.uni.channels[e.ccy][e.channel], e.principal)
	}
	return r.cl.do(ctx, "POST", path, body, idem)
}

// applyReportedFees replaces the configured fees by the ones the server
// reported for this transaction, when present.
func applyReportedFees(e *effect, body []byte) {
	m, err := decodeObject(body)
	if err != nil {
		return
	}
	md, _ := dig(m, "metadata")
	mm, _ := md.(map[string]any)
	if mm == nil {
		return
	}
	if v, ok := toInt64(mm["channel_user_fee_amount"]); ok {
		e.userFee = v
	}
	if v, ok := toInt64(mm["channel_processing_fee_amount"]); ok {
		e.procFee = v
	}
}

func strconvBase36(v uint64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if v == 0 {
		return "0"
	}
	var b [13]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = digits[v%36]
		v /= 36
	}
	return string(b[i:])
}
