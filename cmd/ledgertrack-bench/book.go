package main

import (
	"sync"
	"sync/atomic"
)

// effect describes the ledger movement one operation is expected to cause.
// total is what leaves/enters the wallet leg; principal and the two fees are
// only set for operations that touch a channel.
type effect struct {
	op        Op
	wallet    int
	ccy       int
	channel   int // channel index within the currency, -1 when unused
	total     int64
	principal int64
	userFee   int64
	procFee   int64
}

// book accumulates, lock free, what the ledger should look like after the
// run if every acknowledged operation committed exactly once. Operations whose
// outcome is unknown (timeouts, 5xx, partial failures) are added to the
// *Unc counters, which the consistency check uses as tolerances.
type book struct {
	channels int

	wAvail, wLien       []atomic.Int64
	wUncAvail, wUncLien []atomic.Int64

	ccy []ccyBook

	chanDelta []atomic.Int64 // index ccy*channels+channel, balance delta (<= 0)
	chanUnc   []atomic.Int64

	uncertainOps atomic.Uint64
}

type ccyBook struct {
	control, uncControl atomic.Int64 // system:control balance delta (all shards)
	world, uncWorld     atomic.Int64 // @world balance delta (lien payouts)
	revAcc, revProc     atomic.Int64 // revenue ledger balances deltas
	uncRev              atomic.Int64
}

func newBook(wallets, currencies, channels int) *book {
	return &book{
		channels:  channels,
		wAvail:    make([]atomic.Int64, wallets),
		wLien:     make([]atomic.Int64, wallets),
		wUncAvail: make([]atomic.Int64, wallets),
		wUncLien:  make([]atomic.Int64, wallets),
		ccy:       make([]ccyBook, currencies),
		chanDelta: make([]atomic.Int64, currencies*max(channels, 1)),
		chanUnc:   make([]atomic.Int64, currencies*max(channels, 1)),
	}
}

func (b *book) chanIdx(e effect) int { return e.ccy*b.channels + e.channel }

// commit records an acknowledged (2xx) operation.
func (b *book) commit(e effect) {
	c := &b.ccy[e.ccy]
	switch e.op {
	case OpCredit:
		b.wAvail[e.wallet].Add(e.total)
		c.control.Add(-e.total)
	case OpDebit:
		b.wAvail[e.wallet].Add(-e.total)
		c.control.Add(e.total)
	case OpLien:
		b.wAvail[e.wallet].Add(-e.total)
		b.wLien[e.wallet].Add(e.total)
	case OpLienRelease:
		b.wLien[e.wallet].Add(-e.total)
		b.wAvail[e.wallet].Add(e.total)
	case OpLienPay:
		b.wLien[e.wallet].Add(-e.total)
		c.world.Add(e.total)
		b.commitChannel(c, e)
	case OpChannelDebit:
		b.wAvail[e.wallet].Add(-e.total)
		c.control.Add(e.total)
		b.commitChannel(c, e)
	}
}

func (b *book) commitChannel(c *ccyBook, e effect) {
	b.chanDelta[b.chanIdx(e)].Add(-e.principal)
	c.revAcc.Add(e.userFee)
	c.revProc.Add(e.procFee)
}

// uncertain records an operation that may or may not have committed.
func (b *book) uncertain(e effect) {
	b.uncertainOps.Add(1)
	c := &b.ccy[e.ccy]
	switch e.op {
	case OpCredit, OpDebit:
		b.wUncAvail[e.wallet].Add(e.total)
		c.uncControl.Add(e.total)
	case OpLien, OpLienRelease:
		b.wUncAvail[e.wallet].Add(e.total)
		b.wUncLien[e.wallet].Add(e.total)
	case OpLienPay:
		b.wUncLien[e.wallet].Add(e.total)
		c.uncWorld.Add(e.total)
		b.uncertainChannel(c, e)
	case OpChannelDebit:
		b.wUncAvail[e.wallet].Add(e.total)
		c.uncControl.Add(e.total)
		b.uncertainChannel(c, e)
	}
}

func (b *book) uncertainChannel(c *ccyBook, e effect) {
	b.chanUnc[b.chanIdx(e)].Add(e.principal)
	c.uncRev.Add(e.total - e.principal + e.procFee)
}

// lienPool holds acknowledged liens that a later release/pay can consume, so
// that releases always target an amount the wallet really has on hold.
type lienPool struct {
	mu    sync.Mutex
	items []lienItem
}

type lienItem struct {
	wallet int
	amount int64
}

const lienPoolCap = 1 << 20

func (p *lienPool) push(it lienItem) {
	p.mu.Lock()
	if len(p.items) < lienPoolCap {
		p.items = append(p.items, it)
	}
	p.mu.Unlock()
}

func (p *lienPool) pop() (lienItem, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.items)
	if n == 0 {
		return lienItem{}, false
	}
	it := p.items[n-1]
	p.items = p.items[:n-1]
	return it, true
}

func (p *lienPool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.items)
}
