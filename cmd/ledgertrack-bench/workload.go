package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
)

// universe is the set of entities the run operates on, created in setup.
type universe struct {
	wallets  []walletRef
	channels [][]string // [currency index][channel index] -> channel id
}

type walletRef struct {
	user string // user id; the wallet id is user + "-" + currency
	id   string // wallet id used in URLs
	ccy  int    // index into Config.Currencies
}

func walletUser(prefix string, i int) string { return fmt.Sprintf("%s-w%06d", prefix, i) }

func newUniverse(cfg *Config) *universe {
	u := &universe{wallets: make([]walletRef, cfg.Wallets)}
	for i := range u.wallets {
		ci := i % len(cfg.Currencies)
		user := walletUser(cfg.Prefix, i)
		u.wallets[i] = walletRef{user: user, id: user + "-" + cfg.Currencies[ci], ccy: ci}
	}
	return u
}

// chooser draws operations according to the configured weights.
type chooser struct {
	ops []Op
	cum []float64
}

func newChooser(mix []MixEntry) *chooser {
	c := &chooser{}
	total := 0.0
	for _, m := range mix {
		total += m.Weight
		c.ops = append(c.ops, m.Op)
		c.cum = append(c.cum, total)
	}
	for i := range c.cum {
		c.cum[i] /= total
	}
	return c
}

func (c *chooser) pick(r *rand.Rand) Op {
	x := r.Float64()
	i := sort.SearchFloat64s(c.cum, x)
	if i >= len(c.ops) {
		i = len(c.ops) - 1
	}
	return c.ops[i]
}

// keyPicker selects a wallet index. Rank 0 is the hottest wallet under zipf.
type keyPicker struct {
	n    int
	r    *rand.Rand
	zipf *rand.Zipf
}

func newKeyPicker(cfg *Config, r *rand.Rand) *keyPicker {
	k := &keyPicker{n: cfg.Wallets, r: r}
	if cfg.Dist == "zipf" {
		k.zipf = rand.NewZipf(r, cfg.ZipfS, 1, uint64(cfg.Wallets-1))
	}
	return k
}

func (k *keyPicker) pick() int {
	if k.zipf != nil {
		return int(k.zipf.Uint64())
	}
	return k.r.Intn(k.n)
}

// refGen produces globally unique references: runID-op-sequence.
type refGen struct{ runID string }

func (g refGen) ref(op Op, seq uint64) string {
	return g.runID + "-" + opShort[op] + "-" + strconv.FormatUint(seq, 36)
}

var opShort = [numOps]string{"cr", "db", "ln", "rl", "pay", "cd"}
