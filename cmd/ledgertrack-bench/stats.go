package main

import (
	"sync"
	"sync/atomic"
	"time"
)

const recorderShards = 8

type opStats struct {
	all       Histogram // latency of every completed attempt, measured from intended start
	ok        Histogram // latency of 2xx responses only
	classes   [numClasses]uint64
	ledgerTxs uint64 // estimated ledger transactions committed by 2xx responses
	fallbacks uint64 // release/pay requests that ran as a lien because no lien was on hold
}

type statShard struct {
	mu    sync.Mutex
	ops   [numOps]opStats
	svc   Histogram // service time (actual send to completion), all ops, all attempts
	tlOK  []uint64  // completions of 2xx per second of the window
	tlErr []uint64
}

// recorder collects measurements from all workers. Only attempts whose
// intended start falls inside the measured window are counted, so warmup and
// post-window drain traffic never pollutes the numbers.
type recorder struct {
	shards   [recorderShards]*statShard
	winStart time.Time
	winEnd   time.Time // may be shortened if the run is interrupted
	dropped  atomic.Uint64
}

func newRecorder(winStart, winEnd time.Time) *recorder {
	r := &recorder{winStart: winStart, winEnd: winEnd}
	secs := int(winEnd.Sub(winStart)/time.Second) + 2
	for i := range r.shards {
		r.shards[i] = &statShard{tlOK: make([]uint64, secs), tlErr: make([]uint64, secs)}
	}
	return r
}

func (r *recorder) inWindow(t time.Time) bool {
	return !t.Before(r.winStart) && t.Before(r.winEnd)
}

type sample struct {
	op        Op
	intended  time.Time
	started   time.Time
	done      time.Time
	class     Class
	ledgerTxs int
	fallback  bool
}

func (r *recorder) record(worker int, s sample) {
	sh := r.shards[worker%recorderShards]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if r.inWindow(s.intended) {
		st := &sh.ops[s.op]
		lat := s.done.Sub(s.intended)
		st.all.Record(lat)
		st.classes[s.class]++
		sh.svc.Record(s.done.Sub(s.started))
		if s.class == ClassOK {
			st.ok.Record(lat)
			st.ledgerTxs += uint64(s.ledgerTxs)
		}
		if s.fallback {
			st.fallbacks++
		}
	}
	if !s.done.Before(r.winStart) && s.done.Before(r.winEnd) {
		i := int(s.done.Sub(r.winStart) / time.Second)
		if i < len(sh.tlOK) {
			if s.class == ClassOK {
				sh.tlOK[i]++
			} else if isError(s.op, s.class) {
				sh.tlErr[i]++
			}
		}
	}
}

// merged is the aggregate view used for reporting.
type merged struct {
	ops   [numOps]opStats
	svc   Histogram
	tlOK  []uint64
	tlErr []uint64
}

func (r *recorder) merge() *merged {
	m := &merged{
		tlOK:  make([]uint64, len(r.shards[0].tlOK)),
		tlErr: make([]uint64, len(r.shards[0].tlErr)),
	}
	for _, sh := range r.shards {
		sh.mu.Lock()
		for op := range sh.ops {
			src, dst := &sh.ops[op], &m.ops[op]
			dst.all.Merge(&src.all)
			dst.ok.Merge(&src.ok)
			for c := range src.classes {
				dst.classes[c] += src.classes[c]
			}
			dst.ledgerTxs += src.ledgerTxs
			dst.fallbacks += src.fallbacks
		}
		m.svc.Merge(&sh.svc)
		for i := range sh.tlOK {
			m.tlOK[i] += sh.tlOK[i]
			m.tlErr[i] += sh.tlErr[i]
		}
		sh.mu.Unlock()
	}
	return m
}
