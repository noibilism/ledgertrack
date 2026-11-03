package main

import (
	"math/bits"
	"time"
)

// Histogram is a fixed-memory log-linear latency histogram (HDR style).
// Values are bucketed in microseconds with 64 sub-buckets per power of two,
// which bounds the relative quantile error to about 1.6%. It is not safe for
// concurrent use: every worker owns one and they are merged after the run.
type Histogram struct {
	counts [histBuckets]uint64
	n      uint64
	sum    int64 // microseconds
	max    int64 // microseconds
}

const (
	histSubBits = 6
	histSub     = 1 << histSubBits
	// 2^40 microseconds is about 12.7 days, far beyond any request timeout.
	histBuckets = histSub * 36
)

func histIndex(us int64) int {
	if us < 0 {
		us = 0
	}
	shift := bits.Len64(uint64(us)) - (histSubBits + 1)
	if shift < 0 {
		shift = 0
	}
	idx := histSub*shift + int(us>>uint(shift))
	if idx >= histBuckets {
		idx = histBuckets - 1
	}
	return idx
}

// histUpper returns the largest value (in microseconds) mapped to idx.
func histUpper(idx int) int64 {
	if idx < 2*histSub {
		return int64(idx)
	}
	shift := idx/histSub - 1
	sub := idx - histSub*shift
	return (int64(sub+1) << uint(shift)) - 1
}

func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.counts[histIndex(us)]++
	h.n++
	h.sum += us
	if us > h.max {
		h.max = us
	}
}

func (h *Histogram) Merge(o *Histogram) {
	for i := range h.counts {
		h.counts[i] += o.counts[i]
	}
	h.n += o.n
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

func (h *Histogram) Count() uint64 { return h.n }

func (h *Histogram) Max() time.Duration { return time.Duration(h.max) * time.Microsecond }

func (h *Histogram) Mean() time.Duration {
	if h.n == 0 {
		return 0
	}
	return time.Duration(h.sum/int64(h.n)) * time.Microsecond
}

// Quantile returns the value at quantile q in [0,1]. The result is the upper
// edge of the containing bucket, capped at the observed maximum.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	if q >= 1 {
		return h.Max()
	}
	rank := uint64(q*float64(h.n)) + 1
	if rank > h.n {
		rank = h.n
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			v := histUpper(i)
			if v > h.max {
				v = h.max
			}
			return time.Duration(v) * time.Microsecond
		}
	}
	return h.Max()
}
