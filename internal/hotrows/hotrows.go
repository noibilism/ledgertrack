// Package hotrows picks the ledger accounts that relieve rows every channel
// transfer would otherwise update.
//
// A channel transfer writes (see docs/design/channel-hot-rows.md):
//
//	channels-{CCY}: channel:{id} -> world                        (leg 2)
//	revenue-{CCY}:  world -> revenue:accumulated                 (leg 3, user fee)
//	revenue-{CCY}:  world -> revenue:channel_processing_cost     (leg 3, processing fee)
//
// Each ledger transaction upserts one accounts_volumes row per account it touches
// and keeps that row locked until commit, so `world` (twice), revenue:accumulated
// and revenue:channel_processing_cost serialise every transfer of a currency.
//
// Two opt-in knobs spread those rows. Both pick the shard as
// fnv32a(business reference) mod N, a pure function of the transfer reference, so
// a retry or a recovery run lands on the same accounts (the ledger rejects a
// second use of the reference, which is what makes each leg idempotent). N = 1 is
// the historical layout, byte for byte.
//
//   - revenue shards (--revenue-shards): the revenue leg becomes
//     world:{s} -> revenue:accumulated:{s} (and revenue:channel_processing_cost:{s}).
//     The credit side and the counter side move together: sharding only the
//     credit side leaves the single `world` row in place and gains nothing.
//   - channel world shards (--channel-world-shards): the channel leg becomes
//     channel:{id} -> world:{s}.
//
// Sharded counter accounts (world:{s}) are not the special `world` account, so a
// posting from them must allow an unbounded overdraft. Readers must not assume one
// address: use Pattern(base) to select a base address and all its shards, and
// IsShardOf(base, address) to classify a posting. Changing N never rewrites
// history; old balances stay where they were posted and readers sum everything.
package hotrows

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	// AccumulatedBase is the account that collects user fees.
	AccumulatedBase = "revenue:accumulated"
	// ProcessingCostBase is the account that collects channel processing cost.
	ProcessingCostBase = "revenue:channel_processing_cost"
	// World is the ledger's unbounded source/sink account.
	World = "world"

	// MaxShards bounds the flags so a typo cannot create thousands of rows.
	MaxShards = 1024
)

var (
	revenueShards      atomic.Uint32
	channelWorldShards atomic.Uint32
)

func init() {
	revenueShards.Store(1)
	channelWorldShards.Store(1)
}

func normalize(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	return min(n, MaxShards)
}

// SetRevenueShards sets the process wide revenue shard count and returns the effective value.
func SetRevenueShards(n uint32) uint32 {
	n = normalize(n)
	revenueShards.Store(n)
	return n
}

// SetChannelWorldShards sets the process wide channel `world` shard count and returns the effective value.
func SetChannelWorldShards(n uint32) uint32 {
	n = normalize(n)
	channelWorldShards.Store(n)
	return n
}

func RevenueShards() uint32      { return revenueShards.Load() }
func ChannelWorldShards() uint32 { return channelWorldShards.Load() }

// ShardFor returns the shard of a business reference for n shards.
func ShardFor(reference string, n uint32) uint32 {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(reference))
	return h.Sum32() % n
}

func sharded(base, reference string, n uint32) string {
	if n <= 1 {
		return base
	}
	return fmt.Sprintf("%s:%d", base, ShardFor(reference, n))
}

// RevenueDestinationN returns base (AccumulatedBase or ProcessingCostBase) or its shard for n shards.
func RevenueDestinationN(base, reference string, n uint32) string { return sharded(base, reference, n) }

// RevenueSourceN returns the counter account of a revenue posting for n shards.
func RevenueSourceN(reference string, n uint32) string { return sharded(World, reference, n) }

// ChannelSinkN returns the counter account of a channel posting for n shards.
func ChannelSinkN(reference string, n uint32) string { return sharded(World, reference, n) }

// RevenueDestination uses the configured revenue shard count. reference MUST be the
// business reference of the transfer, without a per leg suffix such as -user-fee.
func RevenueDestination(base, reference string) string {
	return RevenueDestinationN(base, reference, RevenueShards())
}

// RevenueSource uses the configured revenue shard count.
func RevenueSource(reference string) string { return RevenueSourceN(reference, RevenueShards()) }

// ChannelSink uses the configured channel world shard count.
func ChannelSink(reference string) string { return ChannelSinkN(reference, ChannelWorldShards()) }

// IsUnboundedSource reports whether a posting from address needs "allowing unbounded
// overdraft" (everything except the real world account).
func IsUnboundedSource(address string) bool { return address != World }

// SourceClause renders the numscript source expression for address.
func SourceClause(address string) string {
	if IsUnboundedSource(address) {
		return "@" + address + " allowing unbounded overdraft"
	}
	return "@" + address
}

// Pattern is the ledger address filter that selects base and every shard of it.
// The trailing empty segment makes the filter partial (see filterAccountAddress
// in internal/storage/ledger/utils.go): non empty segments must match and no
// length is enforced for balances, so it matches "revenue:accumulated" and
// "revenue:accumulated:7". A plain Match on base is an exact comparison and
// silently misses every shard. NOTE: the transactions filter
// (filterAccountAddressOnTransactions) counts the trailing empty segment, so
// there it matches the shards only and the base must be matched separately.
func Pattern(base string) string { return base + ":" }

// IsShardOf reports whether address is base or one of its numbered shards.
func IsShardOf(base, address string) bool {
	if address == base {
		return true
	}
	rest, ok := strings.CutPrefix(address, base+":")
	if !ok || rest == "" {
		return false
	}
	_, err := strconv.ParseUint(rest, 10, 32)
	return err == nil
}

// EquivalentAccount reports whether a and b are the same logical hot-row account: equal, or the
// same base with and without a numbered shard (revenue:accumulated and revenue:accumulated:3, world
// and world:7, world:1 and world:2, system:control:USD and system:control:USD:5, ...). A replay of a
// leg that already exists on the ledger (the reference is a duplicate) must compare the existing
// posting with the one it would post using this, not ==: the shard count may have changed since the
// first attempt, and the hash of the reference then points to another shard than the one the
// posting was made on.
func EquivalentAccount(a, b string) bool {
	if a == b {
		return true
	}
	for _, base := range []string{AccumulatedBase, ProcessingCostBase, World} {
		if IsShardOf(base, a) && IsShardOf(base, b) {
			return true
		}
	}
	if ca, ok := systemControlCurrency(a); ok {
		if cb, ok := systemControlCurrency(b); ok {
			return ca == cb
		}
	}
	return false
}

const systemControlPrefix = "system:control:"

// systemControlCurrency returns the currency of system:control:{CCY} or system:control:{CCY}:{shard}.
func systemControlCurrency(address string) (string, bool) {
	rest, ok := strings.CutPrefix(address, systemControlPrefix)
	if !ok || rest == "" {
		return "", false
	}
	ccy, shard, hasShard := strings.Cut(rest, ":")
	if ccy == "" {
		return "", false
	}
	if hasShard {
		if _, err := strconv.ParseUint(shard, 10, 32); err != nil {
			return "", false
		}
	}
	return ccy, true
}

// SystemControlShards returns the number of system:control shards configured with
// CBA_SYSTEM_CONTROL_SHARDS (1 when unset or invalid).
func SystemControlShards() uint32 {
	v := strings.TrimSpace(os.Getenv("CBA_SYSTEM_CONTROL_SHARDS"))
	if v == "" {
		return 1
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return 1
	}
	return uint32(n)
}

// SystemControlAddress returns the control account that the wallet credit and debit paths use for a
// wallet: system:control:{CCY}, or system:control:{CCY}:{s} with s = fnv32a(seed) mod N when
// CBA_SYSTEM_CONTROL_SHARDS=N > 1. seed is the wallet id, so every operation of one wallet hits the
// same shard, including the user leg of an atomic transfer.
func SystemControlAddress(seed, currency string) string {
	shards := SystemControlShards()
	if shards <= 1 {
		return systemControlPrefix + currency
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	return fmt.Sprintf("%s%s:%d", systemControlPrefix, currency, h.Sum32()%shards)
}
