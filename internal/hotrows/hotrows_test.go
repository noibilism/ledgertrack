package hotrows

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultsKeepTheHistoricalAddresses(t *testing.T) {
	// Not parallel: the package level configuration is shared.
	SetRevenueShards(1)
	SetChannelWorldShards(1)

	require.Equal(t, "revenue:accumulated", RevenueDestination(AccumulatedBase, "ref-1"))
	require.Equal(t, "revenue:channel_processing_cost", RevenueDestination(ProcessingCostBase, "ref-1"))
	require.Equal(t, "world", RevenueSource("ref-1"))
	require.Equal(t, "world", ChannelSink("ref-1"))
	require.Equal(t, "@world", SourceClause(RevenueSource("ref-1")), "world keeps its plain numscript source")
}

func TestShardingIsADeterministicFunctionOfTheReference(t *testing.T) {
	t.Parallel()

	for _, n := range []uint32{2, 3, 16, 64} {
		for i := 0; i < 500; i++ {
			ref := fmt.Sprintf("transfer-%d", i)
			first := RevenueDestinationN(AccumulatedBase, ref, n)
			// An inline attempt, a client retry and the recovery runner all call this with the
			// same business reference and must land on the same accounts.
			for attempt := 0; attempt < 3; attempt++ {
				require.Equal(t, first, RevenueDestinationN(AccumulatedBase, ref, n))
			}
			shard := ShardFor(ref, n)
			require.Less(t, shard, n)
			require.Equal(t, fmt.Sprintf("revenue:accumulated:%d", shard), first)
			// Both fee entries and the counter accounts of one transfer share the shard.
			require.Equal(t, fmt.Sprintf("revenue:channel_processing_cost:%d", shard), RevenueDestinationN(ProcessingCostBase, ref, n))
			require.Equal(t, fmt.Sprintf("world:%d", shard), RevenueSourceN(ref, n))
			require.Equal(t, fmt.Sprintf("world:%d", shard), ChannelSinkN(ref, n))
		}
	}
}

// Pins the hash: changing it would silently move retries of in flight transfers to another shard
// (the ledger would then post the same reference a second time on a different account).
func TestShardForIsStable(t *testing.T) {
	t.Parallel()

	require.EqualValues(t, 0, ShardFor("anything", 1))
	require.EqualValues(t, 0, ShardFor("anything", 0))
	got := map[string]uint32{}
	for _, ref := range []string{"a", "ref-1", "bench-r1-channel_debit-000001", "00000000-0000-0000-0000-000000000000"} {
		got[ref] = ShardFor(ref, 16)
	}
	require.Equal(t, map[string]uint32{
		"a":                                    ShardFor("a", 16),
		"ref-1":                                ShardFor("ref-1", 16),
		"bench-r1-channel_debit-000001":        ShardFor("bench-r1-channel_debit-000001", 16),
		"00000000-0000-0000-0000-000000000000": ShardFor("00000000-0000-0000-0000-000000000000", 16),
	}, got)
	// fnv32a("a") = 0xe40c292c
	require.EqualValues(t, 0xe40c292c%16, ShardFor("a", 16))
	require.EqualValues(t, 0xe40c292c%7, ShardFor("a", 7))
}

func TestShardsAreSpreadEvenly(t *testing.T) {
	t.Parallel()

	const n, refs = 16, 32000
	counts := make([]int, n)
	for i := 0; i < refs; i++ {
		counts[ShardFor(fmt.Sprintf("bench-r1-channel_debit-%06d", i), n)]++
	}
	for shard, c := range counts {
		// expected 2000 per shard; a skew above 15% would defeat the purpose
		require.InDelta(t, refs/n, c, 0.15*refs/n, "shard %d", shard)
	}
}

func TestLegSuffixChangesTheShard(t *testing.T) {
	t.Parallel()

	// Documents why callers must pass the BUSINESS reference: the per leg reference
	// (ref-user-fee) usually hashes elsewhere, so hashing it would split one transfer's legs.
	moved := 0
	for i := 0; i < 200; i++ {
		ref := fmt.Sprintf("r-%d", i)
		if ShardFor(ref, 16) != ShardFor(ref+"-user-fee", 16) {
			moved++
		}
	}
	require.Greater(t, moved, 100)
}

func TestSetShardsNormalises(t *testing.T) {
	defer SetRevenueShards(1)
	defer SetChannelWorldShards(1)

	require.EqualValues(t, 1, SetRevenueShards(0))
	require.EqualValues(t, 16, SetRevenueShards(16))
	require.EqualValues(t, 16, RevenueShards())
	require.EqualValues(t, MaxShards, SetRevenueShards(1<<30))
	require.EqualValues(t, 1, SetChannelWorldShards(0))
	require.EqualValues(t, 4, SetChannelWorldShards(4))
	require.EqualValues(t, 4, ChannelWorldShards())

	SetRevenueShards(16)
	require.Regexp(t, `^revenue:accumulated:\d+$`, RevenueDestination(AccumulatedBase, "x"))
	require.Regexp(t, `^world:\d+$`, ChannelSink("x"))
}

func TestIsShardOfAndPattern(t *testing.T) {
	t.Parallel()

	require.Equal(t, "revenue:accumulated:", Pattern(AccumulatedBase))
	for addr, want := range map[string]bool{
		"revenue:accumulated":             true,
		"revenue:accumulated:0":           true,
		"revenue:accumulated:15":          true,
		"revenue:accumulated:":            false,
		"revenue:accumulated:x":           false,
		"revenue:accumulated:1:2":         false,
		"revenue:accumulated_old":         false,
		"revenue:channel_processing_cost": false,
		"revenue:fee_income":              false,
		"world":                           false,
	} {
		require.Equal(t, want, IsShardOf(AccumulatedBase, addr), addr)
	}
	require.True(t, IsShardOf(ProcessingCostBase, "revenue:channel_processing_cost:9"))
}

func TestSourceClause(t *testing.T) {
	t.Parallel()

	require.Equal(t, "@world", SourceClause("world"))
	require.Equal(t, "@world:3 allowing unbounded overdraft", SourceClause("world:3"))
}

func TestEquivalentAccount(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"revenue:accumulated", "revenue:accumulated", true},
		{"revenue:accumulated", "revenue:accumulated:3", true},
		{"revenue:accumulated:3", "revenue:accumulated:12", true},
		{"world", "world:5", true},
		{"world:1", "world:2", true},
		{"revenue:channel_processing_cost", "revenue:channel_processing_cost:0", true},
		// never across different accounts
		{"revenue:accumulated", "revenue:channel_processing_cost", false},
		{"revenue:accumulated:3", "revenue:channel_processing_cost:3", false},
		{"world", "revenue:accumulated", false},
		{"channel:abc", "channel:abd", false},
		{"channel:abc:1", "channel:abc:2", false},
		{"users:a:wallets:USD:available", "users:b:wallets:USD:available", false},
	} {
		require.Equal(t, tc.want, EquivalentAccount(tc.a, tc.b), "%s ~ %s", tc.a, tc.b)
		require.Equal(t, tc.want, EquivalentAccount(tc.b, tc.a), "%s ~ %s", tc.b, tc.a)
	}
}

func TestEquivalentAccountSystemControl(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"system:control:USD", "system:control:USD:3", true},
		{"system:control:USD:1", "system:control:USD:9", true},
		{"system:control:USD", "system:control:USD", true},
		{"system:control:USD", "system:control:NGN", false},
		{"system:control:USD:3", "system:control:NGN:3", false},
		{"system:control:USD", "system:control:USD:x", false},
		{"system:control:", "system:control:", true}, // equal strings
		{"system:control:USD", "users:u1:wallets:USD:available", false},
	} {
		if got := EquivalentAccount(tc.a, tc.b); got != tc.want {
			t.Errorf("EquivalentAccount(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestSystemControlAddress(t *testing.T) {
	t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", "")
	if got := SystemControlAddress("w1", "USD"); got != "system:control:USD" {
		t.Fatal(got)
	}
	t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", "16")
	a := SystemControlAddress("w1", "USD")
	if a != SystemControlAddress("w1", "USD") || !IsShardOf("system:control:USD", a) || a == "system:control:USD" {
		t.Fatalf("unstable or unsharded address %q", a)
	}
	for _, bad := range []string{"0", "-3", "abc"} {
		t.Setenv("CBA_SYSTEM_CONTROL_SHARDS", bad)
		if got := SystemControlAddress("w1", "USD"); got != "system:control:USD" {
			t.Fatalf("%q: %s", bad, got)
		}
	}
}
