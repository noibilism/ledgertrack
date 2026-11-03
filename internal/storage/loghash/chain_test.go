package loghash

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainablePrefix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		head     uint64
		ids      []uint64
		resolved uint64
		want     int
	}{
		{"empty", 0, nil, 0, 0},
		{"contiguous from the start", 0, []uint64{1, 2, 3}, 0, 3},
		{"contiguous after head", 10, []uint64{11, 12}, 0, 2},
		{"hole right after head, unresolved", 10, []uint64{12, 13}, 0, 0},
		{"hole in the middle, unresolved", 0, []uint64{1, 2, 4, 5}, 0, 2},
		{"hole in the middle, resolved", 0, []uint64{1, 2, 4, 5}, 5, 4},
		{"hole resolved only up to a point", 0, []uint64{1, 3, 4, 6, 7}, 4, 3},
		{"resolved beyond everything", 0, []uint64{3, 9}, 100, 2},
		{"first id is a hole, resolved", 5, []uint64{9, 10}, 9, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, chainablePrefix(tc.head, tc.ids, tc.resolved))
		})
	}
}
