// Package loghash implements the asynchronous log hash chain used when a ledger is configured
// with HASH_LOGS=ASYNC.
//
// In SYNC mode the hash of a log is computed by a BEFORE INSERT trigger, which needs the previous
// log hash and therefore a ledger wide advisory lock held until commit. In ASYNC mode there is no
// trigger and no lock: logs are committed with a NULL hash and the Hasher defined in this package
// chains them shortly after, using exactly the same hash function (the compute_hash SQL function),
// so a chain produced by the Hasher is byte for byte the chain a SYNC ledger would have produced.
//
// See docs/design/async-log-hashing.md for the audit semantics.
package loghash

import (
	"fmt"
	"regexp"
)

var bucketFormat = regexp.MustCompile("^[0-9a-zA-Z_-]{1,63}$")

// quoteBucket returns the quoted bucket (schema) name. Bucket names are validated when a ledger is
// created, this is a defense in depth as the name ends up in SQL text.
func quoteBucket(bucket string) (string, error) {
	if !bucketFormat.MatchString(bucket) {
		return "", fmt.Errorf("invalid bucket name %q", bucket)
	}
	return `"` + bucket + `"`, nil
}

// chainablePrefix returns how many of the given ids (sorted ascending, all greater than head) can
// be appended to the chain right now.
//
// Log ids are allocated from a sequence at insert time but become visible at commit time, so two
// concurrent transactions can make id N+1 visible before id N. An id that is not visible is one of:
//   - a rolled back transaction (permanent hole, this happens with every deadlock retry)
//   - a transaction that is still in flight (will become visible later)
//
// A hole cannot be told apart from an in flight id by looking at the log table only. So the chain
// only advances over a hole when it is known to be final, which is what resolvedUpTo means: every
// id <= resolvedUpTo that is not visible is final (see Hasher for how it is established).
// Contiguous ids never need that knowledge.
func chainablePrefix(head uint64, ids []uint64, resolvedUpTo uint64) int {
	expected := head + 1
	for i, id := range ids {
		if id != expected && id > resolvedUpTo {
			return i
		}
		expected = id + 1
	}
	return len(ids)
}
