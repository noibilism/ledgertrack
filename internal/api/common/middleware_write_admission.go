package common

import (
	"errors"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

// WriteAdmission bounds the number of write requests (anything but GET, HEAD and OPTIONS) being
// processed at the same time, see ledgercontroller.WriteLimiter.
//
// It must sit at the very top of the stack: waiting here costs a goroutine, while any later step
// (ledger resolution, schema checks, opening the transaction) takes a pooled connection, which is
// exactly the resource the limiter protects. When the limiter is saturated the request is answered
// with 503 and a Retry-After header before anything has been read or written, so it is always safe
// for the client to retry it, including multi-ledger operations such as channel debits.
//
// The admitted context is flagged so that the ledger controllers below do not ask for a second slot.
// A nil limiter returns a pass-through middleware.
func WriteAdmission(limiter *ledgercontroller.WriteLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if limiter == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet, http.MethodHead, http.MethodOptions:
				next.ServeHTTP(w, r)
				return
			}

			ctx, release, err := limiter.Admit(r.Context())
			if err != nil {
				if errors.Is(err, ledgercontroller.ErrWriteOverloaded{}) {
					writeOverloaded(w, err)
					return
				}
				// the client went away while waiting
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			defer release()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
