package main

import (
	"context"
	"net/http"
)

type ctxKey struct{}

func withJob(r *http.Request, j *job) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), ctxKey{}, j))
}
