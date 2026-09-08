package voice

import (
	"context"
	"net/http"
	"time"
)

// NativeRelayObserver receives lifecycle events for native relay attempts.
// Implementations must treat the callbacks as best-effort telemetry: a
// callback failure must never change the relay result or block the provider
// connection.
type NativeRelayObserver interface {
	OnNativeRelayAttempt(context.Context, NativeRelayAttempt)
	OnNativeRelayResult(context.Context, NativeRelayResult)
}

// NativeRelayAttempt identifies one provider connection attempt. URL is the
// already-normalized upstream URL; credentials are never included in it.
type NativeRelayAttempt struct {
	ID             int
	Target         NativeRelayTarget
	URL            string
	RequestHeaders http.Header
	StartedAt      time.Time
}

// NativeRelayResult reports the outcome of one attempt. ResponseHeaders are a
// copy of provider response metadata and Error is the internal failure cause;
// observers must redact before persisting either value.
type NativeRelayResult struct {
	ID              int
	StatusCode      int
	ResponseHeaders http.Header
	ResponseBytes   int64
	Err             error
	Committed       bool
	Retry           bool
	Duration        time.Duration
}

type nativeRelayObserverContextKey struct{}

// WithNativeRelayObserver attaches request-scoped relay telemetry. The
// observer is intentionally carried in context so shared relay instances do
// not hold mutable per-request state.
func WithNativeRelayObserver(ctx context.Context, observer NativeRelayObserver) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, nativeRelayObserverContextKey{}, observer)
}

func nativeRelayObserverFromContext(ctx context.Context) NativeRelayObserver {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(nativeRelayObserverContextKey{}).(NativeRelayObserver)
	return observer
}

func notifyNativeRelayAttempt(ctx context.Context, attempt NativeRelayAttempt) {
	observer := nativeRelayObserverFromContext(ctx)
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer.OnNativeRelayAttempt(ctx, attempt)
}

func notifyNativeRelayResult(ctx context.Context, result NativeRelayResult) {
	observer := nativeRelayObserverFromContext(ctx)
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer.OnNativeRelayResult(ctx, result)
}
