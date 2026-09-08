package voice

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/looplj/axonhub/llm/httpclient"
)

var nativeRelayObserverURLPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s"'<>]+`)

// NativeRelayObserver receives lifecycle events for native relay attempts.
// Relay implementations call observers synchronously, so callbacks must only
// capture bounded in-memory state and must not perform I/O.
type NativeRelayObserver interface {
	OnNativeRelayAttempt(context.Context, NativeRelayAttempt)
	OnNativeRelayResult(context.Context, NativeRelayResult)
}

// NativeRelayAttempt identifies one provider connection attempt. URL is the
// normalized upstream URL without userinfo or query parameters. ChannelID is
// sufficient for telemetry and avoids exposing the in-memory channel entity.
type NativeRelayAttempt struct {
	ID             int
	ChannelID      int
	URL            string
	RequestHeaders http.Header
	StartedAt      time.Time
}

// NativeRelayResult reports the outcome of one attempt. ResponseHeaders are a
// copy of provider response metadata with credential-bearing fields masked.
type NativeRelayResult struct {
	ID              int
	StatusCode      int
	ResponseHeaders http.Header
	ResponseBytes   int64
	Err             error
	Committed       bool
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
	attempt.URL = nativeRelayObserverURL(attempt.URL)
	attempt.RequestHeaders = nativeRelayObserverHeaders(attempt.RequestHeaders)
	defer func() { _ = recover() }()
	observer.OnNativeRelayAttempt(ctx, attempt)
}

func notifyNativeRelayResult(ctx context.Context, result NativeRelayResult) {
	observer := nativeRelayObserverFromContext(ctx)
	if observer == nil {
		return
	}
	result.ResponseHeaders = nativeRelayObserverHeaders(result.ResponseHeaders)
	result.Err = nativeRelayObserverError(result.Err)
	defer func() { _ = recover() }()
	observer.OnNativeRelayResult(ctx, result)
}

func nativeRelayObserverURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	return parsed.String()
}

func nativeRelayObserverHeaders(src http.Header) http.Header {
	if src == nil {
		return nil
	}

	dst := make(http.Header, len(src))
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if httpclient.IsSensitiveHeader(canonical) || isNativeHTTPRelayCredentialHeader(canonical) {
			dst[canonical] = []string{"******"}
			continue
		}
		dst[canonical] = append([]string(nil), values...)
	}
	return dst
}

func nativeRelayObserverError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(nativeRelayObserverURLPattern.ReplaceAllStringFunc(err.Error(), nativeRelayObserverURL))
}
