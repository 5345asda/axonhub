package voice

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm/httpclient"
)

// NativeRelayAdmission applies the channel's existing concurrency and local
// RPM limits to opaque native HTTP and WebSocket attempts. It deliberately
// does not inspect provider payloads, so native requests do not participate
// in TPM accounting.
type NativeRelayAdmission struct {
	limiterManager *orchestrator.ChannelLimiterManager
	requestTracker *orchestrator.ChannelRequestTracker
}

// NativeRelayAdmissionError identifies a local channel admission rejection so
// the API layer can preserve the existing 429 response contract.
type NativeRelayAdmissionError struct {
	StatusCode int
	Cause      error
}

var errNativeRelayUpstream = errors.New("native voice upstream request failed")

func (e *NativeRelayAdmissionError) Error() string {
	if e == nil || e.Cause == nil {
		return "native voice channel admission rejected"
	}
	return e.Cause.Error()
}

func (e *NativeRelayAdmissionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// nativeRelayPublicError preserves local admission failures. Upstream errors
// can embed provider URLs or response details, so they must not cross the
// AxonHub2 boundary.
func nativeRelayPublicError(err error) error {
	if _, ok := errors.AsType[*NativeRelayAdmissionError](err); ok {
		return err
	}
	return errNativeRelayUpstream
}

// NewNativeRelayAdmission wires native relays to the shared channel limiter
// manager and a tracker owned by the native handler set.
func NewNativeRelayAdmission(
	limiterManager *orchestrator.ChannelLimiterManager,
	requestTracker *orchestrator.ChannelRequestTracker,
) *NativeRelayAdmission {
	return &NativeRelayAdmission{
		limiterManager: limiterManager,
		requestTracker: requestTracker,
	}
}

type nativeRelayAdmissionSlot struct {
	limiter *orchestrator.ChannelLimiter
	once    sync.Once
}

func (s *nativeRelayAdmissionSlot) release() {
	if s == nil {
		return
	}

	s.once.Do(func() {
		if s.limiter != nil {
			s.limiter.Release()
		}
	})
}

func (a *NativeRelayAdmission) acquire(ctx context.Context, ch *biz.Channel) (*nativeRelayAdmissionSlot, error) {
	if ch == nil || ch.Channel == nil {
		return nil, errors.New("native voice relay target is missing channel")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if a != nil && a.requestTracker != nil {
		if until, cooling := a.requestTracker.GetCooldownUntil(ch.ID); cooling {
			return nil, &NativeRelayAdmissionError{
				StatusCode: http.StatusTooManyRequests,
				Cause:      fmt.Errorf("native voice channel %d is in cooldown until %s", ch.ID, until.Format(time.RFC3339)),
			}
		}
	}

	var limiter *orchestrator.ChannelLimiter
	if a != nil && a.limiterManager != nil {
		limiter = a.limiterManager.GetOrCreate(ch)
		if limiter != nil {
			if err := limiter.Acquire(ctx); err != nil {
				if errors.Is(err, orchestrator.ErrChannelQueueFull) || errors.Is(err, orchestrator.ErrChannelQueueTimeout) {
					return nil, &NativeRelayAdmissionError{StatusCode: http.StatusTooManyRequests, Cause: err}
				}
				return nil, err
			}
		}
	}

	if a != nil && a.requestTracker != nil {
		if limit := nativeVoiceRPM(ch); limit > 0 && !a.requestTracker.TryAcquireRequest(ch.ID, limit) {
			if limiter != nil {
				limiter.Release()
			}
			return nil, &NativeRelayAdmissionError{
				StatusCode: http.StatusTooManyRequests,
				Cause:      fmt.Errorf("native voice channel %d local rpm limit %d reached", ch.ID, limit),
			}
		}
	}

	return &nativeRelayAdmissionSlot{limiter: limiter}, nil
}

func nativeVoiceRPM(ch *biz.Channel) int64 {
	if ch == nil || ch.Settings == nil || ch.Settings.RateLimit == nil || ch.Settings.RateLimit.RPM == nil {
		return 0
	}
	return *ch.Settings.RateLimit.RPM
}

// observeHTTPResponse shares provider Retry-After cooldowns with the native
// load-balancer scorer. No cooldown is inferred when the provider omits it.
func (a *NativeRelayAdmission) observeHTTPResponse(ch *biz.Channel, resp *http.Response) {
	if a == nil || a.requestTracker == nil || ch == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		return
	}

	err := &httpclient.Error{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
	}
	if !httpclient.HasRetryAfterHeader(err) {
		return
	}

	cooldown, ok := httpclient.ParseRetryAfter(err)
	if ok {
		a.requestTracker.SetCooldown(ch.ID, time.Now().Add(cooldown))
	}
}
