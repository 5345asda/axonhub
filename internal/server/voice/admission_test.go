package voice

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
)

func TestNativeRelayAdmissionReleasesConcurrencyOnCancellation(t *testing.T) {
	maxConcurrent := int64(1)
	queueSize := int64(1)
	channel := &biz.Channel{Channel: &ent.Channel{
		ID: 1,
		Settings: &objects.ChannelSettings{RateLimit: &objects.ChannelRateLimit{
			MaxConcurrent: &maxConcurrent,
			QueueSize:     &queueSize,
		}},
	}}
	manager := orchestrator.NewChannelLimiterManager()
	admission := NewNativeRelayAdmission(manager, orchestrator.NewChannelRequestTracker())

	first, err := admission.acquire(context.Background(), channel)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, acquireErr := admission.acquire(ctx, channel)
		result <- acquireErr
	}()

	require.Eventually(t, func() bool {
		_, waiting, ok := manager.Stats(channel.ID)
		return ok && waiting == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	first.release()
	inFlight, waiting, ok := manager.Stats(channel.ID)
	require.True(t, ok)
	assert.Zero(t, inFlight)
	assert.Zero(t, waiting)
}

func TestNativeRelayAdmissionRPMRejectionReleasesLimiter(t *testing.T) {
	maxConcurrent := int64(1)
	rpm := int64(1)
	channel := &biz.Channel{Channel: &ent.Channel{
		ID: 2,
		Settings: &objects.ChannelSettings{RateLimit: &objects.ChannelRateLimit{
			MaxConcurrent: &maxConcurrent,
			RPM:           &rpm,
		}},
	}}
	manager := orchestrator.NewChannelLimiterManager()
	tracker := orchestrator.NewChannelRequestTracker()
	admission := NewNativeRelayAdmission(manager, tracker)

	slot, err := admission.acquire(context.Background(), channel)
	require.NoError(t, err)
	slot.release()

	_, err = admission.acquire(context.Background(), channel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "local rpm limit")
	var admissionErr *NativeRelayAdmissionError
	require.ErrorAs(t, err, &admissionErr)
	assert.Equal(t, http.StatusTooManyRequests, admissionErr.StatusCode)

	inFlight, waiting, ok := manager.Stats(channel.ID)
	require.True(t, ok)
	assert.Zero(t, inFlight)
	assert.Zero(t, waiting)
	assert.Equal(t, int64(1), tracker.GetRequestCount(channel.ID))
}

func TestNativeRelayAdmissionObservesRetryAfter(t *testing.T) {
	channel := &biz.Channel{Channel: &ent.Channel{ID: 3}}
	tracker := orchestrator.NewChannelRequestTracker()
	admission := NewNativeRelayAdmission(nil, tracker)

	admission.observeHTTPResponse(channel, &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"30"}},
	})

	assert.True(t, tracker.IsCoolingDown(channel.ID))
}

func TestNativeRelayAdmissionDoesNotInventCooldown(t *testing.T) {
	channel := &biz.Channel{Channel: &ent.Channel{ID: 4}}
	tracker := orchestrator.NewChannelRequestTracker()
	admission := NewNativeRelayAdmission(nil, tracker)

	admission.observeHTTPResponse(channel, &http.Response{StatusCode: http.StatusTooManyRequests})

	assert.False(t, tracker.IsCoolingDown(channel.ID))
}

func TestNativeRelayAdmissionRejectsCoolingDownChannel(t *testing.T) {
	maxConcurrent := int64(1)
	rpm := int64(10)
	channel := &biz.Channel{Channel: &ent.Channel{
		ID: 5,
		Settings: &objects.ChannelSettings{RateLimit: &objects.ChannelRateLimit{
			MaxConcurrent: &maxConcurrent,
			RPM:           &rpm,
		}},
	}}
	tracker := orchestrator.NewChannelRequestTracker()
	tracker.SetCooldown(channel.ID, time.Now().Add(time.Minute))
	manager := orchestrator.NewChannelLimiterManager()
	admission := NewNativeRelayAdmission(manager, tracker)

	slot, err := admission.acquire(context.Background(), channel)

	require.Error(t, err)
	assert.Nil(t, slot)
	var admissionErr *NativeRelayAdmissionError
	require.ErrorAs(t, err, &admissionErr)
	assert.Equal(t, http.StatusTooManyRequests, admissionErr.StatusCode)
	assert.Contains(t, err.Error(), "cooldown")
	assert.Equal(t, int64(0), tracker.GetRequestCount(channel.ID))
	_, _, ok := manager.Stats(channel.ID)
	assert.False(t, ok, "cooldown rejection must happen before limiter allocation")
}
