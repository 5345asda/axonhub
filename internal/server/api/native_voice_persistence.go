package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	entrequest "github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/ent/requestexecution"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcontext"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/voice"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

const nativeVoicePersistenceTimeout = 10 * time.Second

var nativeVoiceSensitiveValue = regexp.MustCompile(`(?i)(bearer\s+|(?:api[_-]?key|access[_-]?key|token|secret|authorization)\s*[=:]\s*)([^\s,;&]+)`)
var nativeVoiceURL = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s"'<>]+`)

// nativeVoiceRequestRecorder captures relay metadata without doing I/O from
// callbacks. The handler persists it only after the relay has completed.
type nativeVoiceRequestRecorder struct {
	requestService *biz.RequestService
	protocol       objects.NativeVoiceProtocol
	model          string
	stream         bool
	requestHeaders http.Header
	clientIP       string
	requestBody    []byte
	startedAt      time.Time

	mu       sync.Mutex
	attempts map[int]nativeVoiceRelayAttempt
	results  map[int]voice.NativeRelayResult
	order    []int
}

type nativeVoiceRelayAttempt struct {
	id        int
	channelID int
	url       string
	headers   http.Header
}

func newNativeVoiceRequestRecorder(
	requestService *biz.RequestService,
	protocol objects.NativeVoiceProtocol,
	model string,
	stream bool,
	inbound *http.Request,
	requestBody []byte,
	startedAt time.Time,
) *nativeVoiceRequestRecorder {
	if requestService == nil || inbound == nil {
		return nil
	}

	return &nativeVoiceRequestRecorder{
		requestService: requestService,
		protocol:       protocol,
		model:          model,
		stream:         stream,
		requestHeaders: nativeVoiceMaskedHeaders(inbound.Header),
		clientIP:       inbound.RemoteAddr,
		requestBody:    append([]byte(nil), requestBody...),
		startedAt:      startedAt,
		attempts:       make(map[int]nativeVoiceRelayAttempt),
		results:        make(map[int]voice.NativeRelayResult),
	}
}

func (r *nativeVoiceRequestRecorder) OnNativeRelayAttempt(_ context.Context, attempt voice.NativeRelayAttempt) {
	if r == nil || attempt.ChannelID == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.attempts[attempt.ID]; exists {
		return
	}
	r.attempts[attempt.ID] = nativeVoiceRelayAttempt{
		id:        attempt.ID,
		channelID: attempt.ChannelID,
		url:       nativeVoicePersistedURL(attempt.URL),
		headers:   nativeVoiceMaskedHeaders(attempt.RequestHeaders),
	}
	r.order = append(r.order, attempt.ID)
}

func (r *nativeVoiceRequestRecorder) OnNativeRelayResult(_ context.Context, result voice.NativeRelayResult) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	result.ResponseHeaders = nativeVoiceMaskedHeaders(result.ResponseHeaders)
	r.results[result.ID] = result
}

func (r *nativeVoiceRequestRecorder) finish(ctx context.Context, relayErr error) {
	if r == nil || r.requestService == nil {
		return
	}

	attempts, results, finalID := r.snapshot()
	persistCtx, cancel := nativeVoicePersistenceContext(ctx)
	defer cancel()

	stream := r.stream
	request, err := r.requestService.CreateRequest(
		persistCtx,
		&llm.Request{Model: r.model, Stream: &stream},
		&httpclient.Request{
			Headers:  r.requestHeaders,
			Body:     r.requestBody,
			JSONBody: nativeVoiceJSONBody(r.requestBody),
			ClientIP: r.clientIP,
		},
		llm.APIFormat(r.protocol.APIFormat),
	)
	if err != nil {
		log.Warn(ctx, "failed to persist native voice request", log.Cause(err))
		return
	}

	for _, attempt := range attempts {
		result, hasResult := results[attempt.id]
		execution, createErr := r.requestService.CreateRequestExecution(
			persistCtx,
			&biz.Channel{Channel: &ent.Channel{ID: attempt.channelID}},
			r.model,
			request,
			httpclient.Request{URL: attempt.url, Headers: attempt.headers, Body: r.requestBody, JSONBody: nativeVoiceJSONBody(r.requestBody)},
			llm.APIFormat(r.protocol.APIFormat),
			false,
		)
		if createErr != nil {
			log.Warn(ctx, "failed to persist native voice execution", log.Cause(createErr), log.Int("channel_id", attempt.channelID))
			continue
		}
		if !hasResult || result.Err != nil || result.StatusCode >= http.StatusBadRequest {
			r.updateFailedExecution(ctx, persistCtx, execution.ID, result, hasResult)
			continue
		}
		if updateErr := r.requestService.UpdateRequestExecutionCompleted(persistCtx, execution.ID, "", newNativeVoiceResponseMetadata(result.StatusCode, result.ResponseHeaders, result.ResponseBytes), nativeVoiceLatencyMetrics(result.Duration)); updateErr != nil {
			log.Warn(ctx, "failed to persist native voice execution completion", log.Cause(updateErr), log.Int("execution_id", execution.ID))
		}
	}

	if finalID != 0 {
		for _, attempt := range attempts {
			if attempt.id != finalID {
				continue
			}
			if err := r.requestService.UpdateRequestChannelID(persistCtx, request.ID, attempt.channelID); err != nil {
				log.Warn(ctx, "failed to persist native voice request channel", log.Cause(err), log.Int("request_id", request.ID))
			}
		}
	}

	final, hasFinal := results[finalID]
	failed := relayErr != nil || (hasFinal && (final.Err != nil || final.StatusCode >= http.StatusBadRequest))
	if failed {
		if relayErr != nil {
			err = r.requestService.UpdateRequestStatusFromError(persistCtx, request.ID, relayErr)
		} else {
			err = r.requestService.UpdateRequestStatus(persistCtx, request.ID, entrequest.StatusFailed)
		}
		if err != nil {
			log.Warn(ctx, "failed to persist native voice request failure", log.Cause(err), log.Int("request_id", request.ID))
		}
		return
	}

	if !hasFinal {
		final.StatusCode = http.StatusOK
	}
	if err := r.requestService.UpdateRequestCompleted(persistCtx, request.ID, "", newNativeVoiceResponseMetadata(final.StatusCode, final.ResponseHeaders, final.ResponseBytes), nativeVoiceLatencyMetrics(time.Since(r.startedAt))); err != nil {
		log.Warn(ctx, "failed to persist native voice request completion", log.Cause(err), log.Int("request_id", request.ID))
	}
}

func (r *nativeVoiceRequestRecorder) snapshot() ([]nativeVoiceRelayAttempt, map[int]voice.NativeRelayResult, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	attempts := make([]nativeVoiceRelayAttempt, 0, len(r.order))
	for _, id := range r.order {
		attempt, ok := r.attempts[id]
		if !ok {
			continue
		}
		attempt.headers = nativeVoiceMaskedHeaders(attempt.headers)
		attempts = append(attempts, attempt)
	}
	results := make(map[int]voice.NativeRelayResult, len(r.results))
	for id, result := range r.results {
		result.ResponseHeaders = nativeVoiceMaskedHeaders(result.ResponseHeaders)
		results[id] = result
	}

	finalID := 0
	for _, id := range r.order {
		if result, ok := results[id]; ok && result.Committed {
			finalID = id
		}
	}
	if finalID == 0 && len(r.order) > 0 {
		finalID = r.order[len(r.order)-1]
	}
	return attempts, results, finalID
}

func (r *nativeVoiceRequestRecorder) updateFailedExecution(ctx, persistCtx context.Context, executionID int, result voice.NativeRelayResult, hasResult bool) {
	errorMessage := "native voice upstream attempt did not produce a result"
	statusCode := 0
	if hasResult {
		errorMessage = nativeVoicePersistedError(result.Err)
		statusCode = result.StatusCode
		if errorMessage == "" {
			errorMessage = fmt.Sprintf("native voice upstream returned status %d", statusCode)
		}
	}
	if err := r.requestService.UpdateRequestExecutionStatusWithMetrics(persistCtx, executionID, requestexecution.StatusFailed, errorMessage, nativeVoiceExecutionErrorInfo(statusCode), nativeVoiceLatencyMetrics(result.Duration)); err != nil {
		log.Warn(ctx, "failed to persist native voice execution failure", log.Cause(err), log.Int("execution_id", executionID))
	}
}

func nativeVoicePersistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	persistCtx, cancel := xcontext.DetachWithTimeout(ctx, nativeVoicePersistenceTimeout)
	return authz.WithSystemBypass(persistCtx, "persist-native-voice-request"), cancel
}

func nativeVoiceMaskedHeaders(headers http.Header) http.Header {
	masked := make(http.Header, len(headers))
	for key, values := range headers {
		canonical := http.CanonicalHeaderKey(key)
		if httpclient.IsSensitiveHeader(canonical) || nativeVoiceLegacyCredentialHeader(canonical) {
			masked[canonical] = []string{"******"}
			continue
		}
		masked[canonical] = append([]string(nil), values...)
	}
	return masked
}

func nativeVoiceLegacyCredentialHeader(key string) bool {
	switch key {
	case "X-Api-Access-Key", "X-Api-App-Key", "X-Api-Resource-Id", "X-Api-Connect-Id":
		return true
	default:
		return false
	}
}

func nativeVoiceJSONBody(body []byte) []byte {
	if json.Valid(body) {
		return body
	}
	return nil
}

func nativeVoicePersistedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	return parsed.String()
}

func nativeVoicePersistedError(err error) string {
	if err == nil {
		return ""
	}

	message := nativeVoiceURL.ReplaceAllStringFunc(err.Error(), nativeVoicePersistedURL)
	message = nativeVoiceSensitiveValue.ReplaceAllString(message, "$1[REDACTED]")
	if len(message) > 1024 {
		return message[:1024] + "..."
	}
	return message
}

func nativeVoiceExecutionErrorInfo(statusCode int) *biz.ExecutionErrorInfo {
	if statusCode <= 0 {
		return nil
	}
	return &biz.ExecutionErrorInfo{StatusCode: &statusCode}
}

func nativeVoiceLatencyMetrics(duration time.Duration) *biz.LatencyMetrics {
	latencyMs := biz.ClampLatency(duration.Milliseconds())
	return &biz.LatencyMetrics{LatencyMs: &latencyMs}
}

type nativeVoiceResponseMeta struct {
	Object      string `json:"object"`
	StatusCode  int    `json:"status_code"`
	ContentType string `json:"content_type,omitempty"`
	Bytes       int64  `json:"bytes"`
}

func newNativeVoiceResponseMetadata(statusCode int, headers http.Header, bytes int64) nativeVoiceResponseMeta {
	contentType := ""
	if headers != nil {
		contentType = headers.Get("Content-Type")
	}
	return nativeVoiceResponseMeta{
		Object:      "native.voice.response",
		StatusCode:  statusCode,
		ContentType: contentType,
		Bytes:       bytes,
	}
}
