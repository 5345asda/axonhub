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

// nativeVoiceRequestRecorder persists native relay metadata without inspecting
// or retaining opaque response frames. It is request-scoped and attached to
// the relay context, so fallback attempts remain independently inspectable.
type nativeVoiceRequestRecorder struct {
	requestService *biz.RequestService
	request        *ent.Request
	protocol       objects.NativeVoiceProtocol
	model          string
	stream         bool
	requestBody    []byte
	startedAt      time.Time

	mu         sync.Mutex
	executions map[int]int
	failed     bool
	final      nativeVoiceRelayOutcome
}

type nativeVoiceRelayOutcome struct {
	statusCode int
	headers    http.Header
	bytes      int64
	set        bool
}

func newNativeVoiceRequestRecorder(
	ctx context.Context,
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

	recorder := &nativeVoiceRequestRecorder{
		requestService: requestService,
		protocol:       protocol,
		model:          model,
		stream:         stream,
		requestBody:    append([]byte(nil), requestBody...),
		startedAt:      startedAt,
		executions:     make(map[int]int),
	}

	persistCtx, cancel := nativeVoicePersistenceContext(ctx)
	defer cancel()
	request, err := requestService.CreateRequest(
		persistCtx,
		&llm.Request{Model: model, Stream: &stream},
		&httpclient.Request{
			Method:   inbound.Method,
			URL:      inbound.URL.String(),
			Path:     inbound.URL.Path,
			Query:    inbound.URL.Query(),
			Headers:  nativeVoiceMaskedHeaders(inbound.Header),
			Body:     requestBody,
			JSONBody: nativeVoiceJSONBody(requestBody),
			ClientIP: inbound.RemoteAddr,
		},
		llm.APIFormat(protocol.APIFormat),
	)
	if err != nil {
		log.Warn(ctx, "failed to persist native voice request", log.Cause(err))
		return nil
	}
	recorder.request = request

	return recorder
}

func (r *nativeVoiceRequestRecorder) OnNativeRelayAttempt(ctx context.Context, attempt voice.NativeRelayAttempt) {
	if r == nil || r.request == nil || attempt.Target.Channel == nil {
		return
	}

	persistCtx, cancel := nativeVoicePersistenceContext(ctx)
	defer cancel()

	requestHeaders := nativeVoiceMaskedHeaders(attempt.RequestHeaders)
	execution, err := r.requestService.CreateRequestExecution(
		persistCtx,
		attempt.Target.Channel,
		r.model,
		r.request,
		httpclient.Request{
			URL:      nativeVoicePersistedURL(attempt.URL),
			Headers:  requestHeaders,
			JSONBody: nativeVoiceJSONBody(r.requestBody),
			Body:     r.requestBody,
		},
		llm.APIFormat(r.protocol.APIFormat),
		false,
	)
	if err != nil {
		log.Warn(ctx, "failed to persist native voice execution", log.Cause(err), log.Int("channel_id", attempt.Target.Channel.ID))
		return
	}

	if r.request.ChannelID == 0 {
		if err := r.requestService.UpdateRequestChannelID(persistCtx, r.request.ID, attempt.Target.Channel.ID); err != nil {
			log.Warn(ctx, "failed to persist native voice request channel", log.Cause(err), log.Int("request_id", r.request.ID))
		} else {
			r.request.ChannelID = attempt.Target.Channel.ID
		}
	}

	r.mu.Lock()
	r.executions[attempt.ID] = execution.ID
	r.mu.Unlock()
}

func (r *nativeVoiceRequestRecorder) OnNativeRelayResult(ctx context.Context, result voice.NativeRelayResult) {
	if r == nil {
		return
	}

	r.mu.Lock()
	executionID := r.executions[result.ID]
	r.mu.Unlock()
	if executionID == 0 {
		return
	}

	persistCtx, cancel := nativeVoicePersistenceContext(ctx)
	defer cancel()
	metrics := nativeVoiceLatencyMetrics(result.Duration)
	failed := result.Err != nil || result.StatusCode >= http.StatusBadRequest
	if !result.Retry {
		r.mu.Lock()
		r.final = nativeVoiceRelayOutcome{
			statusCode: result.StatusCode,
			headers:    result.ResponseHeaders.Clone(),
			bytes:      result.ResponseBytes,
			set:        true,
		}
		r.mu.Unlock()
	}
	if failed {
		if !result.Retry {
			r.mu.Lock()
			r.failed = true
			r.mu.Unlock()
		}
		errorMessage := nativeVoicePersistedError(result.Err)
		if errorMessage == "" {
			errorMessage = fmt.Sprintf("native voice upstream returned status %d", result.StatusCode)
		}
		if err := r.requestService.UpdateRequestExecutionStatusWithMetrics(
			persistCtx,
			executionID,
			requestexecution.StatusFailed,
			errorMessage,
			nativeVoiceExecutionErrorInfo(result.StatusCode),
			metrics,
		); err != nil {
			log.Warn(ctx, "failed to persist native voice execution failure", log.Cause(err), log.Int("execution_id", executionID))
		}
		return
	}

	if err := r.requestService.UpdateRequestExecutionCompleted(
		persistCtx,
		executionID,
		"",
		newNativeVoiceResponseMetadata(result.StatusCode, result.ResponseHeaders, result.ResponseBytes),
		metrics,
	); err != nil {
		log.Warn(ctx, "failed to persist native voice execution completion", log.Cause(err), log.Int("execution_id", executionID))
	}
}

func (r *nativeVoiceRequestRecorder) finish(ctx context.Context, relayErr error) {
	if r == nil || r.request == nil {
		return
	}

	persistCtx, cancel := nativeVoicePersistenceContext(ctx)
	defer cancel()
	metrics := nativeVoiceLatencyMetrics(time.Since(r.startedAt))
	r.mu.Lock()
	failed := r.failed
	final := r.final
	r.mu.Unlock()
	if relayErr != nil || failed {
		var err error
		if relayErr != nil {
			err = r.requestService.UpdateRequestStatusFromError(persistCtx, r.request.ID, relayErr)
		} else {
			err = r.requestService.UpdateRequestStatus(persistCtx, r.request.ID, entrequest.StatusFailed)
		}
		if err != nil {
			log.Warn(ctx, "failed to persist native voice request failure", log.Cause(err), log.Int("request_id", r.request.ID))
		}
		return
	}

	if !final.set {
		final.statusCode = http.StatusOK
	}
	if err := r.requestService.UpdateRequestCompleted(
		persistCtx,
		r.request.ID,
		"",
		newNativeVoiceResponseMetadata(final.statusCode, final.headers, final.bytes),
		metrics,
	); err != nil {
		log.Warn(ctx, "failed to persist native voice request completion", log.Cause(err), log.Int("request_id", r.request.ID))
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
