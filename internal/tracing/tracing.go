package tracing

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent/apikey"
)

const (
	// APIKeyModelTracePrefix identifies trace IDs derived from an API key and model.
	APIKeyModelTracePrefix = "akm-v1-"
	apiKeyModelTraceDomain = "axonhub:api-key-model-sticky:v1"
	apiKeyModelNoAuthValue = "AXONHUB_API_KEY_NO_AUTH"
)

type Config struct {
	// ThreadHeader is the header name for thread ID.
	// Default to "AH-Thread-Id".
	ThreadHeader string `conf:"thread_header" yaml:"thread_header" json:"thread_header"`

	// TraceHeader is the header name for trace ID.
	// Default to "AH-Trace-Id".
	TraceHeader string `conf:"trace_header" yaml:"trace_header" json:"trace_header"`

	// RequestHeader is the header name for request ID.
	// Default to "AH-Request-Id".
	RequestHeader string `conf:"request_header" yaml:"request_header" json:"request_header"`

	// ExtraTraceHeaders is the extra header names for trace ID.
	// It will use if primary trace header is not found in request headers.
	// e.g. set it to []string{"Sentry-Trace"} to trace claude-code or any other product using sentry.
	// Default to nil.
	ExtraTraceHeaders []string `conf:"extra_trace_headers" yaml:"extra_trace_headers" json:"extra_trace_headers"`

	// ResponseTraceHeaders is the response header names to write the resolved trace ID to.
	// An empty list disables trace ID response headers.
	// Default to nil.
	ResponseTraceHeaders []string `conf:"response_trace_headers" yaml:"response_trace_headers" json:"response_trace_headers"`

	// ExtraTraceBodyFields is the extra body fields names for trace ID.
	// It will use if primary trace header is not found in request body.
	// Default to nil.
	ExtraTraceBodyFields []string `conf:"extra_trace_body_fields" yaml:"extra_trace_body_fields" json:"extra_trace_body_fields"`

	// APIKeyModelSticky derives a stable trace ID from the authenticated API key
	// and request model before evaluating client-provided trace identifiers.
	// Default to false.
	APIKeyModelSticky bool `conf:"api_key_model_sticky" yaml:"api_key_model_sticky" json:"api_key_model_sticky"`

	// APIKeyModelStickySecret is the server-only HMAC secret used by
	// APIKeyModelSticky. It is omitted from JSON and YAML config previews.
	APIKeyModelStickySecret string `conf:"api_key_model_sticky_secret" yaml:"-" json:"-"`

	// ClaudeCodeTraceEnabled enables extracting trace IDs from Claude Code request metadata.
	// Default to true.
	ClaudeCodeTraceEnabled bool `conf:"claude_code_trace_enabled" yaml:"claude_code_trace_enabled" json:"claude_code_trace_enabled"`

	// CodexTraceEnabled enables extracting trace IDs from Codex request headers.
	// Default to true.
	CodexTraceEnabled bool `conf:"codex_trace_enabled" yaml:"codex_trace_enabled" json:"codex_trace_enabled"`

	// OpenCodeTraceEnabled enables extracting trace IDs from OpenCode request headers.
	// Default to true.
	OpenCodeTraceEnabled bool `conf:"opencode_trace_enabled" yaml:"opencode_trace_enabled" json:"opencode_trace_enabled"`
}

// GenerateTraceID generate trace id, format as at-{{uuid}}.
func GenerateTraceID() string {
	id := uuid.New()
	return fmt.Sprintf("at-%s", id.String())
}

// GenerateRequestID generate request id, format as ar-{{uuid}}.
func GenerateRequestID() string {
	id := uuid.New()
	return fmt.Sprintf("ar-%s", id.String())
}

// WithTraceID store trace id to context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return contexts.WithTraceID(ctx, traceID)
}

// GetTraceID get trace id from context.
func GetTraceID(ctx context.Context) (string, bool) {
	return contexts.GetTraceID(ctx)
}

// WithRequestID store request id to context.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return contexts.WithRequestID(ctx, requestID)
}

// GetRequestID get request id from context.
func GetRequestID(ctx context.Context) (string, bool) {
	return contexts.GetRequestID(ctx)
}

// WithOperationName store operation name to context.
func WithOperationName(ctx context.Context, name string) context.Context {
	return contexts.WithOperationName(ctx, name)
}

// GetOperationName get operation name from context.
func GetOperationName(ctx context.Context) (string, bool) {
	return contexts.GetOperationName(ctx)
}

// APIKeyModelTraceID derives a stable, non-reversible trace ID from the
// authenticated API key and request model. No-auth requests intentionally do
// not participate in this affinity scheme.
func APIKeyModelTraceID(ctx context.Context, model, secret string) (string, error) {
	apiKey, ok := contexts.GetAPIKey(ctx)
	if !ok || apiKey == nil || apiKey.Key == "" || apiKey.Key == apiKeyModelNoAuthValue || apiKey.Type == apikey.TypeNoauth {
		return "", nil
	}

	model = strings.TrimSpace(model)
	if model == "" {
		return "", nil
	}

	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", fmt.Errorf("server.trace.api_key_model_sticky_secret must be configured when API key and model sticky tracing is enabled")
	}

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = io.WriteString(mac, apiKeyModelTraceDomain)
	writeLengthPrefixed(mac, apiKey.Key)
	writeLengthPrefixed(mac, model)

	return APIKeyModelTracePrefix + hex.EncodeToString(mac.Sum(nil)), nil
}

func writeLengthPrefixed(w io.Writer, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = io.WriteString(w, value)
}
