package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/tracing"
	"github.com/looplj/axonhub/llm/httpclient"
)

var errAPIKeyModelStickyRequestBody = fmt.Errorf("failed to inspect request body for API key and model sticky tracing")

// tryGetTraceIDFromAPIKeyAndModel derives a stable, non-reversible trace ID
// from the authenticated API key and the request model. The request body is
// restored so downstream middleware and handlers can read it normally.
func tryGetTraceIDFromAPIKeyAndModel(c *gin.Context, secret string) (string, error) {
	apiKey, ok := contexts.GetAPIKey(c.Request.Context())
	if !ok || apiKey == nil || apiKey.Key == "" || apiKey.Key == biz.NoAuthAPIKeyValue || apiKey.Type == apikey.TypeNoauth {
		return "", nil
	}

	// No-auth requests must not pay the cost of reading or decompressing a body.
	// The HMAC helper also checks this invariant for callers that already have a
	// model, but keeping the early return here avoids touching untrusted input.
	model, err := requestModel(c)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errAPIKeyModelStickyRequestBody, err)
	}

	return tracing.APIKeyModelTraceID(c.Request.Context(), model, secret)
}

func requestModel(c *gin.Context) (string, error) {
	if c.Request == nil {
		return "", nil
	}

	if c.Request.Body != nil && c.Request.Method != "GET" && c.Request.Method != "HEAD" {
		mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(c.GetHeader("Content-Type"), ";", 2)[0]))
		if mediaType == "" || mediaType == "application/json" || strings.HasSuffix(mediaType, "+json") {
			genericRequest, err := httpclient.ReadHTTPRequest(c.Request)
			if err != nil {
				return "", err
			}

			// ReadHTTPRequest normalizes supported encodings in place. Reinstall the
			// normalized body and metadata before the next middleware/handler runs.
			c.Request.Body = io.NopCloser(bytes.NewReader(genericRequest.Body))
			c.Request.ContentLength = int64(len(genericRequest.Body))
			c.Request.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(genericRequest.Body)), nil
			}

			if model := modelFromJSON(genericRequest.Body); model != "" {
				return model, nil
			}
		}
	}

	return modelFromPath(c.Request.URL.Path), nil
}

func modelFromJSON(body []byte) string {
	var payload struct {
		Model string `json:"model"`
	}
	if len(body) == 0 || json.Unmarshal(body, &payload) != nil {
		return ""
	}

	return strings.TrimSpace(payload.Model)
}

func modelFromPath(path string) string {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	for index, segment := range segments {
		if segment != "models" || index+1 >= len(segments) {
			continue
		}

		modelSegment := segments[index+1]
		var model string
		switch {
		case strings.HasSuffix(modelSegment, ":generateContent"):
			model = strings.TrimSuffix(modelSegment, ":generateContent")
		case strings.HasSuffix(modelSegment, ":streamGenerateContent"):
			model = strings.TrimSuffix(modelSegment, ":streamGenerateContent")
		default:
			continue
		}

		if decoded, err := url.PathUnescape(model); err == nil {
			model = decoded
		}
		return strings.TrimSpace(model)
	}

	return ""
}
