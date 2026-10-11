package middleware

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/contexts"
)

const apiKeyModelTracePrefix = "akm-v1-"

// tryGetTraceIDFromAPIKeyAndModel derives a stable, non-reversible trace ID
// from the authenticated API key and the request model. The request body is
// restored so downstream middleware and handlers can read it normally.
func tryGetTraceIDFromAPIKeyAndModel(c *gin.Context, secret string) (string, error) {
	model, err := requestModel(c)
	if err != nil {
		return "", err
	}

	apiKey, ok := contexts.GetAPIKey(c.Request.Context())
	if !ok || apiKey == nil || apiKey.Key == "" || model == "" {
		return "", nil
	}

	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", fmt.Errorf("server.trace.api_key_model_sticky_secret must be configured when API key and model sticky tracing is enabled")
	}

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("axonhub:api-key-model-sticky:v1"))
	writeLengthPrefixed(mac, apiKey.Key)
	writeLengthPrefixed(mac, model)

	return apiKeyModelTracePrefix + hex.EncodeToString(mac.Sum(nil)), nil
}

func writeLengthPrefixed(w io.Writer, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

func requestModel(c *gin.Context) (string, error) {
	if c.Request == nil {
		return "", nil
	}

	if c.Request.Body != nil && c.Request.Method != "GET" && c.Request.Method != "HEAD" {
		mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(c.GetHeader("Content-Type"), ";", 2)[0]))
		if mediaType == "" || mediaType == "application/json" || strings.HasSuffix(mediaType, "+json") {
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				return "", fmt.Errorf("failed to read request body: %w", err)
			}
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			if len(body) > 0 {
				if model := strings.TrimSpace(gjson.GetBytes(body, "model").String()); model != "" {
					return model, nil
				}
			}
		}
	}

	return modelFromPath(c.Request.URL.Path), nil
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
