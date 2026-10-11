package middleware

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/project"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/tracing"
)

type apiKeyModelStickyFailingReader struct{}

func (apiKeyModelStickyFailingReader) Read([]byte) (int, error) {
	return 0, errors.New("request body should not be read")
}

func (apiKeyModelStickyFailingReader) Close() error { return nil }

func newAPIKeyModelTraceContext(t *testing.T, method, target, body, apiKey string) *gin.Context {
	t.Helper()

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req = req.WithContext(contexts.WithAPIKey(req.Context(), &ent.APIKey{Key: apiKey}))
	c.Request = req

	return c
}

func TestTryGetTraceIDFromAPIKeyAndModel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const (
		secret = "test-sticky-secret"
		apiKey = "sk-test-key-for-sticky-trace"
		body   = `{"model":"gpt-test","messages":[]}`
	)

	first := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", body, apiKey)
	firstTraceID, err := tryGetTraceIDFromAPIKeyAndModel(first, secret)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(firstTraceID, tracing.APIKeyModelTracePrefix))
	require.NotContains(t, firstTraceID, apiKey)
	require.NotContains(t, firstTraceID, "gpt-test")

	restoredBody, err := io.ReadAll(first.Request.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(restoredBody))

	second := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", body, apiKey)
	secondTraceID, err := tryGetTraceIDFromAPIKeyAndModel(second, secret)
	require.NoError(t, err)
	require.Equal(t, firstTraceID, secondTraceID)

	differentModel := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", `{"model":"gpt-other"}`, apiKey)
	differentModelTraceID, err := tryGetTraceIDFromAPIKeyAndModel(differentModel, secret)
	require.NoError(t, err)
	require.NotEqual(t, firstTraceID, differentModelTraceID)

	differentKey := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", body, "sk-other-key")
	differentKeyTraceID, err := tryGetTraceIDFromAPIKeyAndModel(differentKey, secret)
	require.NoError(t, err)
	require.NotEqual(t, firstTraceID, differentKeyTraceID)
}

func TestTryGetTraceIDFromAPIKeyAndModelUsesLastDuplicateJSONModel(t *testing.T) {
	first := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", `{"model":"first"}`, "sk-test-key")
	second := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", `{"model":"second"}`, "sk-test-key")
	duplicate := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", `{"model":"first","model":"second"}`, "sk-test-key")

	firstID, err := tryGetTraceIDFromAPIKeyAndModel(first, "test-sticky-secret")
	require.NoError(t, err)
	secondID, err := tryGetTraceIDFromAPIKeyAndModel(second, "test-sticky-secret")
	require.NoError(t, err)
	duplicateID, err := tryGetTraceIDFromAPIKeyAndModel(duplicate, "test-sticky-secret")
	require.NoError(t, err)

	require.NotEqual(t, firstID, secondID)
	require.Equal(t, secondID, duplicateID)
}

func TestTryGetTraceIDFromAPIKeyAndModelSkipsNoAuthBeforeReadingBody(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Body = apiKeyModelStickyFailingReader{}
	req = req.WithContext(contexts.WithAPIKey(req.Context(), &ent.APIKey{Key: "no-auth", Type: apikey.TypeNoauth}))
	c.Request = req

	traceID, err := tryGetTraceIDFromAPIKeyAndModel(c, "test-sticky-secret")
	require.NoError(t, err)
	require.Empty(t, traceID)
}

func TestTryGetTraceIDFromAPIKeyAndModelSkipsNoAuthKeyValueBeforeReadingBody(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Body = apiKeyModelStickyFailingReader{}
	// Keep Type unset to defend against synthetic no-auth contexts that are
	// assembled before the API key entity is fully loaded.
	req = req.WithContext(contexts.WithAPIKey(req.Context(), &ent.APIKey{Key: biz.NoAuthAPIKeyValue}))
	c.Request = req

	traceID, err := tryGetTraceIDFromAPIKeyAndModel(c, "test-sticky-secret")
	require.NoError(t, err)
	require.Empty(t, traceID)
}

func TestTryGetTraceIDFromAPIKeyAndModelNormalizesGzipBody(t *testing.T) {
	originalBody := []byte(`{"model":"gpt-gzip","messages":[]}`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(originalBody)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	c := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", "", "sk-test-key")
	c.Request.Body = io.NopCloser(bytes.NewReader(compressed.Bytes()))
	c.Request.ContentLength = int64(compressed.Len())
	c.Request.Header.Set("Content-Encoding", "gzip")

	traceID, err := tryGetTraceIDFromAPIKeyAndModel(c, "test-sticky-secret")
	require.NoError(t, err)
	require.NotEmpty(t, traceID)
	require.Equal(t, int64(len(originalBody)), c.Request.ContentLength)
	require.Empty(t, c.Request.Header.Get("Content-Encoding"))
	gotBody, err := io.ReadAll(c.Request.Body)
	require.NoError(t, err)
	require.Equal(t, originalBody, gotBody)
}

func TestWithTraceAPIKeyModelStickyRejectsMalformedEncodingAsBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(contexts.WithAPIKey(c.Request.Context(), &ent.APIKey{Key: "sk-test-key"}))
		c.Next()
	})
	router.Use(WithTrace(tracing.Config{
		APIKeyModelSticky:       true,
		APIKeyModelStickySecret: "test-sticky-secret",
	}, nil))
	router.POST("/v1/chat/completions", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("not-gzip"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTryGetTraceIDFromAPIKeyAndModel_UsesGeminiPathModel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c := newAPIKeyModelTraceContext(
		t,
		http.MethodPost,
		"/v1beta/models/gemini-2.5-pro:generateContent",
		`{"contents":[]}`,
		"sk-test-key-for-sticky-trace",
	)

	traceID, err := tryGetTraceIDFromAPIKeyAndModel(c, "test-sticky-secret")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(traceID, tracing.APIKeyModelTracePrefix))
}

func TestTryGetTraceIDFromAPIKeyAndModel_IgnoresPlainModelLookupPath(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c := newAPIKeyModelTraceContext(t, http.MethodGet, "/v1/models/gpt-test", "", "sk-test-key-for-sticky-trace")
	traceID, err := tryGetTraceIDFromAPIKeyAndModel(c, "test-sticky-secret")
	require.NoError(t, err)
	require.Empty(t, traceID)
}

func TestTryGetTraceIDFromAPIKeyAndModel_RequiresSecret(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c := newAPIKeyModelTraceContext(t, http.MethodPost, "/v1/chat/completions", `{"model":"gpt-test"}`, "sk-test-key")
	traceID, err := tryGetTraceIDFromAPIKeyAndModel(c, "")
	require.Error(t, err)
	require.Empty(t, traceID)
}

func TestAPIKeyModelStickySecretIsExcludedFromConfigJSON(t *testing.T) {
	configJSON, err := json.Marshal(tracing.Config{
		APIKeyModelSticky:       true,
		APIKeyModelStickySecret: "test-sticky-secret",
	})
	require.NoError(t, err)
	require.Contains(t, string(configJSON), `"api_key_model_sticky":true`)
	require.NotContains(t, string(configJSON), "test-sticky-secret")
}

func TestAPIKeyModelStickySecretIsExcludedFromConfigYAML(t *testing.T) {
	configYAML, err := yaml.Marshal(tracing.Config{
		APIKeyModelSticky:       true,
		APIKeyModelStickySecret: "test-sticky-secret",
	})
	require.NoError(t, err)
	require.Contains(t, string(configYAML), "api_key_model_sticky: true")
	require.NotContains(t, string(configYAML), "test-sticky-secret")
	require.NotContains(t, string(configYAML), "api_key_model_sticky_secret")
}

func TestWithTraceAPIKeyModelStickyTakesPrecedence(t *testing.T) {
	gin.SetMode(gin.TestMode)

	config := tracing.Config{
		APIKeyModelSticky:       true,
		APIKeyModelStickySecret: "test-sticky-secret",
		ResponseTraceHeaders:    []string{"AH-Trace-Id"},
	}

	router, client, traceService := setupTestTraceMiddleware(t)
	defer client.Close()

	ctx := authz.WithTestBypass(httptest.NewRequest(http.MethodGet, "/", nil).Context())
	ctx = ent.NewContext(ctx, client)
	testProject, err := client.Project.Create().
		SetName("sticky-trace-project").
		SetStatus(project.StatusActive).
		Save(ctx)
	require.NoError(t, err)

	router.Use(func(c *gin.Context) {
		reqCtx := authz.WithTestBypass(c.Request.Context())
		reqCtx = ent.NewContext(reqCtx, client)
		reqCtx = contexts.WithProjectID(reqCtx, testProject.ID)
		reqCtx = contexts.WithAPIKey(reqCtx, &ent.APIKey{Key: "sk-test-key-for-sticky-trace"})
		c.Request = c.Request.WithContext(reqCtx)
		c.Next()
	})
	router.Use(WithTrace(config, traceService))

	var resolvedTraceID string
	router.POST("/v1/chat/completions", func(c *gin.Context) {
		resolvedTraceID, _ = tracing.GetTraceID(c.Request.Context())
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"model":"gpt-test","messages":[]}`, string(body))
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewBufferString(`{"model":"gpt-test","messages":[]}`),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("AH-Trace-Id", "client-supplied-trace")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, strings.HasPrefix(resolvedTraceID, tracing.APIKeyModelTracePrefix))
	require.NotEqual(t, "client-supplied-trace", resolvedTraceID)
	require.Equal(t, resolvedTraceID, w.Header().Get("AH-Trace-Id"))
}
