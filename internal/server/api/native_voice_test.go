package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/voice"
)

func TestExtractNativeVoiceRequestModelReadsModelAfterLargePayload(t *testing.T) {
	body := []byte(`{"text":"` + strings.Repeat("x", 1<<20) + `","model":"speech-2.8-hd"}`)
	req := &http.Request{
		URL:  &url.URL{},
		Body: io.NopCloser(bytes.NewReader(body)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "model"}}, objects.ChannelEndpointTransportHTTP)

	require.NoError(t, err)
	require.Equal(t, "speech-2.8-hd", model)
	restored, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, body, restored)
}

func TestExtractNativeVoiceRequestModelRejectsQueryBodyMismatch(t *testing.T) {
	req := &http.Request{
		URL:  &url.URL{RawQuery: "model=allowed-model"},
		Body: io.NopCloser(strings.NewReader(`{"model":"blocked-model"}`)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "model"}}, objects.ChannelEndpointTransportHTTP)

	require.Error(t, err)
	require.Empty(t, model)
	restored, readErr := io.ReadAll(req.Body)
	require.NoError(t, readErr)
	require.JSONEq(t, `{"model":"blocked-model"}`, string(restored))
}

func TestExtractNativeVoiceRequestModelReadsDoubaoNestedModel(t *testing.T) {
	req := &http.Request{
		URL:  &url.URL{},
		Body: io.NopCloser(strings.NewReader(`{"request":{"model":"seed-tts-1.1"}}`)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "request.model"}}, objects.ChannelEndpointTransportHTTP)

	require.NoError(t, err)
	require.Equal(t, "seed-tts-1.1", model)
}

func TestExtractNativeVoiceRequestModelRejectsInvalidBodyInsteadOfUsingQuery(t *testing.T) {
	req := &http.Request{
		URL:  &url.URL{RawQuery: "model=allowed-model"},
		Body: io.NopCloser(strings.NewReader(`{"model":`)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "model"}}, objects.ChannelEndpointTransportHTTP)

	require.Error(t, err)
	require.Empty(t, model)
}

func TestExtractNativeVoiceRequestModelRejectsQueryWhenBodyModelIsEmpty(t *testing.T) {
	req := &http.Request{
		URL:  &url.URL{RawQuery: "model=allowed-model"},
		Body: io.NopCloser(strings.NewReader(`{"model":""}`)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "model"}}, objects.ChannelEndpointTransportHTTP)

	require.Error(t, err)
	require.Empty(t, model)
}

func TestExtractNativeVoiceRequestModelRequiresConfiguredHTTPModel(t *testing.T) {
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2A)
	require.True(t, ok)

	for _, body := range []string{`{}`, `{"model":""}`, `{"model":"   "}`} {
		req := &http.Request{
			URL:  &url.URL{},
			Body: io.NopCloser(strings.NewReader(body)),
		}

		model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{protocol}, objects.ChannelEndpointTransportHTTP)

		require.Error(t, err, body)
		require.Empty(t, model, body)
	}
}

func TestExtractNativeVoiceRequestModelAllowsOmittedOptionalHTTPModel(t *testing.T) {
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatDoubaoTTS)
	require.True(t, ok)
	req := &http.Request{
		URL:  &url.URL{},
		Body: io.NopCloser(strings.NewReader(`{"request":{"text":"hello"}}`)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{protocol}, objects.ChannelEndpointTransportHTTP)

	require.NoError(t, err)
	require.Empty(t, model)
}

func TestExtractNativeVoiceRequestModelRejectsQueryWithoutHTTPBody(t *testing.T) {
	req := &http.Request{URL: &url.URL{RawQuery: "model=allowed-model"}}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "model"}}, objects.ChannelEndpointTransportHTTP)

	require.Error(t, err)
	require.Empty(t, model)
}

func TestExtractNativeVoiceRequestModelRejectsQueryWhenBodyModelIsMissing(t *testing.T) {
	req := &http.Request{
		URL:  &url.URL{RawQuery: "model=allowed-model"},
		Body: io.NopCloser(strings.NewReader(`{"text":"hello"}`)),
	}

	model, err := extractNativeVoiceRequestModel(req, []objects.NativeVoiceProtocol{{ModelPath: "model"}}, objects.ChannelEndpointTransportHTTP)

	require.Error(t, err)
	require.Empty(t, model)
}

func TestResolveNativeVoiceTargetsTreatsWebSocketQueryModelAsOpaque(t *testing.T) {
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2ABidi)
	require.True(t, ok)

	channel := &biz.Channel{Channel: &ent.Channel{
		ID:              1,
		Name:            "native",
		Type:            channel.TypeMinimax,
		Status:          channel.StatusEnabled,
		SupportedModels: []string{"allowed", "blocked"},
		Endpoints: []objects.ChannelEndpoint{{
			APIFormat: protocol.APIFormat,
			Path:      protocol.Path,
			Transport: protocol.Transport,
		}},
		Credentials: objects.ChannelCredentials{APIKey: "provider-key"},
	}}
	selector := voice.NewCandidateSelector(func() []*biz.Channel { return []*biz.Channel{channel} }, nil)
	apiKey := &ent.APIKey{Profiles: &objects.APIKeyProfiles{
		ActiveProfile: "voice",
		Profiles: []objects.APIKeyProfile{{
			Name:     "voice",
			ModelIDs: []string{"allowed"},
		}},
	}}
	req := httptest.NewRequest(http.MethodGet, "/ws/v1/t2a_v2_bidi?model=allowed", nil)
	req = req.WithContext(contexts.WithAPIKey(req.Context(), apiKey))

	_, targets, err := resolveNativeVoiceTargets(req.Context(), selector, req, objects.ChannelEndpointTransportWebSocket)

	require.Error(t, err)
	require.Empty(t, targets)
	require.Equal(t, "allowed", req.URL.Query().Get("model"))
}

func TestNativeVoiceEndpointBaseURLUsesNativeDefaultWhenEndpointIsEmpty(t *testing.T) {
	baseURL := nativeVoiceEndpointBaseURL("", "https://openspeech.bytedance.com")

	require.Equal(t, "https://openspeech.bytedance.com", baseURL)
}

func TestNativeVoiceEndpointBaseURLPreservesExplicitOverride(t *testing.T) {
	baseURL := nativeVoiceEndpointBaseURL("https://voice-proxy.test/prefix", "https://openspeech.bytedance.com")

	require.Equal(t, "https://voice-proxy.test/prefix", baseURL)
}

func TestRegisterNativeVoiceWebSocketRoutesAcceptsBailianInferenceAlias(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterNativeVoiceWebSocketRoutes(router, &NativeVoiceHandlers{
		HandleWebSocket: func(c *gin.Context) { c.Status(http.StatusNoContent) },
	})

	for _, path := range []string{"/api-ws/v1/inference", "/api-ws/v1/inference/"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		require.Equal(t, http.StatusNoContent, recorder.Code, path)
	}
}
