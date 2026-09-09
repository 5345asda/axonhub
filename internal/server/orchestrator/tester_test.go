package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

// TestBuildTestRequestUsesConfiguredPrompts verifies ordinary channel tests retain their configured prompts.
func TestBuildTestRequestUsesConfiguredPrompts(t *testing.T) {
	req := buildChannelTestRequest("test-model", true, "system prompt", "user prompt", false)

	require.Equal(t, "test-model", req.Model)
	require.Len(t, req.Messages, 2)
	require.Equal(t, "system", req.Messages[0].Role)
	require.Equal(t, "system prompt", *req.Messages[0].Content.Content)
	require.Equal(t, "user", req.Messages[1].Role)
	require.Equal(t, "user prompt", *req.Messages[1].Content.Content)
	require.Equal(t, int64(256), *req.MaxCompletionTokens)
	require.True(t, *req.Stream)
}

// TestBuildTestRequestUsesPingForResponsesWebSocket verifies WebSocket tests use a minimal compatible payload.
func TestBuildTestRequestUsesPingForResponsesWebSocket(t *testing.T) {
	req := buildChannelTestRequest("test-model", false, "system prompt", "user prompt", true)

	require.Equal(t, "test-model", req.Model)
	require.Len(t, req.Messages, 1)
	require.Equal(t, "user", req.Messages[0].Role)
	require.Equal(t, responsesWebSocketTestPrompt, *req.Messages[0].Content.Content)
	require.Nil(t, req.MaxCompletionTokens)
	require.True(t, *req.Stream)
}

// TestUsesResponsesWebSocket verifies explicit and URL-inferred WebSocket transports.
func TestUsesResponsesWebSocket(t *testing.T) {
	t.Run("inferred from channel base URL", func(t *testing.T) {
		ch := &biz.Channel{Channel: &ent.Channel{
			Type:    channel.TypeOpenaiResponses,
			BaseURL: "wss://api.openai.com/v1",
		}}

		require.True(t, usesResponsesWebSocket(ch))
	})

	t.Run("explicit transport", func(t *testing.T) {
		ch := &biz.Channel{Channel: &ent.Channel{
			Type:    channel.TypeOpenai,
			BaseURL: "https://api.example.com/v1",
			Endpoints: []objects.ChannelEndpoint{{
				APIFormat: llm.APIFormatOpenAIResponse.String(),
				Transport: objects.ChannelEndpointTransportWebSocket,
			}},
		}}

		require.True(t, usesResponsesWebSocket(ch))
	})

	t.Run("HTTP transport", func(t *testing.T) {
		ch := &biz.Channel{Channel: &ent.Channel{
			Type:    channel.TypeOpenaiResponses,
			BaseURL: "https://api.openai.com/v1",
		}}

		require.False(t, usesResponsesWebSocket(ch))
	})
}

func TestTestChannelUsesNativeBailianASRProbe(t *testing.T) {
	ctx, client := setupTest(t)
	const (
		model  = "qwen-audio-3.0-asr-flash-streaming"
		apiKey = "upstream-test-key"
	)

	type runTask struct {
		Header struct {
			Action    string `json:"action"`
			TaskID    string `json:"task_id"`
			Streaming string `json:"streaming"`
		} `json:"header"`
		Payload struct {
			TaskGroup  string `json:"task_group"`
			Task       string `json:"task"`
			Function   string `json:"function"`
			Model      string `json:"model"`
			Parameters struct {
				Format     string `json:"format"`
				SampleRate int    `json:"sample_rate"`
			} `json:"parameters"`
			Input map[string]any `json:"input"`
		} `json:"payload"`
	}

	received := make(chan runTask, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			http.Error(w, "native ASR requires websocket", http.StatusBadRequest)
			return
		}
		if r.URL.Path != "/api-ws/v1/inference" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			http.Error(w, "unexpected authorization", http.StatusUnauthorized)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var event runTask
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Errorf("decode run-task: %v", err)
			return
		}
		received <- event
		_ = conn.WriteJSON(map[string]any{"header": map[string]any{"event": "task-started", "task_id": event.Header.TaskID}})
	}))
	defer server.Close()

	channelID := createNativeBailianASRTestChannel(t, ctx, client, server.URL, model, apiKey)
	result, err := newNativeASRTestChannelOrchestrator(t, client).TestChannel(ctx, objects.GUID{ID: channelID}, nil, nil)

	require.NoError(t, err)
	require.True(t, result.Success)
	require.Nil(t, result.Error)

	select {
	case event := <-received:
		require.Equal(t, "run-task", event.Header.Action)
		require.NotEmpty(t, event.Header.TaskID)
		require.Equal(t, "duplex", event.Header.Streaming)
		require.Equal(t, "audio", event.Payload.TaskGroup)
		require.Equal(t, "asr", event.Payload.Task)
		require.Equal(t, "recognition", event.Payload.Function)
		require.Equal(t, model, event.Payload.Model)
		require.Equal(t, "pcm", event.Payload.Parameters.Format)
		require.Equal(t, 16000, event.Payload.Parameters.SampleRate)
		require.Empty(t, event.Payload.Input)
	case <-time.After(time.Second):
		t.Fatal("native Bailian ASR probe did not send run-task")
	}
}

func TestTestChannelReturnsRedactedNativeBailianASRTaskFailure(t *testing.T) {
	ctx, client := setupTest(t)
	const (
		model  = "qwen-audio-3.0-asr-flash-streaming"
		apiKey = "upstream-test-key"
	)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			http.Error(w, "native ASR requires websocket", http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var request struct {
			Header struct {
				TaskID string `json:"task_id"`
			} `json:"header"`
		}
		if json.Unmarshal(raw, &request) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"header": map[string]any{
			"event":         "task-failed",
			"task_id":       request.Header.TaskID,
			"error_code":    "InvalidParameter",
			"error_message": "invalid format: Bearer " + apiKey,
		}})
	}))
	defer server.Close()

	channelID := createNativeBailianASRTestChannel(t, ctx, client, server.URL, model, apiKey)
	result, err := newNativeASRTestChannelOrchestrator(t, client).TestChannel(ctx, objects.GUID{ID: channelID}, nil, nil)

	require.NoError(t, err)
	require.False(t, result.Success)
	require.NotNil(t, result.Error)
	require.Contains(t, *result.Error, "InvalidParameter")
	require.Contains(t, *result.Error, "invalid format")
	require.Contains(t, *result.Error, "[REDACTED]")
	require.NotContains(t, *result.Error, apiKey)
}

func TestNativeBailianASRTestEndpointRequiresExactDirectModel(t *testing.T) {
	const model = "qwen-audio-3.0-asr-flash-streaming"
	newChannel := func() *biz.Channel {
		return &biz.Channel{Channel: &ent.Channel{
			Type:            channel.TypeBailian,
			SupportedModels: []string{model},
			Endpoints: []objects.ChannelEndpoint{{
				APIFormat: objects.NativeVoiceAPIFormatBailianASRInference,
				Path:      "/api-ws/v1/inference",
				Transport: objects.ChannelEndpointTransportWebSocket,
			}},
			Settings: &objects.ChannelSettings{ModelProtocols: []objects.ModelProtocol{{
				Model:      model,
				APIFormats: []string{objects.NativeVoiceAPIFormatBailianASRInference},
			}}},
		}}
	}
	ch := newChannel()

	_, ok := nativeBailianASRTestEndpoint(ch, model)
	require.True(t, ok)

	_, ok = nativeBailianASRTestEndpoint(ch, "qwen-audio-3.0-asr-flash-streaming-alias")
	require.False(t, ok)

	ch.Type = channel.TypeOpenai
	_, ok = nativeBailianASRTestEndpoint(ch, model)
	require.False(t, ok)

	for _, test := range []struct {
		name   string
		mutate func(*biz.Channel)
	}{
		{
			name: "wrong path",
			mutate: func(ch *biz.Channel) {
				ch.Endpoints[0].Path = "/api-ws/v1/realtime"
			},
		},
		{
			name: "non websocket transport",
			mutate: func(ch *biz.Channel) {
				ch.Endpoints[0].Transport = objects.ChannelEndpointTransportHTTP
			},
		},
		{
			name: "ambiguous protocol api formats",
			mutate: func(ch *biz.Channel) {
				ch.Settings.ModelProtocols[0].APIFormats = []string{
					objects.NativeVoiceAPIFormatBailianASRInference,
					"bailian/asr_realtime",
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ch := newChannel()
			test.mutate(ch)
			_, ok := nativeBailianASRTestEndpoint(ch, model)
			require.False(t, ok)
		})
	}
}

func TestNativeBailianASRHandshakeErrorWithoutBody(t *testing.T) {
	require.Equal(t, "native Bailian ASR handshake failed", nativeBailianASRHandshakeError(&http.Response{StatusCode: http.StatusUnauthorized}))
}

func TestSanitizeNativeBailianASRErrorRedactsURLSafeKey(t *testing.T) {
	secret := "sk-abcdefghijk_-"
	redacted := sanitizeNativeBailianASRError("provider rejected " + secret)

	require.Equal(t, "provider rejected [REDACTED]", redacted)
}

func createNativeBailianASRTestChannel(
	t *testing.T,
	ctx context.Context,
	client *ent.Client,
	baseURL string,
	model string,
	apiKey string,
) int {
	t.Helper()
	entity, err := client.Channel.Create().
		SetType(channel.TypeBailian).
		SetName("native-bailian-asr-test").
		SetBaseURL(baseURL).
		SetCredentials(objects.ChannelCredentials{APIKey: apiKey}).
		SetSupportedModels([]string{model}).
		SetDefaultTestModel(model).
		SetEndpoints([]objects.ChannelEndpoint{{
			APIFormat: objects.NativeVoiceAPIFormatBailianASRInference,
			Path:      "/api-ws/v1/inference",
			BaseURL:   baseURL,
			Transport: objects.ChannelEndpointTransportWebSocket,
		}}).
		SetSettings(&objects.ChannelSettings{ModelProtocols: []objects.ModelProtocol{{
			Model:      model,
			APIFormats: []string{objects.NativeVoiceAPIFormatBailianASRInference},
		}}}).
		SetStatus(channel.StatusEnabled).
		Save(ctx)
	require.NoError(t, err)
	return entity.ID
}

func newNativeASRTestChannelOrchestrator(t *testing.T, client *ent.Client) *TestChannelOrchestrator {
	t.Helper()
	channelService := biz.NewChannelServiceForTest(client)
	t.Cleanup(channelService.Stop)
	systemService := biz.NewSystemService(biz.SystemServiceParams{
		CacheConfig: xcache.Config{Mode: xcache.ModeMemory},
		Ent:         client,
	})
	usageLogService := biz.NewUsageLogService(client, systemService, channelService)
	dataStorageService := biz.NewDataStorageService(biz.DataStorageServiceParams{
		SystemService: systemService,
		CacheConfig:   xcache.Config{Mode: xcache.ModeMemory},
		Client:        client,
	})
	requestService := biz.NewRequestService(
		client,
		xcache.Config{Mode: xcache.ModeMemory},
		systemService,
		usageLogService,
		dataStorageService,
		biz.NewLiveStreamRegistry(),
	)
	promptProtectionRuleService := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{
		CacheConfig: xcache.Config{Mode: xcache.ModeMemory},
		Ent:         client,
	})
	t.Cleanup(promptProtectionRuleService.Stop)

	return NewTestChannelOrchestrator(
		channelService,
		requestService,
		systemService,
		usageLogService,
		promptProtectionRuleService,
		httpclient.NewHttpClient(),
	)
}
