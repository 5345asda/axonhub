package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xjson"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/pipeline/stream"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer/openai"
)

const testChannelAPIKeysMaxConcurrency = 8

const responsesWebSocketTestPrompt = "ping"

const (
	nativeBailianASRTestModel        = "qwen-audio-3.0-asr-flash-streaming"
	nativeBailianASRTestTimeout      = 10 * time.Second
	nativeBailianASRTestMaxErrorBody = 8 << 10
)

var nativeBailianASRKeyPattern = regexp.MustCompile(`(?i)(?:sk|ak)-[a-z0-9_-]{8,}`)

// TestChannelOrchestrator handles channel testing functionality.
// It is stateless and can be reused across multiple test requests.
type TestChannelOrchestrator struct {
	channelService              *biz.ChannelService
	requestService              *biz.RequestService
	systemService               *biz.SystemService
	usageLogService             *biz.UsageLogService
	promptProtectionRuleService *biz.PromptProtectionRuleService
	httpClient                  *httpclient.HttpClient
	modelCircuitBreaker         *biz.ModelCircuitBreaker
	modelMapper                 *ModelMapper
	loadBalancer                *LoadBalancer
	channelLimiterManager       *ChannelLimiterManager
}

// NewTestChannelOrchestrator creates a new TestChannelOrchestrator.
func NewTestChannelOrchestrator(
	channelService *biz.ChannelService,
	requestService *biz.RequestService,
	systemService *biz.SystemService,
	usageLogService *biz.UsageLogService,
	promptProtectionRuleService *biz.PromptProtectionRuleService,
	httpClient *httpclient.HttpClient,
) *TestChannelOrchestrator {
	return &TestChannelOrchestrator{
		channelService:              channelService,
		requestService:              requestService,
		systemService:               systemService,
		usageLogService:             usageLogService,
		promptProtectionRuleService: promptProtectionRuleService,
		httpClient:                  httpClient,
		modelCircuitBreaker:         biz.NewModelCircuitBreaker(),
		modelMapper:                 NewModelMapper(),
		loadBalancer:                NewLoadBalancer(systemService, channelService, NewWeightStrategy()),
		channelLimiterManager:       NewChannelLimiterManager(),
	}
}

// TestChannelRequest represents a channel test request.
type TestChannelRequest struct {
	ChannelID objects.GUID
	ModelID   *string
}

// buildChannelTestRequest creates the request used by channel tests.
func buildChannelTestRequest(model string, useStream bool, systemPrompt string, userPrompt string, responsesWebSocket bool) *llm.Request {
	req := &llm.Request{
		Model: model,
		Messages: []llm.Message{
			{
				Role:    "system",
				Content: llm.MessageContent{Content: lo.ToPtr(systemPrompt)},
			},
			{
				Role:    "user",
				Content: llm.MessageContent{Content: lo.ToPtr(userPrompt)},
			},
		},
		MaxCompletionTokens: lo.ToPtr(int64(256)),
		Stream:              lo.ToPtr(useStream),
	}

	if responsesWebSocket {
		req.Messages = []llm.Message{{
			Role:    "user",
			Content: llm.MessageContent{Content: lo.ToPtr(responsesWebSocketTestPrompt)},
		}}
		req.MaxCompletionTokens = nil
		req.Stream = lo.ToPtr(true)
	}

	return req
}

// usesResponsesWebSocket reports whether a channel routes Responses requests over WebSocket.
func usesResponsesWebSocket(channel *biz.Channel) bool {
	if channel == nil {
		return false
	}

	for _, endpoint := range channel.ResolveEndpoints() {
		if endpoint.APIFormat != llm.APIFormatOpenAIResponse.String() && endpoint.APIFormat != llm.APIFormatOpenAIResponseCompact.String() {
			continue
		}

		transport := strings.ToLower(strings.TrimSpace(endpoint.Transport))
		if transport == objects.ChannelEndpointTransportWebSocket {
			return true
		}
		if transport != "" {
			continue
		}

		baseURL := endpoint.BaseURL
		if baseURL == "" {
			baseURL = channel.BaseURL
		}
		baseURL = strings.ToLower(strings.TrimSpace(baseURL))
		if strings.HasPrefix(baseURL, "ws://") || strings.HasPrefix(baseURL, "wss://") {
			return true
		}
	}

	return false
}

// TestChannelResult represents the result of a channel test.
type TestChannelResult struct {
	Latency float64
	Success bool
	Message *string
	Error   *string
}

type nativeBailianASRProbeHeader struct {
	Action       string `json:"action,omitempty"`
	TaskID       string `json:"task_id,omitempty"`
	Streaming    string `json:"streaming,omitempty"`
	Event        string `json:"event,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type nativeBailianASRProbeRequest struct {
	Header  nativeBailianASRProbeHeader `json:"header"`
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

type nativeBailianASRProbeEvent struct {
	Header nativeBailianASRProbeHeader `json:"header"`
}

// TestChannel tests a specific channel with a simple request.
func (processor *TestChannelOrchestrator) TestChannel(
	ctx context.Context,
	channelID objects.GUID,
	modelID *string,
	proxy *httpclient.ProxyConfig,
) (*TestChannelResult, error) {
	channel, err := processor.channelService.GetChannel(ctx, channelID.ID)
	if err != nil {
		return nil, err
	}

	testModel := lo.FromPtr(modelID)
	if testModel == "" {
		testModel = channel.DefaultTestModel
	}
	if endpoint, ok := nativeBailianASRTestEndpoint(channel, testModel); ok {
		return processor.testNativeBailianASR(ctx, channel, endpoint, proxy), nil
	}
	inbound := openai.NewInboundTransformer()
	chatProcessor := &ChatCompletionOrchestrator{
		channelSelector: NewSpecifiedChannelSelector(processor.channelService, channelID),
		RequestService:  processor.requestService,
		ChannelService:  processor.channelService,
		PromptProvider:  &stubPromptProvider{},
		PromptProtecter: processor.promptProtectionRuleService,
		PipelineFactory: pipeline.NewFactory(processor.httpClient),
		Middlewares: []pipeline.Middleware{
			stream.EnsureUsage(),
		},
		Inbound:                    inbound,
		SystemService:              processor.systemService,
		UsageLogService:            processor.usageLogService,
		proxy:                      proxy,
		ModelMapper:                processor.modelMapper,
		adaptiveLoadBalancer:       processor.loadBalancer,
		failoverLoadBalancer:       processor.loadBalancer,
		circuitBreakerLoadBalancer: processor.loadBalancer,
		channelLimiterManager:      processor.channelLimiterManager,
		modelCircuitBreaker:        processor.modelCircuitBreaker,
	}
	systemPrompt, userPrompt, err := processor.systemService.ChannelTestPrompts(ctx)
	if err != nil {
		return nil, err
	}

	// Check if the channel requires streaming
	useStream := channel != nil && channel.Policies.Stream == objects.CapabilityPolicyRequire

	llmRequest := buildChannelTestRequest(testModel, useStream, systemPrompt, userPrompt, usesResponsesWebSocket(channel))

	body, err := json.Marshal(llmRequest)
	if err != nil {
		return nil, err
	}

	// Measure latency
	startTime := time.Now()
	rawResponse, err := chatProcessor.Process(ctx, &httpclient.Request{
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: body,
	})

	rawErr := inbound.TransformError(ctx, err)
	message := gjson.GetBytes(rawErr.Body, "error.message").String()

	if err != nil {
		return &TestChannelResult{
			Latency: time.Since(startTime).Seconds(),
			Success: false,
			Message: new(""),
			Error:   new(message),
		}, nil
	}

	// Handle streaming response
	if rawResponse.ChatCompletionStream != nil {
		return processor.handleStreamResponse(ctx, rawResponse.ChatCompletionStream, startTime)
	}

	latency := time.Since(startTime).Seconds()

	// Handle non-streaming response
	response, err := xjson.To[llm.Response](rawResponse.ChatCompletion.Body)
	if err != nil {
		return &TestChannelResult{
			Latency: latency,
			Success: false,
			Message: new(""),
			Error:   new(err.Error()),
		}, nil
	}

	if len(response.Choices) == 0 {
		return &TestChannelResult{
			Latency: latency,
			Success: false,
			Message: new(""),
			Error:   new("No message in response"),
		}, nil
	}

	return &TestChannelResult{
		Latency: latency,
		Success: true,
		Message: response.Choices[0].Message.Content.Content,
		Error:   nil,
	}, nil
}

// nativeBailianASRTestEndpoint returns only the exact native ASR endpoint that
// can be tested without translating the provider protocol through ChatCompletion.
func nativeBailianASRTestEndpoint(ch *biz.Channel, model string) (objects.ChannelEndpoint, bool) {
	if ch == nil || ch.Channel == nil || ch.Type != channel.TypeBailian || model != nativeBailianASRTestModel {
		return objects.ChannelEndpoint{}, false
	}

	entry, ok := ch.GetDirectModelEntries()[model]
	if !ok || entry.ActualModel != model {
		return objects.ChannelEndpoint{}, false
	}

	formats := ch.ForcedAPIFormats(model)
	if len(formats) != 1 || formats[0] != objects.NativeVoiceAPIFormatBailianASRInference {
		return objects.ChannelEndpoint{}, false
	}

	for _, endpoint := range ch.ResolveEndpoints() {
		if endpoint.APIFormat == objects.NativeVoiceAPIFormatBailianASRInference &&
			endpoint.Path == "/api-ws/v1/inference" &&
			objects.NativeVoiceEndpointTransport(endpoint) == objects.ChannelEndpointTransportWebSocket {
			return endpoint, true
		}
	}
	return objects.ChannelEndpoint{}, false
}

func (processor *TestChannelOrchestrator) testNativeBailianASR(
	ctx context.Context,
	channel *biz.Channel,
	endpoint objects.ChannelEndpoint,
	proxy *httpclient.ProxyConfig,
) *TestChannelResult {
	startedAt := time.Now()
	result := func(success bool, message, errText string) *TestChannelResult {
		var testError *string
		if errText != "" {
			testError = lo.ToPtr(errText)
		}
		return &TestChannelResult{
			Latency: time.Since(startedAt).Seconds(),
			Success: success,
			Message: lo.ToPtr(message),
			Error:   testError,
		}
	}

	upstreamURL, err := nativeBailianASRWebSocketURL(channel, endpoint)
	if err != nil {
		return result(false, "", "native voice infrastructure error: "+err.Error())
	}

	apiKey := channel.SelectAPIKey(ctx)
	if apiKey == "" {
		return result(false, "", "native voice infrastructure error: no enabled API key")
	}

	dialer := nativeBailianASRWebSocketDialer(channel, proxy)
	probeCtx, cancel := context.WithTimeout(ctx, nativeBailianASRTestTimeout)
	defer cancel()
	conn, response, err := dialer.DialContext(probeCtx, upstreamURL, http.Header{"Authorization": {"Bearer " + apiKey}})
	if err != nil {
		if response != nil && response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError {
			return result(false, "", nativeBailianASRHandshakeError(response))
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return result(false, "", "native voice infrastructure error: failed to dial upstream: "+sanitizeNativeBailianASRError(err.Error()))
	}
	defer conn.Close()

	taskID := uuid.NewString()
	request := nativeBailianASRProbeRequest{
		Header: nativeBailianASRProbeHeader{Action: "run-task", TaskID: taskID, Streaming: "duplex"},
	}
	request.Payload.TaskGroup = "audio"
	request.Payload.Task = "asr"
	request.Payload.Function = "recognition"
	request.Payload.Model = nativeBailianASRTestModel
	request.Payload.Parameters.Format = "pcm"
	request.Payload.Parameters.SampleRate = 16000
	request.Payload.Input = map[string]any{}
	if err := conn.WriteJSON(request); err != nil {
		return result(false, "", "native voice infrastructure error: failed to write run-task: "+sanitizeNativeBailianASRError(err.Error()))
	}

	if err := conn.SetReadDeadline(time.Now().Add(nativeBailianASRTestTimeout)); err != nil {
		return result(false, "", "native voice infrastructure error: failed to set read deadline")
	}
	for {
		select {
		case <-probeCtx.Done():
			return result(false, "", "native voice infrastructure error: "+probeCtx.Err().Error())
		default:
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return result(false, "", "native voice infrastructure error: failed to read task event: "+sanitizeNativeBailianASRError(err.Error()))
		}

		var event nativeBailianASRProbeEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return result(false, "", "native voice infrastructure error: invalid task event")
		}
		if event.Header.TaskID != taskID {
			continue
		}
		switch event.Header.Event {
		case "task-started":
			return result(true, "native Bailian ASR task started", "")
		case "task-failed":
			errText := strings.Trim(strings.Join([]string{event.Header.ErrorCode, event.Header.ErrorMessage}, ": "), ": ")
			if errText == "" {
				errText = "native Bailian ASR task failed"
			}
			return result(false, "", sanitizeNativeBailianASRError(errText))
		}
	}
}

func nativeBailianASRWebSocketURL(ch *biz.Channel, endpoint objects.ChannelEndpoint) (string, error) {
	baseURL := endpoint.BaseURL
	if baseURL == "" && ch != nil {
		baseURL = ch.BaseURL
	}
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid native Bailian ASR base URL")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("invalid native Bailian ASR upstream scheme")
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawPath = ""
	parsed.Path = nativeBailianASRJoinPath(parsed.Path, endpoint.Path)
	return parsed.String(), nil
}

func nativeBailianASRJoinPath(basePath, endpointPath string) string {
	basePath = strings.Trim(strings.TrimSpace(basePath), "/")
	endpointPath = strings.Trim(strings.TrimSpace(endpointPath), "/")
	if basePath == "" {
		return "/" + endpointPath
	}
	if endpointPath == "" {
		return "/" + basePath
	}
	if endpointPath == basePath || strings.HasPrefix(endpointPath, basePath+"/") {
		return "/" + endpointPath
	}
	return path.Join("/"+basePath, endpointPath)
}

func nativeBailianASRWebSocketDialer(ch *biz.Channel, override *httpclient.ProxyConfig) *websocket.Dialer {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = nativeBailianASRTestTimeout
	if override != nil {
		dialer.Proxy = httpclient.NewHttpClientWithProxy(override).ProxyFunc()
		return &dialer
	}
	if ch != nil && ch.HTTPClient != nil {
		dialer.Proxy = ch.HTTPClient.ProxyFunc()
		if native := ch.HTTPClient.GetNativeClient(); native != nil {
			if transport, ok := native.Transport.(*http.Transport); ok && transport.TLSClientConfig != nil {
				dialer.TLSClientConfig = transport.TLSClientConfig.Clone()
			}
		}
	}
	return &dialer
}

func nativeBailianASRHandshakeError(response *http.Response) string {
	if response == nil || response.Body == nil {
		return "native Bailian ASR handshake failed"
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, nativeBailianASRTestMaxErrorBody))
	message := sanitizeNativeBailianASRError(string(body))
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	return fmt.Sprintf("native Bailian ASR handshake HTTP %d: %s", response.StatusCode, message)
}

func sanitizeNativeBailianASRError(text string) string {
	redacted := string(sanitizeResponseBody([]byte(text), nativeBailianASRTestMaxErrorBody))
	return strings.TrimSpace(nativeBailianASRKeyPattern.ReplaceAllString(redacted, "[REDACTED]"))
}

// handleStreamResponse processes a streaming response and accumulates the content.
func (processor *TestChannelOrchestrator) handleStreamResponse(
	ctx context.Context,
	stream streams.Stream[*httpclient.StreamEvent],
	startTime time.Time,
) (*TestChannelResult, error) {
	defer func() {
		_ = stream.Close()
	}()

	// Accumulate stream chunks
	var accumulatedContent string

	for stream.Next() {
		select {
		case <-ctx.Done():
			return &TestChannelResult{
				Latency: time.Since(startTime).Seconds(),
				Success: false,
				Message: lo.ToPtr(accumulatedContent),
				Error:   lo.ToPtr(ctx.Err().Error()),
			}, nil
		default:
		}

		event := stream.Current()
		if event == nil {
			continue
		}

		// The stream may end with a "[DONE]" message which is not valid JSON.
		if string(event.Data) == "[DONE]" {
			continue
		}

		// Parse the stream event data
		var chunk llm.Response
		if err := json.Unmarshal(event.Data, &chunk); err != nil {
			log.Warn(ctx, "failed to unmarshal stream event data", log.Cause(err), log.ByteString("data", event.Data))
			continue
		}

		// Accumulate content from the first choice
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta != nil && chunk.Choices[0].Delta.Content.Content != nil {
			accumulatedContent += *chunk.Choices[0].Delta.Content.Content
		}
	}

	// Calculate latency after processing all stream events
	latency := time.Since(startTime).Seconds()

	if err := ctx.Err(); err != nil {
		return &TestChannelResult{
			Latency: latency,
			Success: false,
			Message: lo.ToPtr(accumulatedContent),
			Error:   lo.ToPtr(err.Error()),
		}, nil
	}

	if stream.Err() != nil {
		return &TestChannelResult{
			Latency: latency,
			Success: false,
			Message: lo.ToPtr(""),
			Error:   lo.ToPtr(stream.Err().Error()),
		}, nil
	}

	if accumulatedContent == "" {
		return &TestChannelResult{
			Latency: latency,
			Success: false,
			Message: lo.ToPtr(""),
			Error:   lo.ToPtr("No content in stream response"),
		}, nil
	}

	return &TestChannelResult{
		Latency: latency,
		Success: true,
		Message: lo.ToPtr(accumulatedContent),
		Error:   nil,
	}, nil
}

// TestAPIKeyResult represents the result of testing a single API key.
type TestAPIKeyResult struct {
	KeyPrefix string
	Success   bool
	Latency   float64
	Error     *string
	Disabled  bool
}

// TestChannelAPIKeysResult represents the aggregated result of testing all API keys.
type TestChannelAPIKeysResult struct {
	ChannelID    objects.GUID
	Total        int
	SuccessCount int
	FailedCount  int
	Results      []*TestAPIKeyResult
}

// TestChannelAPIKeys tests all API keys for a specific channel individually.
func (processor *TestChannelOrchestrator) TestChannelAPIKeys(
	ctx context.Context,
	channelID objects.GUID,
	modelID *string,
	proxy *httpclient.ProxyConfig,
) (*TestChannelAPIKeysResult, error) {
	ch, err := processor.channelService.GetChannel(ctx, channelID.ID)
	if err != nil {
		return nil, err
	}

	allKeys := ch.Credentials.GetAllAPIKeys()
	if len(allKeys) == 0 {
		return nil, fmt.Errorf("no API keys configured for channel")
	}

	// Build disabled set
	disabledSet := make(map[string]struct{}, len(ch.DisabledAPIKeys))
	for _, dk := range ch.DisabledAPIKeys {
		disabledSet[dk.Key] = struct{}{}
	}

	testModel := lo.FromPtr(modelID)
	if testModel == "" {
		testModel = ch.DefaultTestModel
	}

	useStream := ch.Policies.Stream == objects.CapabilityPolicyRequire
	responsesWebSocket := usesResponsesWebSocket(ch)
	systemPrompt, userPrompt, err := processor.systemService.ChannelTestPrompts(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]*TestAPIKeyResult, len(allKeys))

	var (
		successCount int32
		failedCount  int32
	)

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(testChannelAPIKeysMaxConcurrency, len(allKeys)))

	for i, key := range allKeys {
		index := i
		apiKey := key

		group.Go(func() error {
			select {
			case <-groupCtx.Done():
				errMsg := groupCtx.Err().Error()
				results[index] = &TestAPIKeyResult{
					KeyPrefix: maskAPIKey(apiKey),
					Success:   false,
					Error:     &errMsg,
				}

				atomic.AddInt32(&failedCount, 1)

				return nil
			default:
			}

			result := processor.testSingleKey(groupCtx, channelID, apiKey, testModel, useStream, responsesWebSocket, proxy, systemPrompt, userPrompt)
			_, isDisabled := disabledSet[apiKey]
			result.Disabled = isDisabled
			results[index] = result

			if result.Success {
				atomic.AddInt32(&successCount, 1)
				return nil
			}

			atomic.AddInt32(&failedCount, 1)

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return &TestChannelAPIKeysResult{
		ChannelID:    channelID,
		Total:        len(allKeys),
		SuccessCount: int(successCount),
		FailedCount:  int(failedCount),
		Results:      results,
	}, nil
}

// TestSingleAPIKey tests a single API key for a channel.
// It verifies that the provided key belongs to the channel before testing.
func (processor *TestChannelOrchestrator) TestSingleAPIKey(
	ctx context.Context,
	channelID objects.GUID,
	key string,
	modelID *string,
	proxy *httpclient.ProxyConfig,
) (*TestAPIKeyResult, error) {
	ch, err := processor.channelService.GetChannel(ctx, channelID.ID)
	if err != nil {
		return nil, err
	}

	// Verify the provided key is actually configured for this channel.
	channelKeys := ch.Credentials.GetAllAPIKeys()
	if len(channelKeys) == 0 {
		return nil, fmt.Errorf("no API keys configured for channel")
	}

	keyBelongsToChannel := lo.Contains(channelKeys, key)
	if !keyBelongsToChannel {
		return nil, fmt.Errorf("the provided API key is not configured for this channel")
	}

	testModel := lo.FromPtr(modelID)
	if testModel == "" {
		testModel = ch.DefaultTestModel
	}

	useStream := ch.Policies.Stream == objects.CapabilityPolicyRequire
	responsesWebSocket := usesResponsesWebSocket(ch)
	systemPrompt, userPrompt, err := processor.systemService.ChannelTestPrompts(ctx)
	if err != nil {
		return nil, err
	}

	disabledSet := make(map[string]struct{}, len(ch.DisabledAPIKeys))
	for _, dk := range ch.DisabledAPIKeys {
		disabledSet[dk.Key] = struct{}{}
	}

	result := processor.testSingleKey(ctx, channelID, key, testModel, useStream, responsesWebSocket, proxy, systemPrompt, userPrompt)
	_, isDisabled := disabledSet[key]
	result.Disabled = isDisabled

	return result, nil
}

// testSingleKey tests a single API key by forcing the use of a specific key via SetAPIKey.
func (processor *TestChannelOrchestrator) testSingleKey(
	ctx context.Context,
	channelID objects.GUID,
	key string,
	testModel string,
	useStream bool,
	responsesWebSocket bool,
	proxy *httpclient.ProxyConfig,
	systemPrompt string,
	userPrompt string,
) *TestAPIKeyResult {
	keyPrefix := maskAPIKey(key)

	inbound := openai.NewInboundTransformer()

	chatProcessor := &ChatCompletionOrchestrator{
		channelSelector: &SpecifiedChannelSelector{
			ChannelService: processor.channelService,
			ChannelID:      channelID,
			SelectedAPIKey: key,
		},
		RequestService:  processor.requestService,
		ChannelService:  processor.channelService,
		PromptProvider:  &stubPromptProvider{},
		PromptProtecter: processor.promptProtectionRuleService,
		PipelineFactory: pipeline.NewFactory(processor.httpClient),
		Middlewares: []pipeline.Middleware{
			stream.EnsureUsage(),
		},
		Inbound:                    inbound,
		SystemService:              processor.systemService,
		UsageLogService:            processor.usageLogService,
		proxy:                      proxy,
		ModelMapper:                processor.modelMapper,
		adaptiveLoadBalancer:       processor.loadBalancer,
		failoverLoadBalancer:       processor.loadBalancer,
		circuitBreakerLoadBalancer: processor.loadBalancer,
		channelLimiterManager:      processor.channelLimiterManager,
		modelCircuitBreaker:        processor.modelCircuitBreaker,
	}

	llmRequest := buildChannelTestRequest(testModel, useStream, systemPrompt, userPrompt, responsesWebSocket)

	body, err := json.Marshal(llmRequest)
	if err != nil {
		errMsg := err.Error()

		return &TestAPIKeyResult{
			KeyPrefix: keyPrefix,
			Success:   false,
			Error:     &errMsg,
		}
	}

	startTime := time.Now()

	rawResponse, err := chatProcessor.Process(ctx, &httpclient.Request{
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: body,
	})
	if err != nil {
		rawErr := inbound.TransformError(ctx, err)
		message := gjson.GetBytes(rawErr.Body, "error.message").String()

		return &TestAPIKeyResult{
			KeyPrefix: keyPrefix,
			Success:   false,
			Latency:   time.Since(startTime).Seconds(),
			Error:     new(message),
		}
	}

	// Handle streaming response
	if rawResponse.ChatCompletionStream != nil {
		streamResult, _ := processor.handleStreamResponse(ctx, rawResponse.ChatCompletionStream, startTime)

		return &TestAPIKeyResult{
			KeyPrefix: keyPrefix,
			Success:   streamResult.Success,
			Latency:   streamResult.Latency,
			Error:     streamResult.Error,
		}
	}

	latency := time.Since(startTime).Seconds()

	// Handle non-streaming response
	response, err := xjson.To[llm.Response](rawResponse.ChatCompletion.Body)
	if err != nil {
		errMsg := err.Error()

		return &TestAPIKeyResult{
			KeyPrefix: keyPrefix,
			Success:   false,
			Latency:   latency,
			Error:     &errMsg,
		}
	}

	if len(response.Choices) == 0 {
		errMsg := "No message in response"

		return &TestAPIKeyResult{
			KeyPrefix: keyPrefix,
			Success:   false,
			Latency:   latency,
			Error:     &errMsg,
		}
	}

	return &TestAPIKeyResult{
		KeyPrefix: keyPrefix,
		Success:   true,
		Latency:   latency,
	}
}

// maskAPIKey returns a masked version of the API key for display.
func maskAPIKey(key string) string {
	if len(key) <= 8 {
		return "****"
	}

	return key[:4] + "****" + key[len(key)-4:]
}
