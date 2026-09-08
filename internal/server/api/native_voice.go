package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/internal/server/voice"
)

type NativeVoiceHandlersParams struct {
	fx.In

	ChannelService              *biz.ChannelService
	SystemService               *biz.SystemService
	ChannelLimiterManager       *orchestrator.ChannelLimiterManager
	ProviderQuotaStatusProvider orchestrator.ProviderQuotaStatusProvider
}

type NativeVoiceHandlers struct {
	HandleHTTP      gin.HandlerFunc
	HandleWebSocket gin.HandlerFunc
}

func NewNativeVoiceHandlers(params NativeVoiceHandlersParams) *NativeVoiceHandlers {
	channelService := params.ChannelService
	systemService := params.SystemService

	rateLimitTracker := orchestrator.NewChannelRequestTracker()
	nativeLoadTracker := orchestrator.NewChannelRequestTracker()
	loadBalancer := orchestrator.NewLoadBalancer(
		systemService,
		nativeLoadTracker,
		orchestrator.NewErrorAwareStrategy(channelService),
		orchestrator.NewWeightRoundRobinStrategy(nativeLoadTracker),
		orchestrator.NewLatencyAwareStrategy(channelService),
		orchestrator.NewRateLimitAwareStrategy(rateLimitTracker, params.ChannelLimiterManager),
		orchestrator.NewQuotaAwareStrategy(params.ProviderQuotaStatusProvider, systemService),
	)

	selector := voice.NewCandidateSelector(channelService.GetEnabledChannels, loadBalancer.SortAllWithoutTracking)
	admission := voice.NewNativeRelayAdmission(params.ChannelLimiterManager, rateLimitTracker)
	httpRelay := voice.NewNativeHTTPRelay(admission)
	wsRelay := voice.NewNativeWebSocketRelay(admission)

	handlers := &NativeVoiceHandlers{}
	handlers.HandleHTTP = func(c *gin.Context) {
		serveNativeVoiceHTTP(c, selector, loadBalancer, httpRelay)
	}
	handlers.HandleWebSocket = func(c *gin.Context) {
		serveNativeVoiceWebSocket(c, selector, loadBalancer, wsRelay)
	}

	return handlers
}

func RegisterNativeVoiceHTTPRoutes(router gin.IRoutes, handlers *NativeVoiceHandlers) {
	if router == nil || handlers == nil || handlers.HandleHTTP == nil {
		return
	}

	router.POST("/v1/t2a_v2", handlers.HandleHTTP)
	router.POST("/api/v1/services/audio/tts/SpeechSynthesizer", handlers.HandleHTTP)
	router.POST("/api/v1/services/aigc/multimodal-generation/generation", handlers.HandleHTTP)
	router.POST("/api/v3/tts/unidirectional", handlers.HandleHTTP)
	router.POST("/api/v3/tts/unidirectional/sse", handlers.HandleHTTP)
}

func RegisterNativeVoiceWebSocketRoutes(router gin.IRoutes, handlers *NativeVoiceHandlers) {
	if router == nil || handlers == nil || handlers.HandleWebSocket == nil {
		return
	}

	router.GET("/ws/v1/t2a_v2", handlers.HandleWebSocket)
	router.GET("/ws/v1/t2a_v2_bidi", handlers.HandleWebSocket)
	router.GET("/api-ws/v1/realtime", handlers.HandleWebSocket)
	router.GET("/api-ws/v1/inference", handlers.HandleWebSocket)
	router.GET("/api-ws/v1/inference/", handlers.HandleWebSocket)
	router.GET("/api/v3/sauc/bigmodel_async", handlers.HandleWebSocket)
	router.GET("/api/v3/sauc/bigmodel_nostream", handlers.HandleWebSocket)
	router.GET("/api/v3/sauc/bigmodel", handlers.HandleWebSocket)
	router.GET("/api/v3/tts/bidirection", handlers.HandleWebSocket)
	router.GET("/api/v3/tts/unidirectional/stream", handlers.HandleWebSocket)
}

func serveNativeVoiceHTTP(c *gin.Context, selector *voice.CandidateSelector, loadBalancer *orchestrator.LoadBalancer, relay *voice.NativeHTTPRelay) {
	ctx := c.Request.Context()
	protocol, targets, err := resolveNativeVoiceTargets(ctx, selector, c.Request, objects.ChannelEndpointTransportHTTP)
	if err != nil {
		JSONError(c, http.StatusNotFound, err)
		return
	}

	if protocol.APIFormat == "" || len(targets) == 0 {
		JSONError(c, http.StatusNotFound, errors.New("native voice route is not configured"))
		return
	}

	trackNativeVoiceSelection(loadBalancer, targets)
	if err := relay.Relay(ctx, c.Writer, c.Request, targets); err != nil && !c.Writer.Written() {
		JSONError(c, nativeVoiceErrorStatus(err), err)
	}
}

func serveNativeVoiceWebSocket(c *gin.Context, selector *voice.CandidateSelector, loadBalancer *orchestrator.LoadBalancer, relay *voice.NativeWebSocketRelay) {
	ctx := c.Request.Context()
	protocol, targets, err := resolveNativeVoiceTargets(ctx, selector, c.Request, objects.ChannelEndpointTransportWebSocket)
	if err != nil {
		JSONError(c, http.StatusNotFound, err)
		return
	}

	if protocol.APIFormat == "" || len(targets) == 0 {
		JSONError(c, http.StatusNotFound, errors.New("native voice route is not configured"))
		return
	}

	trackNativeVoiceSelection(loadBalancer, targets)
	if err := relay.Relay(ctx, c.Writer, c.Request, targets); err != nil && !c.Writer.Written() {
		JSONError(c, nativeVoiceErrorStatus(err), err)
	}
}

func trackNativeVoiceSelection(loadBalancer *orchestrator.LoadBalancer, targets []voice.NativeRelayTarget) {
	if loadBalancer == nil || len(targets) == 0 || targets[0].Channel == nil {
		return
	}
	loadBalancer.TrackSelection(&orchestrator.ChannelModelsCandidate{Channel: targets[0].Channel})
}

func nativeVoiceErrorStatus(err error) int {
	if admissionErr, ok := errors.AsType[*voice.NativeRelayAdmissionError](err); ok && admissionErr.StatusCode > 0 {
		return admissionErr.StatusCode
	}
	return http.StatusBadGateway
}

func resolveNativeVoiceTargets(
	ctx context.Context,
	selector *voice.CandidateSelector,
	req *http.Request,
	transport string,
) (objects.NativeVoiceProtocol, []voice.NativeRelayTarget, error) {
	if req == nil {
		return objects.NativeVoiceProtocol{}, nil, errors.New("native voice request is required")
	}
	if req.URL == nil {
		return objects.NativeVoiceProtocol{}, nil, errors.New("native voice request URL is required")
	}
	if selector == nil {
		return objects.NativeVoiceProtocol{}, nil, errors.New("native voice candidate selector is unavailable")
	}

	resourceID := extractNativeVoiceResourceID(req)

	protocols := objects.LookupNativeVoiceProtocols(req.Method, req.URL.Path, transport)
	if len(protocols) == 0 {
		return objects.NativeVoiceProtocol{}, nil, errors.New("native voice route is not configured")
	}
	requestModel, err := extractNativeVoiceRequestModel(req, protocols, transport)
	if err != nil {
		return objects.NativeVoiceProtocol{}, nil, err
	}

	type protocolTargets struct {
		protocol objects.NativeVoiceProtocol
		targets  []voice.NativeRelayTarget
	}

	var routeGroups []protocolTargets
	for _, protocol := range protocols {
		candidates, err := selector.Select(ctx, voice.CandidateRequest{
			APIFormat:   protocol.APIFormat,
			Model:       requestModel,
			ResourceID:  resourceID,
			Stream:      transport == objects.ChannelEndpointTransportWebSocket,
			OpaqueModel: transport == objects.ChannelEndpointTransportWebSocket,
		})
		if err != nil {
			return objects.NativeVoiceProtocol{}, nil, err
		}
		if len(candidates) == 0 {
			continue
		}

		targets := make([]voice.NativeRelayTarget, 0, len(candidates))
		for _, candidate := range candidates {
			endpoint := candidate.Endpoint
			endpoint.BaseURL = nativeVoiceEndpointBaseURL(endpoint.BaseURL, protocol.DefaultBaseURL)
			targets = append(targets, voice.NativeRelayTarget{
				Channel:  candidate.Channel,
				Endpoint: endpoint,
				Protocol: candidate.Protocol,
			})
		}
		if len(targets) > 0 {
			routeGroups = append(routeGroups, protocolTargets{protocol: protocol, targets: targets})
		}
	}

	if len(routeGroups) == 0 {
		return objects.NativeVoiceProtocol{}, nil, errors.New("no native voice candidate matched")
	}
	if len(routeGroups) == 1 {
		return routeGroups[0].protocol, routeGroups[0].targets, nil
	}

	if protocolHint := nativeVoiceSharedRouteModelHint(req, protocols, transport); protocolHint != "" {
		hintedGroups := make([]protocolTargets, 0, len(routeGroups))
		for _, group := range routeGroups {
			if nativeVoiceProtocolMatchesHint(group.targets, protocolHint) {
				hintedGroups = append(hintedGroups, group)
			}
		}
		if len(hintedGroups) == 1 {
			return hintedGroups[0].protocol, hintedGroups[0].targets, nil
		}
	}

	return objects.NativeVoiceProtocol{}, nil, fmt.Errorf("native voice route %s is ambiguous; configure one api_format or an exact model protocol mapping", req.URL.Path)
}

// nativeVoiceSharedRouteModelHint is only a protocol disambiguation hint for a
// shared native WebSocket path. It is not used as the request model or as a
// Profile authorization signal.
func nativeVoiceSharedRouteModelHint(req *http.Request, protocols []objects.NativeVoiceProtocol, transport string) string {
	if req == nil || req.URL == nil || transport != objects.ChannelEndpointTransportWebSocket || len(protocols) < 2 {
		return ""
	}

	models, present := req.URL.Query()["model"]
	if !present || len(models) != 1 {
		return ""
	}

	return strings.TrimSpace(models[0])
}

func nativeVoiceProtocolMatchesHint(targets []voice.NativeRelayTarget, model string) bool {
	for _, target := range targets {
		if target.Channel != nil && slices.Contains(target.Channel.ForcedAPIFormats(model), target.Protocol.APIFormat) {
			return true
		}
	}

	return false
}

// Empty endpoint BaseURLs use the native registry default, never the channel's
// LLM BaseURL. Providers can use distinct chat and native speech authorities.
func nativeVoiceEndpointBaseURL(endpointBaseURL, defaultBaseURL string) string {
	if baseURL := strings.TrimSpace(endpointBaseURL); baseURL != "" {
		return baseURL
	}
	return strings.TrimSpace(defaultBaseURL)
}

func extractNativeVoiceRequestModel(req *http.Request, protocols []objects.NativeVoiceProtocol, transport string) (string, error) {
	if req == nil {
		return "", nil
	}
	if transport == objects.ChannelEndpointTransportWebSocket {
		// Native WebSocket protocols declare the model in an opaque frame after
		// the handshake. Preserve the query for upstream relay, but never trust it
		// for candidate or Profile authorization.
		return "", nil
	}

	var queryModel string
	if req.URL != nil {
		queryModel = strings.TrimSpace(req.URL.Query().Get("model"))
	}

	if transport != objects.ChannelEndpointTransportHTTP || len(protocols) != 1 || protocols[0].ModelPath == "" {
		return queryModel, nil
	}
	if req.Body == nil {
		if queryModel != "" {
			return "", errors.New("native voice HTTP request model must be in the request body")
		}
		if protocols[0].ModelRequired {
			return "", errors.New("native voice HTTP request model is required in the request body")
		}
		return "", nil
	}

	originalBody := req.Body
	body, err := io.ReadAll(originalBody)
	req.Body = &nativeVoiceRequestBody{
		Reader: bytes.NewReader(body),
		Closer: originalBody,
	}
	if err != nil {
		return "", fmt.Errorf("failed to read native voice request body: %w", err)
	}

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", errors.New("native voice request body must be valid JSON")
	}

	bodyModel, bodyModelPresent, err := nativeVoiceJSONModelAtPath(payload, protocols[0].ModelPath)
	if err != nil {
		return "", err
	}
	if bodyModelPresent && queryModel != "" && bodyModel != queryModel {
		return "", errors.New("native voice request model query and body do not match")
	}
	if bodyModelPresent {
		if protocols[0].ModelRequired && bodyModel == "" {
			return "", errors.New("native voice HTTP request model must be a non-empty string")
		}
		return bodyModel, nil
	}
	if queryModel != "" {
		return "", errors.New("native voice HTTP request model must be in the request body")
	}
	if protocols[0].ModelRequired {
		return "", errors.New("native voice HTTP request model is required in the request body")
	}
	return "", nil
}

func nativeVoiceJSONModelAtPath(payload any, fieldPath string) (string, bool, error) {
	value := payload
	for field := range strings.SplitSeq(fieldPath, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return "", false, nil
		}
		value, ok = object[field]
		if !ok {
			return "", false, nil
		}
		if value == nil {
			return "", true, errors.New("native voice request model must be a string")
		}
	}

	model, ok := value.(string)
	if !ok {
		return "", true, errors.New("native voice request model must be a string")
	}
	return strings.TrimSpace(model), true, nil
}

type nativeVoiceRequestBody struct {
	io.Reader

	Closer io.Closer
}

func (b *nativeVoiceRequestBody) Close() error {
	if b == nil || b.Closer == nil {
		return nil
	}
	return b.Closer.Close()
}

func extractNativeVoiceResourceID(req *http.Request) string {
	if req == nil {
		return ""
	}

	if req.URL != nil {
		if resourceID := strings.TrimSpace(req.URL.Query().Get("resource_id")); resourceID != "" {
			return resourceID
		}
	}

	return strings.TrimSpace(req.Header.Get("X-Api-Resource-Id"))
}
