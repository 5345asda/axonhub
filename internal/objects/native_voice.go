package objects

import (
	"net/http"
	"net/url"
	"strings"
)

// NativeVoiceAPIFormat values are the finite provider-native formats that may
// be configured in ChannelEndpoint.APIFormat. The format identifies the
// provider contract; the registry below supplies its exact method, path and
// transport.
const (
	NativeVoiceAPIFormatBailianASRRealtime  = "bailian/asr_realtime"
	NativeVoiceAPIFormatBailianASRInference = "bailian/asr_inference"
	NativeVoiceAPIFormatBailianTTSRealtime  = "bailian/tts_realtime"
	NativeVoiceAPIFormatBailianTTSInference = "bailian/tts_inference"
	NativeVoiceAPIFormatBailianTTS          = "bailian/tts"
	NativeVoiceAPIFormatBailianMultimodal   = "bailian/multimodal_generation"

	// Keep this value unchanged: it is already persisted on production MiniMax
	// channels and is the legacy HTTP T2A endpoint.
	NativeVoiceAPIFormatMiniMaxT2A     = "minimax/t2a_v2"
	NativeVoiceAPIFormatMiniMaxT2AWS   = "minimax/t2a_v2_ws"
	NativeVoiceAPIFormatMiniMaxT2ABidi = "minimax/t2a_v2_bidi"

	NativeVoiceAPIFormatDoubaoASRBidi     = "doubao/asr_bidi"
	NativeVoiceAPIFormatDoubaoASRNostream = "doubao/asr_nostream"
	NativeVoiceAPIFormatDoubaoASR         = "doubao/asr"
	NativeVoiceAPIFormatDoubaoTTSBidi     = "doubao/tts_bidi"
	NativeVoiceAPIFormatDoubaoTTSWS       = "doubao/tts_ws"
	NativeVoiceAPIFormatDoubaoTTS         = "doubao/tts"
	NativeVoiceAPIFormatDoubaoTTSSSE      = "doubao/tts_sse"
)

type NativeVoiceAuthMode string

const (
	NativeVoiceAuthMiniMax   NativeVoiceAuthMode = "minimax_bearer"
	NativeVoiceAuthDashScope NativeVoiceAuthMode = "dashscope_bearer"
	NativeVoiceAuthDoubaoV3  NativeVoiceAuthMode = "doubao_v3"
)

// NativeVoiceProtocol is metadata for one supported provider-native route.
// It contains no credentials and does not permit caller-defined paths.
type NativeVoiceProtocol struct {
	APIFormat string
	Method    string
	Path      string
	Transport string
	// ModelPath identifies the canonical JSON body model field for HTTP routes.
	// An empty value means the model is carried in the query or an opaque frame.
	ModelPath             string
	ModelRequired         bool
	DefaultBaseURL        string
	AuthMode              NativeVoiceAuthMode
	InspectBusinessStatus bool
}

var nativeVoiceProtocols = []NativeVoiceProtocol{
	{
		APIFormat: NativeVoiceAPIFormatBailianASRRealtime, Method: http.MethodGet,
		Path: "/api-ws/v1/realtime", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://dashscope.aliyuncs.com", AuthMode: NativeVoiceAuthDashScope,
	},
	{
		APIFormat: NativeVoiceAPIFormatBailianASRInference, Method: http.MethodGet,
		Path: "/api-ws/v1/inference", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://dashscope.aliyuncs.com", AuthMode: NativeVoiceAuthDashScope,
	},
	{
		APIFormat: NativeVoiceAPIFormatBailianTTSRealtime, Method: http.MethodGet,
		Path: "/api-ws/v1/realtime", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://dashscope.aliyuncs.com", AuthMode: NativeVoiceAuthDashScope,
	},
	{
		APIFormat: NativeVoiceAPIFormatBailianTTSInference, Method: http.MethodGet,
		Path: "/api-ws/v1/inference", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://dashscope.aliyuncs.com", AuthMode: NativeVoiceAuthDashScope,
	},
	{
		APIFormat: NativeVoiceAPIFormatBailianTTS, Method: http.MethodPost,
		Path: "/api/v1/services/audio/tts/SpeechSynthesizer", Transport: ChannelEndpointTransportHTTP,
		ModelPath: "model", ModelRequired: true,
		DefaultBaseURL: "https://dashscope.aliyuncs.com", AuthMode: NativeVoiceAuthDashScope,
	},
	{
		APIFormat: NativeVoiceAPIFormatBailianMultimodal, Method: http.MethodPost,
		Path: "/api/v1/services/aigc/multimodal-generation/generation", Transport: ChannelEndpointTransportHTTP,
		ModelPath: "model", ModelRequired: true,
		DefaultBaseURL: "https://dashscope.aliyuncs.com", AuthMode: NativeVoiceAuthDashScope,
	},
	{
		APIFormat: NativeVoiceAPIFormatMiniMaxT2A, Method: http.MethodPost,
		Path: "/v1/t2a_v2", Transport: ChannelEndpointTransportHTTP,
		ModelPath: "model", ModelRequired: true,
		DefaultBaseURL: "https://api.minimax.cn", AuthMode: NativeVoiceAuthMiniMax,
		InspectBusinessStatus: true,
	},
	{
		APIFormat: NativeVoiceAPIFormatMiniMaxT2AWS, Method: http.MethodGet,
		Path: "/ws/v1/t2a_v2", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://api.minimax.cn", AuthMode: NativeVoiceAuthMiniMax,
	},
	{
		APIFormat: NativeVoiceAPIFormatMiniMaxT2ABidi, Method: http.MethodGet,
		Path: "/ws/v1/t2a_v2_bidi", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://api.minimax.cn", AuthMode: NativeVoiceAuthMiniMax,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoASRBidi, Method: http.MethodGet,
		Path: "/api/v3/sauc/bigmodel_async", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoASRNostream, Method: http.MethodGet,
		Path: "/api/v3/sauc/bigmodel_nostream", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoASR, Method: http.MethodGet,
		Path: "/api/v3/sauc/bigmodel", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoTTSBidi, Method: http.MethodGet,
		Path: "/api/v3/tts/bidirection", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoTTSWS, Method: http.MethodGet,
		Path: "/api/v3/tts/unidirectional/stream", Transport: ChannelEndpointTransportWebSocket,
		DefaultBaseURL: "wss://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoTTS, Method: http.MethodPost,
		Path: "/api/v3/tts/unidirectional", Transport: ChannelEndpointTransportHTTP,
		ModelPath:      "request.model",
		DefaultBaseURL: "https://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
	{
		APIFormat: NativeVoiceAPIFormatDoubaoTTSSSE, Method: http.MethodPost,
		Path: "/api/v3/tts/unidirectional/sse", Transport: ChannelEndpointTransportHTTP,
		ModelPath:      "request.model",
		DefaultBaseURL: "https://openspeech.bytedance.com", AuthMode: NativeVoiceAuthDoubaoV3,
	},
}

var nativeVoiceProtocolByAPIFormat = func() map[string]NativeVoiceProtocol {
	result := make(map[string]NativeVoiceProtocol, len(nativeVoiceProtocols))
	for _, protocol := range nativeVoiceProtocols {
		result[protocol.APIFormat] = protocol
	}
	return result
}()

// NativeVoiceProtocolByAPIFormat resolves an explicitly configured format.
func NativeVoiceProtocolByAPIFormat(apiFormat string) (NativeVoiceProtocol, bool) {
	protocol, ok := nativeVoiceProtocolByAPIFormat[apiFormat]
	return protocol, ok
}

func IsNativeVoiceAPIFormat(apiFormat string) bool {
	_, ok := NativeVoiceProtocolByAPIFormat(apiFormat)
	return ok
}

// NativeVoiceEndpointTransport treats omitted transport as HTTP for backwards
// compatible HTTP endpoint records. WebSocket native endpoints must be explicit.
func NativeVoiceEndpointTransport(endpoint ChannelEndpoint) string {
	if endpoint.Transport != "" {
		return endpoint.Transport
	}
	return ChannelEndpointTransportHTTP
}

func normalizeNativeVoicePath(raw string) (string, bool) {
	if raw == "" || !strings.HasPrefix(raw, "/") {
		return "", false
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Path == "" {
		return "", false
	}
	// XiaoZhi's Bailian TTS adapter uses this provider-accepted trailing-slash
	// spelling. Keep the registry canonical while accepting only that alias.
	if parsed.Path == "/api-ws/v1/inference/" {
		parsed.Path = "/api-ws/v1/inference"
	}
	return parsed.Path, true
}

// LookupNativeVoiceProtocols returns all registered protocols for an exact
// method/path/transport tuple. Bailian intentionally has two protocols on its
// shared realtime path; callers must disambiguate with configured format/model.
func LookupNativeVoiceProtocols(method, path, transport string) []NativeVoiceProtocol {
	normalizedPath, ok := normalizeNativeVoicePath(path)
	if !ok {
		return nil
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	transport = strings.TrimSpace(transport)
	result := make([]NativeVoiceProtocol, 0, 2)
	for _, protocol := range nativeVoiceProtocols {
		if strings.ToUpper(protocol.Method) == method && protocol.Path == normalizedPath && protocol.Transport == transport {
			result = append(result, protocol)
		}
	}
	return result
}

// NativeVoiceProtocolAllowsBaseURLScheme accepts URL schemes that can carry
// the registered transport. HTTP(S) is accepted for WebSocket routes because
// the relay upgrades it to WS(S) when dialing upstream.
func NativeVoiceProtocolAllowsBaseURLScheme(apiFormat, scheme string) bool {
	protocol, ok := NativeVoiceProtocolByAPIFormat(apiFormat)
	if !ok {
		return false
	}
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if protocol.Transport == ChannelEndpointTransportHTTP {
		return scheme == "http" || scheme == "https"
	}
	return scheme == "http" || scheme == "https" || scheme == "ws" || scheme == "wss"
}
