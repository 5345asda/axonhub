package objects

import "testing"

func TestNativeVoiceMiniMaxDefaultsUseChinaProductionHost(t *testing.T) {
	for apiFormat, wantBaseURL := range map[string]string{
		NativeVoiceAPIFormatMiniMaxT2A:     "https://api.minimax.cn",
		NativeVoiceAPIFormatMiniMaxT2AWS:   "wss://api.minimax.cn",
		NativeVoiceAPIFormatMiniMaxT2ABidi: "wss://api.minimax.cn",
	} {
		protocol, ok := NativeVoiceProtocolByAPIFormat(apiFormat)
		if !ok {
			t.Fatalf("missing protocol %q", apiFormat)
		}
		if protocol.DefaultBaseURL != wantBaseURL {
			t.Fatalf("%s default base URL = %q, want %q", apiFormat, protocol.DefaultBaseURL, wantBaseURL)
		}
	}
}

func TestLookupNativeVoiceProtocols(t *testing.T) {
	tests := []struct {
		method, path, transport string
		want                    []string
	}{
		{method: "POST", path: "/v1/t2a_v2", transport: ChannelEndpointTransportHTTP, want: []string{NativeVoiceAPIFormatMiniMaxT2A}},
		{method: "GET", path: "/ws/v1/t2a_v2_bidi", transport: ChannelEndpointTransportWebSocket, want: []string{NativeVoiceAPIFormatMiniMaxT2ABidi}},
		{method: "GET", path: "/api/v3/tts/bidirection", transport: ChannelEndpointTransportWebSocket, want: []string{NativeVoiceAPIFormatDoubaoTTSBidi}},
		{method: "GET", path: "/api-ws/v1/realtime", transport: ChannelEndpointTransportWebSocket, want: []string{NativeVoiceAPIFormatBailianASRRealtime, NativeVoiceAPIFormatBailianTTSRealtime}},
		{method: "GET", path: "/api-ws/v1/inference", transport: ChannelEndpointTransportWebSocket, want: []string{NativeVoiceAPIFormatBailianASRInference, NativeVoiceAPIFormatBailianTTSInference}},
		{method: "GET", path: "/api-ws/v1/inference/", transport: ChannelEndpointTransportWebSocket, want: []string{NativeVoiceAPIFormatBailianASRInference, NativeVoiceAPIFormatBailianTTSInference}},
		{method: "POST", path: "/api/v1/services/aigc/multimodal-generation/generation", transport: ChannelEndpointTransportHTTP, want: []string{NativeVoiceAPIFormatBailianMultimodal}},
	}

	for _, tt := range tests {
		protocols := LookupNativeVoiceProtocols(tt.method, tt.path, tt.transport)
		if len(protocols) != len(tt.want) {
			t.Fatalf("%s %s: got %d protocols, want %d", tt.method, tt.path, len(protocols), len(tt.want))
		}
		for i, protocol := range protocols {
			if protocol.APIFormat != tt.want[i] {
				t.Fatalf("%s %s: protocol[%d] = %q, want %q", tt.method, tt.path, i, protocol.APIFormat, tt.want[i])
			}
		}
	}
}

func TestLookupNativeVoiceProtocolsExcludesAsyncTaskRoutes(t *testing.T) {
	for _, path := range []string{"/api/v1/services/audio/asr/transcription", "/v1/audio/task/query", "/api/v3/tts/async/submit", "/api/v3/auc/bigmodel/idle/submit"} {
		if protocols := LookupNativeVoiceProtocols("POST", path, ChannelEndpointTransportHTTP); len(protocols) != 0 {
			t.Fatalf("async task route was accepted: %s", path)
		}
	}
}
