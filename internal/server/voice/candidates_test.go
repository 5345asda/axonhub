package voice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
)

func nativeChannelProvider(channels ...*biz.Channel) ChannelProviderFn {
	return func() []*biz.Channel { return channels }
}

func TestCandidateSelectorUsesExistingChannelEndpointForMiniMaxT2A(t *testing.T) {
	selector := NewCandidateSelector(nativeChannelProvider(
		nativeCandidateChannel(10, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{{
			APIFormat: objects.NativeVoiceAPIFormatMiniMaxT2A,
			Path:      "/v1/t2a_v2",
		}}, nil),
	), nil)

	candidates, err := selector.Select(context.Background(), CandidateRequest{
		APIFormat: objects.NativeVoiceAPIFormatMiniMaxT2A,
		Model:     "speech-2.8-hd",
	})
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "/v1/t2a_v2", candidates[0].Endpoint.Path)
}

func TestCandidateSelectorRestrictsCandidatesToActiveProfileAndProtocol(t *testing.T) {
	allowed := nativeCandidateChannel(1, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2A, ""),
	}, []string{"voice"})
	skippedByProfile := nativeCandidateChannel(2, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2A, ""),
	}, []string{"voice"})
	wrongProtocol := nativeCandidateChannel(3, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, ""),
	}, []string{"voice"})

	apiKey := &ent.APIKey{Profiles: &objects.APIKeyProfiles{
		ActiveProfile: "voice",
		Profiles: []objects.APIKeyProfile{{
			Name:       "voice",
			ChannelIDs: []int{1, 3},
			ModelIDs:   []string{"speech-2.8-hd"},
		}},
	}}
	ctx := contexts.WithAPIKey(context.Background(), apiKey)
	selector := NewCandidateSelector(nativeChannelProvider(allowed, skippedByProfile, wrongProtocol), func(_ context.Context, candidates []*orchestrator.ChannelModelsCandidate, _ string, _ bool) []*orchestrator.ChannelModelsCandidate {
		require.Len(t, candidates, 1)
		require.Equal(t, objects.NativeVoiceAPIFormatMiniMaxT2A, candidates[0].APIFormat)
		return candidates
	})

	candidates, err := selector.Select(ctx, CandidateRequest{
		APIFormat: objects.NativeVoiceAPIFormatMiniMaxT2A,
		Model:     "speech-2.8-hd",
	})
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, 1, candidates[0].Channel.ID)
}

func TestCandidateSelectorAllowsModelInFirstWebSocketFrame(t *testing.T) {
	selector := NewCandidateSelector(nativeChannelProvider(
		nativeCandidateChannel(1, []string{"speech-2.8-hd", "speech-2.8-turbo"}, []objects.ChannelEndpoint{
			nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, ""),
		}, nil),
	), nil)

	candidates, err := selector.Select(context.Background(), CandidateRequest{
		APIFormat:   objects.NativeVoiceAPIFormatMiniMaxT2ABidi,
		Stream:      true,
		OpaqueModel: true,
	})
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Empty(t, candidates[0].Model.ActualModel)
}

func TestCandidateSelectorRejectsModelAliasesForTransparentRelay(t *testing.T) {
	channel := nativeCandidateChannel(1, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2A, ""),
	}, nil)
	channel.Settings = &objects.ChannelSettings{}
	channel.Settings.ModelMappings = []objects.ModelMapping{{From: "xiaozhi-speech", To: "speech-2.8-hd"}}

	selector := NewCandidateSelector(nativeChannelProvider(channel), nil)
	candidates, err := selector.Select(context.Background(), CandidateRequest{
		APIFormat: objects.NativeVoiceAPIFormatMiniMaxT2A,
		Model:     "xiaozhi-speech",
	})

	require.NoError(t, err)
	require.Empty(t, candidates)
}

func TestCandidateSelectorHonorsHiddenDirectModelForTransparentRelay(t *testing.T) {
	channel := nativeCandidateChannel(1, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2A, ""),
	}, nil)
	channel.Settings = &objects.ChannelSettings{HideOriginalModels: true}
	selector := NewCandidateSelector(nativeChannelProvider(channel), nil)

	candidates, err := selector.Select(context.Background(), CandidateRequest{
		APIFormat: objects.NativeVoiceAPIFormatMiniMaxT2A,
		Model:     "speech-2.8-hd",
	})

	require.NoError(t, err)
	require.Empty(t, candidates)
}

func TestCandidateSelectorAllowsOpaqueWebSocketModelWhenProfileMatchesChannel(t *testing.T) {
	allowed := nativeCandidateChannel(1, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, ""),
	}, nil)
	skipped := nativeCandidateChannel(2, []string{"other-model"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, ""),
	}, nil)
	apiKey := &ent.APIKey{Profiles: &objects.APIKeyProfiles{
		ActiveProfile: "voice",
		Profiles:      []objects.APIKeyProfile{{Name: "voice", ModelIDs: []string{"speech-2.8-hd"}}},
	}}
	selector := NewCandidateSelector(nativeChannelProvider(allowed, skipped), nil)

	candidates, err := selector.Select(contexts.WithAPIKey(context.Background(), apiKey), CandidateRequest{
		APIFormat:   objects.NativeVoiceAPIFormatMiniMaxT2ABidi,
		Stream:      true,
		OpaqueModel: true,
	})

	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, 1, candidates[0].Channel.ID)
}

func TestCandidateSelectorRejectsOpaqueWebSocketModelOutsideProfileScope(t *testing.T) {
	channel := nativeCandidateChannel(1, []string{"speech-2.8-hd", "speech-2.8-turbo"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, ""),
	}, nil)
	apiKey := &ent.APIKey{Profiles: &objects.APIKeyProfiles{
		ActiveProfile: "voice",
		Profiles:      []objects.APIKeyProfile{{Name: "voice", ModelIDs: []string{"speech-2.8-hd"}}},
	}}
	selector := NewCandidateSelector(nativeChannelProvider(channel), nil)

	candidates, err := selector.Select(contexts.WithAPIKey(context.Background(), apiKey), CandidateRequest{
		APIFormat:   objects.NativeVoiceAPIFormatMiniMaxT2ABidi,
		Stream:      true,
		OpaqueModel: true,
	})

	require.NoError(t, err)
	require.Empty(t, candidates)
}

func TestCandidateSelectorRejectsOpaqueWebSocketAliasInProfile(t *testing.T) {
	channel := nativeCandidateChannel(1, []string{"speech-2.8-hd"}, []objects.ChannelEndpoint{
		nativeEndpoint(t, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, ""),
	}, nil)
	channel.Settings = &objects.ChannelSettings{
		ModelMappings: []objects.ModelMapping{{From: "xiaozhi-speech", To: "speech-2.8-hd"}},
	}
	apiKey := &ent.APIKey{Profiles: &objects.APIKeyProfiles{
		ActiveProfile: "voice",
		Profiles:      []objects.APIKeyProfile{{Name: "voice", ModelIDs: []string{"xiaozhi-speech"}}},
	}}
	selector := NewCandidateSelector(nativeChannelProvider(channel), nil)

	candidates, err := selector.Select(contexts.WithAPIKey(context.Background(), apiKey), CandidateRequest{
		APIFormat: objects.NativeVoiceAPIFormatMiniMaxT2ABidi,
		Stream:    true,
	})

	require.NoError(t, err)
	require.Empty(t, candidates)
}

func TestCandidateSelectorFiltersDoubaoResourceID(t *testing.T) {
	selector := NewCandidateSelector(nativeChannelProvider(
		nativeCandidateChannel(1, []string{"doubao-tts"}, []objects.ChannelEndpoint{
			nativeEndpoint(t, objects.NativeVoiceAPIFormatDoubaoTTSBidi, "resource-a"),
		}, nil),
		nativeCandidateChannel(2, []string{"doubao-tts"}, []objects.ChannelEndpoint{
			nativeEndpoint(t, objects.NativeVoiceAPIFormatDoubaoTTSBidi, "resource-b"),
		}, nil),
	), nil)

	candidates, err := selector.Select(context.Background(), CandidateRequest{
		APIFormat:  objects.NativeVoiceAPIFormatDoubaoTTSBidi,
		Model:      "doubao-tts",
		ResourceID: "resource-b",
	})
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, 2, candidates[0].Channel.ID)
}

func TestCandidateSelectorRejectsUnregisteredFormat(t *testing.T) {
	selector := NewCandidateSelector(nativeChannelProvider(), nil)
	candidates, err := selector.Select(context.Background(), CandidateRequest{APIFormat: "minimax/t2a_async"})
	require.Error(t, err)
	require.Nil(t, candidates)
}

func nativeCandidateChannel(id int, supportedModels []string, endpoints []objects.ChannelEndpoint, tags []string) *biz.Channel {
	return &biz.Channel{Channel: &ent.Channel{
		ID:              id,
		Name:            "native",
		Type:            channel.TypeMinimax,
		Status:          channel.StatusEnabled,
		SupportedModels: supportedModels,
		Endpoints:       endpoints,
		Tags:            tags,
		Credentials:     objects.ChannelCredentials{APIKey: "provider-key"},
	}}
}

func nativeEndpoint(t *testing.T, apiFormat, resourceID string) objects.ChannelEndpoint {
	t.Helper()
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(apiFormat)
	require.True(t, ok)
	endpoint := objects.ChannelEndpoint{
		APIFormat:  apiFormat,
		Path:       protocol.Path,
		ResourceID: resourceID,
	}
	if protocol.Transport == objects.ChannelEndpointTransportWebSocket {
		endpoint.Transport = protocol.Transport
	}
	return endpoint
}
