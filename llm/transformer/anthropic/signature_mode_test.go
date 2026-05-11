package anthropic

import (
	"encoding/json"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/auth"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

func TestOutboundStream_SignaturePassthroughModePreservesRawSignature(t *testing.T) {
	transformer, err := NewOutboundTransformerWithConfig(&Config{
		Type:           PlatformDirect,
		BaseURL:        "https://api.anthropic.com",
		APIKeyProvider: auth.NewStaticKeyProvider("test-key"),
		SignatureMode:  AnthropicSignatureModePassthrough,
	})
	require.NoError(t, err)

	rawEvent, err := json.Marshal(StreamEvent{
		Type: "content_block_delta",
		Delta: &StreamDelta{
			Type:      lo.ToPtr("signature_delta"),
			Signature: lo.ToPtr("raw_sig"),
		},
	})
	require.NoError(t, err)

	ctx := t.Context()
	stream := streams.SliceStream([]*httpclient.StreamEvent{
		{Type: "content_block_delta", Data: rawEvent},
	})

	transformed, err := transformer.TransformStream(ctx, nil, stream)
	require.NoError(t, err)
	require.True(t, transformed.Next())

	resp := transformed.Current()
	require.NotNil(t, resp)
	require.NotNil(t, resp.Choices[0].Delta)
	require.Equal(t, "raw_sig", lo.FromPtr(resp.Choices[0].Delta.ReasoningSignature))
}

func TestConvertToLlmResponse_SignaturePassthroughModePreservesRawSignature(t *testing.T) {
	resp := &Message{
		ID:   "msg_passthrough",
		Type: "message",
		Role: "assistant",
		Content: []MessageContentBlock{
			{
				Type:      "thinking",
				Thinking:  lo.ToPtr("thinking"),
				Signature: lo.ToPtr("raw_sig"),
			},
			{
				Type: "text",
				Text: lo.ToPtr("done"),
			},
		},
	}

	result := convertToLlmResponseWithConfig(resp, PlatformDirect, &Config{
		SignatureMode: AnthropicSignatureModePassthrough,
	}, t.Context())

	require.NotNil(t, result)
	require.Len(t, result.Choices, 1)
	require.NotNil(t, result.Choices[0].Message)
	require.Equal(t, "raw_sig", lo.FromPtr(result.Choices[0].Message.ReasoningSignature))
}

func TestInboundTransformResponse_PassthroughModeDoesNotGeneratePlaceholderSignature(t *testing.T) {
	transformer := NewInboundTransformerWithConfig(&Config{
		SignatureMode: AnthropicSignatureModePassthrough,
	})

	resp, err := transformer.TransformResponse(t.Context(), &llm.Response{
		ID:    "resp_1",
		Model: "claude-opus-4-7",
		Choices: []llm.Choice{
			{
				Index: 0,
				Message: &llm.Message{
					Role: "assistant",
					Content: llm.MessageContent{
						Content: lo.ToPtr("answer"),
					},
					ReasoningContent: lo.ToPtr("thinking"),
				},
			},
		},
	})
	require.NoError(t, err)

	var body Message
	err = json.Unmarshal(resp.Body, &body)
	require.NoError(t, err)
	require.Len(t, body.Content, 2)
	require.Equal(t, "thinking", body.Content[0].Type)
	require.Nil(t, body.Content[0].Signature)
}

func TestInboundStream_PassthroughModeOmitsPlaceholderSignatureDelta(t *testing.T) {
	transformer := NewInboundTransformerWithConfig(&Config{
		SignatureMode: AnthropicSignatureModePassthrough,
	})

	stream := streams.SliceStream([]*llm.Response{
		buildChunk("msg_passthrough", "claude-opus-4-7", withUsage(10, 1)),
		buildChunk("msg_passthrough", "claude-opus-4-7", withReasoningContent("Think")),
		buildChunk("msg_passthrough", "claude-opus-4-7", withTextContent("Answer")),
		buildChunk("msg_passthrough", "claude-opus-4-7", withFinishReason("stop")),
	})

	out, err := transformer.TransformStream(t.Context(), stream)
	require.NoError(t, err)

	var events []StreamEvent
	for out.Next() {
		var ev StreamEvent
		err = json.Unmarshal(out.Current().Data, &ev)
		require.NoError(t, err)
		events = append(events, ev)
	}
	require.NoError(t, out.Err())

	for _, ev := range events {
		if ev.Delta != nil && ev.Delta.Type != nil {
			require.NotEqual(t, "signature_delta", *ev.Delta.Type)
		}
	}
}
