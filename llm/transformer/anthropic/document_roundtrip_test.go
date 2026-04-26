package anthropic

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

func TestAnthropicDocumentPDFRoundTrip(t *testing.T) {
	inboundTransformer := NewInboundTransformer()
	outboundTransformer, err := NewOutboundTransformer("https://api.anthropic.com", "test-api-key")
	require.NoError(t, err)

	httpReq := &httpclient.Request{
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: []byte(`{
			"model": "claude-opus-4-7",
			"max_tokens": 10240,
			"messages": [
				{
					"role": "user",
					"content": [
						{
							"type": "document",
							"source": {
								"type": "base64",
								"media_type": "application/pdf",
								"data": "JVBERi0xLjQK"
							}
						},
						{
							"type": "text",
							"text": "What text does this PDF contain? 只返回文字"
						}
					]
				}
			]
		}`),
	}

	chatReq, err := inboundTransformer.TransformRequest(t.Context(), httpReq)
	require.NoError(t, err)
	require.Len(t, chatReq.Messages, 1)
	require.Len(t, chatReq.Messages[0].Content.MultipleContent, 2)

	documentPart := chatReq.Messages[0].Content.MultipleContent[0]
	require.Equal(t, "document", documentPart.Type)
	require.NotNil(t, documentPart.Document)
	require.Equal(t, "data:application/pdf;base64,JVBERi0xLjQK", documentPart.Document.URL)
	require.Equal(t, "application/pdf", documentPart.Document.MIMEType)

	textPart := chatReq.Messages[0].Content.MultipleContent[1]
	require.Equal(t, "text", textPart.Type)
	require.Equal(t, "What text does this PDF contain? 只返回文字", lo.FromPtr(textPart.Text))

	outboundReq, err := outboundTransformer.TransformRequest(t.Context(), chatReq)
	require.NoError(t, err)

	var outboundBody map[string]any

	err = json.Unmarshal(outboundReq.Body, &outboundBody)
	require.NoError(t, err)

	messages, ok := outboundBody["messages"].([]any)
	require.True(t, ok)
	require.Len(t, messages, 1)

	message, ok := messages[0].(map[string]any)
	require.True(t, ok)

	content, ok := message["content"].([]any)
	require.True(t, ok)
	require.Len(t, content, 2)

	documentBlock, ok := content[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "document", documentBlock["type"])

	source, ok := documentBlock["source"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "base64", source["type"])
	require.Equal(t, "application/pdf", source["media_type"])
	require.Equal(t, "JVBERi0xLjQK", source["data"])

	textBlock, ok := content[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "text", textBlock["type"])
	require.Equal(t, "What text does this PDF contain? 只返回文字", textBlock["text"])
}

func TestAnthropicOutboundPreservesDocumentPDFBlock(t *testing.T) {
	outboundTransformer, err := NewOutboundTransformer("https://api.anthropic.com", "test-api-key")
	require.NoError(t, err)

	req := &llm.Request{
		Model:     "claude-opus-4-7",
		MaxTokens: lo.ToPtr(int64(10240)),
		Messages: []llm.Message{
			{
				Role: "user",
				Content: llm.MessageContent{
					MultipleContent: []llm.MessageContentPart{
						{
							Type: "document",
							Document: &llm.DocumentURL{
								URL:      "data:application/pdf;base64,JVBERi0xLjQK",
								MIMEType: "application/pdf",
							},
						},
						{
							Type: "text",
							Text: lo.ToPtr("What text does this PDF contain? 只返回文字"),
						},
					},
				},
			},
		},
	}

	outboundReq, err := outboundTransformer.TransformRequest(t.Context(), req)
	require.NoError(t, err)

	var outboundBody map[string]any

	err = json.Unmarshal(outboundReq.Body, &outboundBody)
	require.NoError(t, err)

	messages, ok := outboundBody["messages"].([]any)
	require.True(t, ok)
	require.Len(t, messages, 1)

	message, ok := messages[0].(map[string]any)
	require.True(t, ok)

	content, ok := message["content"].([]any)
	require.True(t, ok)
	require.Len(t, content, 2)

	documentBlock, ok := content[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "document", documentBlock["type"])

	source, ok := documentBlock["source"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "base64", source["type"])
	require.Equal(t, "application/pdf", source["media_type"])
	require.Equal(t, "JVBERi0xLjQK", source["data"])
}
