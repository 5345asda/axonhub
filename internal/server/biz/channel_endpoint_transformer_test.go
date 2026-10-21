package biz

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
)

func TestAnthropicPlatformForCustomEndpoint(t *testing.T) {
	tests := []struct {
		channelType channel.Type
		want        anthropic.PlatformType
	}{
		{channel.TypeDeepseek, anthropic.PlatformDeepSeek},
		{channel.TypeDeepseekAnthropic, anthropic.PlatformDeepSeek},
		{channel.TypeDoubao, anthropic.PlatformDoubao},
		{channel.TypeDoubaoAnthropic, anthropic.PlatformDoubao},
		{channel.TypeMoonshot, anthropic.PlatformMoonshot},
		{channel.TypeMoonshotAnthropic, anthropic.PlatformMoonshot},
		{channel.TypeZhipu, anthropic.PlatformZhipu},
		{channel.TypeZhipuAnthropic, anthropic.PlatformZhipu},
		{channel.TypeZai, anthropic.PlatformZai},
		{channel.TypeZaiAnthropic, anthropic.PlatformZai},
		{channel.TypeBailian, anthropic.PlatformDirect},
	}

	for _, tt := range tests {
		t.Run(string(tt.channelType), func(t *testing.T) {
			require.Equal(t, tt.want, anthropicPlatformForCustomEndpoint(tt.channelType))
		})
	}
}
