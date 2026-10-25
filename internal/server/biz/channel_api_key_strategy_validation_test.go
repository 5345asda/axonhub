package biz

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
)

func TestValidateAPIKeyStrategy(t *testing.T) {
	tests := []struct {
		name     string
		settings *objects.ChannelSettings
		wantErr  bool
	}{
		{
			name:     "accepts nil settings",
			settings: nil,
		},
		{
			name:     "accepts an unset strategy",
			settings: &objects.ChannelSettings{},
		},
		{
			name:     "accepts empty strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr("")},
		},
		{
			name:     "accepts sticky strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategySticky)},
		},
		{
			name:     "accepts random strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategyRandom)},
		},
		{
			name:     "accepts round robin strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategyRoundRobin)},
		},
		{
			name:     "accepts successful round robin strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategyRoundRobinSuccess)},
		},
		{
			name:     "accepts priority strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategyPriority)},
		},
		{
			name:     "accepts fixed strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategyFixed)},
		},
		{
			name:     "rejects unknown strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr("not-a-strategy")},
			wantErr:  true,
		},
		{
			name:     "rejects whitespace strategy",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr(" round_robin")},
			wantErr:  true,
		},
		{
			name:     "rejects case variant",
			settings: &objects.ChannelSettings{APIKeyStrategy: lo.ToPtr("ROUND_ROBIN")},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAPIKeyStrategy(tt.settings)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestChannelService_CreateChannelRejectsInvalidAPIKeyStrategy(t *testing.T) {
	svc, client := setupTestChannelService(t)
	defer client.Close()

	ctx := channelAutoPriceTestContext(client)
	created, err := svc.CreateChannel(ctx, ent.CreateChannelInput{
		Type:            channel.TypeOpenai,
		Name:            "Invalid API Key Strategy Create",
		BaseURL:         lo.ToPtr("https://api.openai.com/v1"),
		Credentials:     objects.ChannelCredentials{APIKey: "test-api-key"},
		SupportedModels: []string{"gpt-test"},
		Settings: &objects.ChannelSettings{
			APIKeyStrategy: lo.ToPtr("not-a-strategy"),
		},
	})
	require.ErrorContains(t, err, "invalid API key strategy")
	require.Nil(t, created)

	exists, err := client.Channel.Query().Where(channel.Name("Invalid API Key Strategy Create")).Exist(ctx)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestChannelService_UpdateChannelSettingsRejectsInvalidAPIKeyStrategy(t *testing.T) {
	svc, client := setupTestChannelService(t)
	defer client.Close()

	ctx := channelAutoPriceTestContext(client)
	created, err := svc.CreateChannel(ctx, ent.CreateChannelInput{
		Type:            channel.TypeOpenai,
		Name:            "Invalid API Key Strategy Patch",
		BaseURL:         lo.ToPtr("https://api.openai.com/v1"),
		Credentials:     objects.ChannelCredentials{APIKey: "test-api-key"},
		SupportedModels: []string{"gpt-test"},
		Settings: &objects.ChannelSettings{
			APIKeyStrategy: lo.ToPtr(objects.APIKeyStrategyPriority),
		},
	})
	require.NoError(t, err)

	updated, err := svc.UpdateChannelSettings(ctx, created.ID, &UpdateChannelSettingsInput{
		APIKeyStrategy: lo.ToPtr("not-a-strategy"),
	})
	require.ErrorContains(t, err, "invalid API key strategy")
	require.Nil(t, updated)

	persisted, err := client.Channel.Get(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted.Settings)
	require.Equal(t, objects.APIKeyStrategyPriority, *persisted.Settings.APIKeyStrategy)
}
