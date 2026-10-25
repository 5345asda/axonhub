package backup

import (
	"encoding/json"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
)

func TestBackupService_RestoreRejectsInvalidAPIKeyStrategyAndRollsBack(t *testing.T) {
	client, service, ctx := setupBackupTest(t)
	defer client.Close()

	backupData := BackupData{
		Version: BackupVersion,
		Channels: []*BackupChannel{
			{
				Channel: ent.Channel{
					Type:            channel.TypeOpenai,
					Name:            "Valid Channel Before Invalid Strategy",
					BaseURL:         "https://api.example.com/v1",
					Status:          channel.StatusEnabled,
					SupportedModels: []string{"gpt-test"},
				},
				Credentials: objects.ChannelCredentials{APIKey: "valid-test-key"},
			},
			{
				Channel: ent.Channel{
					Type:            channel.TypeOpenai,
					Name:            "Invalid API Key Strategy",
					BaseURL:         "https://api.example.com/v1",
					Status:          channel.StatusEnabled,
					SupportedModels: []string{"gpt-test"},
					Settings: &objects.ChannelSettings{
						APIKeyStrategy: lo.ToPtr("not-a-strategy"),
					},
				},
				Credentials: objects.ChannelCredentials{APIKey: "invalid-test-key"},
			},
		},
	}

	data, err := json.Marshal(backupData)
	require.NoError(t, err)

	err = service.Restore(ctx, data, RestoreOptions{
		IncludeChannels:         true,
		ChannelConflictStrategy: ConflictStrategyOverwrite,
	})
	require.ErrorContains(t, err, "invalid API key strategy")

	count, err := client.Channel.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count, "the prior channel write must be rolled back")
}

func TestBackupService_RestoreSkipsCredentiallessChannelBeforeStrategyValidation(t *testing.T) {
	client, service, ctx := setupBackupTest(t)
	defer client.Close()

	backupData := BackupData{
		Version: BackupVersion,
		Channels: []*BackupChannel{
			{
				Channel: ent.Channel{
					Type:            channel.TypeOpenai,
					Name:            "Credentialless Historical Channel",
					BaseURL:         "https://api.example.com/v1",
					Status:          channel.StatusEnabled,
					SupportedModels: []string{"gpt-test"},
					Settings: &objects.ChannelSettings{
						APIKeyStrategy: lo.ToPtr("retired-strategy"),
					},
				},
			},
		},
	}

	data, err := json.Marshal(backupData)
	require.NoError(t, err)

	err = service.Restore(ctx, data, RestoreOptions{
		IncludeChannels:         true,
		ChannelConflictStrategy: ConflictStrategyOverwrite,
	})
	require.NoError(t, err)

	count, err := client.Channel.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count, "credentialless channels are skipped before strategy validation")
}
