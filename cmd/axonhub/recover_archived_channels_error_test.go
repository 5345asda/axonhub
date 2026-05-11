package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivedChannelDeleteReasons(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{
		"This app isn't live yet",
		"Run this app to see the results here.",
		"Cannot POST /api/anthropic/v1/messages",
	}, archivedChannelDeletePageReasons())

	require.Equal(t, []string{
		"This app isn't live yet",
		"Run this app to see the results here.",
		"Cannot POST /api/anthropic/v1/messages",
		"FREE_TIER_BUDGET_EXCEEDED - Provider account unavailable.",
	}, resolveArchivedChannelDeleteReasons(recoverArchivedChannelsOptions{
		DeletePageErrors: true,
		DeleteReasons: []string{
			`{"error":{"code":"FREE_TIER_BUDGET_EXCEEDED","message":"Provider account unavailable."}}`,
		},
	}))

	require.Equal(t, archivedChannelDeletePageReasons(), resolveArchivedChannelDeleteReasons(recoverArchivedChannelsOptions{
		DeletePageErrors: true,
	}))

	require.Equal(t, []string{"Forbidden"}, resolveArchivedChannelDeleteReasons(recoverArchivedChannelsOptions{
		DeleteReasons: []string{"Forbidden"},
	}))

	require.Equal(t, []string{}, resolveArchivedChannelDeleteReasons(recoverArchivedChannelsOptions{}))
}
