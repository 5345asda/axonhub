package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeArchivedChannelFailureReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "extracts replit not live title from html",
			raw:  "<!DOCTYPE html><html><head><title>This app isn&#39;t live yet</title></head><body><h1>This app isn&#39;t live yet</h1></body></html>",
			want: "This app isn't live yet",
		},
		{
			name: "extracts run this app title from html",
			raw:  "<!DOCTYPE html><html><head><title>Run this app to see the results here.</title></head><body></body></html>",
			want: "Run this app to see the results here.",
		},
		{
			name: "extracts cannot post route from html body",
			raw:  "<!DOCTYPE html><html><body><pre>Cannot POST /api/anthropic/v1/messages</pre></body></html>",
			want: "Cannot POST /api/anthropic/v1/messages",
		},
		{
			name: "extracts code and message from json payload",
			raw:  `{"error":{"code":"FREE_TIER_BUDGET_EXCEEDED","message":"Provider account unavailable."}}`,
			want: "FREE_TIER_BUDGET_EXCEEDED - Provider account unavailable.",
		},
		{
			name: "keeps plain text error",
			raw:  "Forbidden",
			want: "Forbidden",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, NormalizeArchivedChannelFailureReason(tt.raw))
		})
	}
}
