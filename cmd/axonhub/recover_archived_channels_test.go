package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/orchestrator"
)

func TestParseRecoverArchivedChannelsArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []string
		want     recoverArchivedChannelsOptions
		wantErr  bool
		errMatch string
	}{
		{
			name: "default options",
			args: nil,
			want: recoverArchivedChannelsOptions{
				Concurrency: defaultRecoverArchivedChannelsConcurrency,
			},
		},
		{
			name: "dry run with model override and remote auth",
			args: []string{
				"--dry-run",
				"--model", "gemini-2.5-pro",
				"--endpoint", "https://example.com/",
				"--token", "test-token",
				"--concurrency", "6",
			},
			want: recoverArchivedChannelsOptions{
				DryRun:      true,
				ModelID:     lo.ToPtr("gemini-2.5-pro"),
				Endpoint:    "https://example.com/",
				Token:       "test-token",
				Concurrency: 6,
			},
		},
		{
			name: "delete page errors with custom delete reason",
			args: []string{"--delete-page-errors", "--delete-reason", "Forbidden", "--delete-reason", "This app isn't live yet"},
			want: recoverArchivedChannelsOptions{
				DeletePageErrors: true,
				Concurrency:      defaultRecoverArchivedChannelsConcurrency,
				DeleteReasons: []string{
					"Forbidden",
					"This app isn't live yet",
				},
			},
		},
		{
			name:     "unexpected positional arg",
			args:     []string{"extra"},
			wantErr:  true,
			errMatch: "unexpected arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseRecoverArchivedChannelsArgs(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMatch)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want.DryRun, got.DryRun)
			require.Equal(t, lo.FromPtr(tt.want.ModelID), lo.FromPtr(got.ModelID))
			require.Equal(t, tt.want.DeletePageErrors, got.DeletePageErrors)
			require.Equal(t, tt.want.DeleteReasons, got.DeleteReasons)
			require.Equal(t, tt.want.Endpoint, got.Endpoint)
			require.Equal(t, tt.want.Token, got.Token)
			require.Equal(t, tt.want.Concurrency, got.Concurrency)
		})
	}
}

func TestResolveRecoverArchivedChannelsRuntime(t *testing.T) {
	t.Parallel()

	t.Run("uses explicit endpoint and token", func(t *testing.T) {
		t.Parallel()

		runtime, err := resolveRecoverArchivedChannelsRuntime(
			recoverArchivedChannelsOptions{
				Endpoint: "https://example.com/admin/graphql",
				Token:    "flag-token",
			},
			func(string) string { return "" },
		)
		require.NoError(t, err)
		require.Equal(t, "https://example.com/admin/graphql", runtime.Endpoint)
		require.Equal(t, "flag-token", runtime.Token)
	})

	t.Run("normalizes base url and falls back to env token", func(t *testing.T) {
		t.Parallel()

		runtime, err := resolveRecoverArchivedChannelsRuntime(
			recoverArchivedChannelsOptions{
				Endpoint: "https://example.com/",
			},
			func(key string) string {
				if key == "AXONHUB_CHANNEL_SYNC_TOKEN" {
					return "env-token"
				}

				return ""
			},
		)
		require.NoError(t, err)
		require.Equal(t, "https://example.com/admin/graphql", runtime.Endpoint)
		require.Equal(t, "env-token", runtime.Token)
	})

	t.Run("uses env endpoint fallback", func(t *testing.T) {
		t.Parallel()

		runtime, err := resolveRecoverArchivedChannelsRuntime(
			recoverArchivedChannelsOptions{},
			func(key string) string {
				switch key {
				case "AXONHUB_ADMIN_GRAPHQL_ENDPOINT":
					return "https://example.com/root"
				case "AXONHUB_CHANNEL_SYNC_TOKEN":
					return "env-token"
				default:
					return ""
				}
			},
		)
		require.NoError(t, err)
		require.Equal(t, "https://example.com/root/admin/graphql", runtime.Endpoint)
		require.Equal(t, "env-token", runtime.Token)
	})

	t.Run("requires endpoint", func(t *testing.T) {
		t.Parallel()

		_, err := resolveRecoverArchivedChannelsRuntime(
			recoverArchivedChannelsOptions{Token: "token"},
			func(string) string { return "" },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "endpoint is required")
	})

	t.Run("requires token", func(t *testing.T) {
		t.Parallel()

		_, err := resolveRecoverArchivedChannelsRuntime(
			recoverArchivedChannelsOptions{Endpoint: "https://example.com"},
			func(string) string { return "" },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "token is required")
	})
}

func TestFormatRecoverArchivedChannelsResult(t *testing.T) {
	t.Parallel()

	output := formatRecoverArchivedChannelsResult(&orchestrator.ArchivedChannelRecoveryResult{
		Total:            2,
		Tested:           2,
		Succeeded:        1,
		Failed:           1,
		Recoverable:      1,
		Recovered:        1,
		DeleteCandidates: 1,
		Deleted:          1,
		FailureReasonCounts: map[string]int{
			"This app isn't live yet": 1,
		},
		DeletedReasonCounts: map[string]int{
			"This app isn't live yet": 1,
		},
		Items: []*orchestrator.ArchivedChannelRecoveryItem{
			{
				ChannelID: objects.GUID{Type: "Channel", ID: 101},
				Name:      "Archived A",
				ModelID:   "gpt-4o-mini",
				Success:   true,
				Recovered: true,
				Latency:   0.12,
				Message:   lo.ToPtr("ok"),
			},
			{
				ChannelID:     objects.GUID{Type: "Channel", ID: 102},
				Name:          "Archived B",
				ModelID:       "claude-3-5-sonnet",
				Success:       false,
				Recovered:     false,
				DeleteMatched: true,
				Deleted:       true,
				Latency:       0.34,
				Error:         lo.ToPtr("<!DOCTYPE html><html><head><title>This app isn&#39;t live yet</title></head></html>"),
				FailureReason: lo.ToPtr("This app isn't live yet"),
			},
		},
	})

	require.Contains(t, output, "Total archived channels: 2")
	require.Contains(t, output, "Succeeded: 1")
	require.Contains(t, output, "Recovered: 1")
	require.Contains(t, output, "Delete candidates: 1")
	require.Contains(t, output, "Deleted: 1")
	require.Contains(t, output, "Failure reasons:")
	require.Contains(t, output, "  - 1 x This app isn't live yet")
	require.Contains(t, output, "Deleted failure reasons:")
	require.Contains(t, output, "[RECOVERED] id=101 name=\"Archived A\" model=\"gpt-4o-mini\"")
	require.Contains(t, output, "[DELETED] id=102 name=\"Archived B\" model=\"claude-3-5-sonnet\"")
	require.Contains(t, output, "failure_reason=\"This app isn't live yet\"")
}

func TestAxonHubAdminGraphQLClient_ListArchivedChannels(t *testing.T) {
	t.Parallel()

	reqLog := &graphQLRequestLog{}
	server := newGraphQLTestServer(t, reqLog, func(w http.ResponseWriter, request graphQLTestRequest) {
		require.Equal(t, "RecoverArchivedChannelsList", request.OperationName)
		require.True(t, request.QueryContains("allChannelSummarys"))
		require.Equal(t, true, request.Variables["includeArchived"])

		writeGraphQLJSON(t, w, map[string]any{
			"data": map[string]any{
				"allChannelSummarys": []map[string]any{
					{"id": "gid://axonhub/Channel/10", "name": "Archived A", "status": "archived", "defaultTestModel": "gpt-4o-mini"},
					{"id": "gid://axonhub/Channel/11", "name": "Enabled B", "status": "enabled", "defaultTestModel": "gpt-4.1"},
					{"id": "gid://axonhub/Channel/12", "name": "Archived C", "status": "ARCHIVED", "defaultTestModel": "claude-3-5-sonnet"},
				},
			},
		})
	})
	defer server.Close()

	client := newAxonHubAdminGraphQLClient(server.URL, "test-token", server.Client())

	channels, err := client.ListArchivedChannels(context.Background())
	require.NoError(t, err)
	require.Len(t, channels, 2)
	require.Equal(t, objects.GUID{Type: "Channel", ID: 10}, channels[0].ID)
	require.Equal(t, "Archived A", channels[0].Name)
	require.Equal(t, "gpt-4o-mini", channels[0].DefaultTestModel)
	require.Equal(t, objects.GUID{Type: "Channel", ID: 12}, channels[1].ID)
	require.Equal(t, 1, reqLog.Count())
}

func TestAxonHubAdminGraphQLClient_TestChannel(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, nil, func(w http.ResponseWriter, request graphQLTestRequest) {
		require.Equal(t, "RecoverArchivedChannelsTest", request.OperationName)
		require.True(t, request.QueryContains("testChannel"))

		input := requireMapValue(t, request.Variables, "input")
		require.Equal(t, "gid://axonhub/Channel/42", input["channelID"])
		require.Equal(t, "gpt-4o-mini", input["modelID"])

		writeGraphQLJSON(t, w, map[string]any{
			"data": map[string]any{
				"testChannel": map[string]any{
					"latency": 0.42,
					"success": false,
					"error":   "Forbidden",
					"message": "",
				},
			},
		})
	})
	defer server.Close()

	client := newAxonHubAdminGraphQLClient(server.URL, "test-token", server.Client())

	result, err := client.TestChannel(context.Background(), objects.GUID{Type: "Channel", ID: 42}, lo.ToPtr("gpt-4o-mini"), nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.Success)
	require.Equal(t, 0.42, result.Latency)
	require.Equal(t, "Forbidden", lo.FromPtr(result.Error))
}

func TestAxonHubAdminGraphQLClient_BulkMutationsUseGUIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		run           func(client *axonHubAdminGraphQLClient) error
		operationName string
		field         string
	}{
		{
			name: "recover",
			run: func(client *axonHubAdminGraphQLClient) error {
				return client.BulkRecoverChannels(context.Background(), []int{7})
			},
			operationName: "RecoverArchivedChannelsRecover",
			field:         "bulkRecoverChannels",
		},
		{
			name: "delete",
			run: func(client *axonHubAdminGraphQLClient) error {
				return client.BulkDeleteChannels(context.Background(), []int{8})
			},
			operationName: "RecoverArchivedChannelsDelete",
			field:         "bulkDeleteChannels",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, nil, func(w http.ResponseWriter, request graphQLTestRequest) {
				require.Equal(t, tt.operationName, request.OperationName)
				require.True(t, request.QueryContains(tt.field))
				require.Equal(t, []any{channelGUIDString(map[string]int{"recover": 7, "delete": 8}[tt.name])}, request.Variables["ids"])

				writeGraphQLJSON(t, w, map[string]any{
					"data": map[string]any{
						tt.field: true,
					},
				})
			})
			defer server.Close()

			client := newAxonHubAdminGraphQLClient(server.URL, "test-token", server.Client())
			require.NoError(t, tt.run(client))
		})
	}
}

func TestRunRecoverArchivedChannelsCommand_UsesRemoteGraphQL(t *testing.T) {
	var operationNames []string

	server := newGraphQLTestServer(t, nil, func(w http.ResponseWriter, request graphQLTestRequest) {
		operationNames = append(operationNames, request.OperationName)

		switch request.OperationName {
		case "RecoverArchivedChannelsList":
			writeGraphQLJSON(t, w, map[string]any{
				"data": map[string]any{
					"allChannelSummarys": []map[string]any{
						{"id": "gid://axonhub/Channel/21", "name": "Archived Remote", "status": "archived", "defaultTestModel": "gpt-4o-mini"},
						{"id": "gid://axonhub/Channel/22", "name": "Enabled Remote", "status": "enabled", "defaultTestModel": "gpt-4o-mini"},
					},
				},
			})
		case "RecoverArchivedChannelsTest":
			input := requireMapValue(t, request.Variables, "input")
			require.Equal(t, "gid://axonhub/Channel/21", input["channelID"])

			writeGraphQLJSON(t, w, map[string]any{
				"data": map[string]any{
					"testChannel": map[string]any{
						"latency": 0.15,
						"success": true,
						"message": "ok",
						"error":   nil,
					},
				},
			})
		default:
			t.Fatalf("unexpected operation: %s", request.OperationName)
		}
	})
	defer server.Close()

	t.Setenv("AXONHUB_CHANNEL_SYNC_TOKEN", "test-token")

	var stdout bytes.Buffer
	err := runRecoverArchivedChannelsCommand(&stdout, recoverArchivedChannelsOptions{
		DryRun:   true,
		Endpoint: server.URL,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"RecoverArchivedChannelsList", "RecoverArchivedChannelsTest"}, operationNames)

	output := stdout.String()
	require.Contains(t, output, "Total archived channels: 1")
	require.Contains(t, output, "Dry run: true")
	require.Contains(t, output, "[WOULD_RECOVER] id=21 name=\"Archived Remote\" model=\"gpt-4o-mini\"")
	require.NotContains(t, output, "system not initialized")
}

type graphQLRequestLog struct {
	requests []graphQLTestRequest
}

func (l *graphQLRequestLog) Append(request graphQLTestRequest) {
	l.requests = append(l.requests, request)
}

func (l *graphQLRequestLog) Count() int {
	return len(l.requests)
}

type graphQLTestRequest struct {
	OperationName string         `json:"operationName"`
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
}

func (r graphQLTestRequest) QueryContains(value string) bool {
	return strings.Contains(r.Query, value)
}

func newGraphQLTestServer(
	t *testing.T,
	reqLog *graphQLRequestLog,
	handler func(w http.ResponseWriter, request graphQLTestRequest),
) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var request graphQLTestRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		if reqLog != nil {
			reqLog.Append(request)
		}

		handler(w, request)
	}))
}

func writeGraphQLJSON(t *testing.T, w http.ResponseWriter, body map[string]any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(body))
}

func requireMapValue(t *testing.T, values map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := values[key]
	require.True(t, ok)

	result, ok := value.(map[string]any)
	require.True(t, ok)

	return result
}
