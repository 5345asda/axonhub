package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm/httpclient"
)

const (
	recoverArchivedChannelsListOperation    = "RecoverArchivedChannelsList"
	recoverArchivedChannelsTestOperation    = "RecoverArchivedChannelsTest"
	recoverArchivedChannelsRecoverOperation = "RecoverArchivedChannelsRecover"
	recoverArchivedChannelsDeleteOperation  = "RecoverArchivedChannelsDelete"
)

const (
	recoverArchivedChannelsListQuery = `
query RecoverArchivedChannelsList($includeArchived: Boolean) {
  allChannelSummarys(includeArchived: $includeArchived) {
    id
    name
    status
    defaultTestModel
  }
}
`
	recoverArchivedChannelsTestQuery = `
mutation RecoverArchivedChannelsTest($input: TestChannelInput!) {
  testChannel(input: $input) {
    latency
    success
    error
    message
  }
}
`
	recoverArchivedChannelsRecoverMutation = `
mutation RecoverArchivedChannelsRecover($ids: [ID!]!) {
  bulkRecoverChannels(ids: $ids)
}
`
	recoverArchivedChannelsDeleteMutation = `
mutation RecoverArchivedChannelsDelete($ids: [ID!]!) {
  bulkDeleteChannels(ids: $ids)
}
`
)

type recoverArchivedChannelsRuntime struct {
	Endpoint string
	Token    string
}

func resolveRecoverArchivedChannelsRuntime(
	options recoverArchivedChannelsOptions,
	getenv func(string) string,
) (recoverArchivedChannelsRuntime, error) {
	endpoint := firstRecoverArchivedChannelsValue(
		options.Endpoint,
		getenv("AXONHUB_ADMIN_GRAPHQL_ENDPOINT"),
		getenv("AXONHUB_CHANNEL_SYNC_ENDPOINT"),
		getenv("AXONHUB_CHANNEL_SYNC_URL"),
	)
	if endpoint == "" {
		return recoverArchivedChannelsRuntime{}, fmt.Errorf(
			"admin graphql endpoint is required via --endpoint or AXONHUB_ADMIN_GRAPHQL_ENDPOINT/AXONHUB_CHANNEL_SYNC_ENDPOINT",
		)
	}

	normalizedEndpoint, err := normalizeRecoverArchivedChannelsEndpoint(endpoint)
	if err != nil {
		return recoverArchivedChannelsRuntime{}, err
	}

	token := firstRecoverArchivedChannelsValue(
		options.Token,
		getenv("AXONHUB_CHANNEL_SYNC_TOKEN"),
		getenv("AXONHUB_ADMIN_GRAPHQL_TOKEN"),
	)
	if token == "" {
		return recoverArchivedChannelsRuntime{}, fmt.Errorf(
			"channel sync token is required via --token or AXONHUB_CHANNEL_SYNC_TOKEN",
		)
	}

	return recoverArchivedChannelsRuntime{
		Endpoint: normalizedEndpoint,
		Token:    token,
	}, nil
}

func firstRecoverArchivedChannelsValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}

	return ""
}

func normalizeRecoverArchivedChannelsEndpoint(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("admin graphql endpoint is empty")
	}

	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("invalid admin graphql endpoint %q: %w", raw, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid admin graphql endpoint %q", raw)
	}

	path := strings.TrimRight(parsed.Path, "/")
	if strings.HasSuffix(path, "/graphql") {
		parsed.Path = path
		return parsed.String(), nil
	}

	if path == "" {
		parsed.Path = "/admin/graphql"
		return parsed.String(), nil
	}

	parsed.Path = path + "/admin/graphql"
	return parsed.String(), nil
}

type axonHubAdminGraphQLClient struct {
	endpoint   string
	token      string
	httpClient *http.Client
}

func newAxonHubAdminGraphQLClient(endpoint string, token string, client *http.Client) *axonHubAdminGraphQLClient {
	if client == nil {
		client = &http.Client{
			Timeout: 2 * time.Minute,
		}
	}

	return &axonHubAdminGraphQLClient{
		endpoint:   endpoint,
		token:      token,
		httpClient: client,
	}
}

func (c *axonHubAdminGraphQLClient) ListArchivedChannels(ctx context.Context) ([]orchestrator.ArchivedChannelCandidate, error) {
	var data struct {
		AllChannelSummarys []struct {
			ID               string `json:"id"`
			Name             string `json:"name"`
			Status           string `json:"status"`
			DefaultTestModel string `json:"defaultTestModel"`
		} `json:"allChannelSummarys"`
	}

	if err := c.execute(
		ctx,
		recoverArchivedChannelsListOperation,
		recoverArchivedChannelsListQuery,
		map[string]any{"includeArchived": true},
		&data,
	); err != nil {
		return nil, err
	}

	result := make([]orchestrator.ArchivedChannelCandidate, 0, len(data.AllChannelSummarys))
	for _, channel := range data.AllChannelSummarys {
		if !strings.EqualFold(channel.Status, "archived") {
			continue
		}

		guid, err := objects.ParseGUID(channel.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse channel id %q: %w", channel.ID, err)
		}

		result = append(result, orchestrator.ArchivedChannelCandidate{
			ID:               guid,
			Name:             channel.Name,
			DefaultTestModel: channel.DefaultTestModel,
		})
	}

	return result, nil
}

func (c *axonHubAdminGraphQLClient) TestChannel(
	ctx context.Context,
	channelID objects.GUID,
	modelID *string,
	proxy *httpclient.ProxyConfig,
) (*orchestrator.TestChannelResult, error) {
	input := map[string]any{
		"channelID": channelGUIDString(channelID.ID),
	}
	if value := lo.FromPtr(modelID); value != "" {
		input["modelID"] = value
	}
	if proxy != nil {
		input["proxy"] = encodeRecoverArchivedChannelsProxy(proxy)
	}

	var data struct {
		TestChannel struct {
			Latency float64 `json:"latency"`
			Success bool    `json:"success"`
			Error   *string `json:"error"`
			Message *string `json:"message"`
		} `json:"testChannel"`
	}

	if err := c.execute(
		ctx,
		recoverArchivedChannelsTestOperation,
		recoverArchivedChannelsTestQuery,
		map[string]any{"input": input},
		&data,
	); err != nil {
		return nil, err
	}

	return &orchestrator.TestChannelResult{
		Latency: data.TestChannel.Latency,
		Success: data.TestChannel.Success,
		Error:   data.TestChannel.Error,
		Message: data.TestChannel.Message,
	}, nil
}

func (c *axonHubAdminGraphQLClient) BulkRecoverChannels(ctx context.Context, ids []int) error {
	var data struct {
		BulkRecoverChannels bool `json:"bulkRecoverChannels"`
	}

	if err := c.execute(
		ctx,
		recoverArchivedChannelsRecoverOperation,
		recoverArchivedChannelsRecoverMutation,
		map[string]any{"ids": channelGUIDStrings(ids)},
		&data,
	); err != nil {
		return err
	}
	if !data.BulkRecoverChannels {
		return fmt.Errorf("bulkRecoverChannels returned false")
	}

	return nil
}

func (c *axonHubAdminGraphQLClient) BulkDeleteChannels(ctx context.Context, ids []int) error {
	var data struct {
		BulkDeleteChannels bool `json:"bulkDeleteChannels"`
	}

	if err := c.execute(
		ctx,
		recoverArchivedChannelsDeleteOperation,
		recoverArchivedChannelsDeleteMutation,
		map[string]any{"ids": channelGUIDStrings(ids)},
		&data,
	); err != nil {
		return err
	}
	if !data.BulkDeleteChannels {
		return fmt.Errorf("bulkDeleteChannels returned false")
	}

	return nil
}

func (c *axonHubAdminGraphQLClient) execute(
	ctx context.Context,
	operationName string,
	query string,
	variables map[string]any,
	target any,
) error {
	payload, err := json.Marshal(map[string]any{
		"operationName": operationName,
		"query":         query,
		"variables":     variables,
	})
	if err != nil {
		return fmt.Errorf("failed to encode %s request: %w", operationName, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create %s request: %w", operationName, err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s request failed: %w", operationName, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("failed to read %s response: %w", operationName, err)
	}

	var graphQLResponse struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &graphQLResponse); err != nil {
		return fmt.Errorf(
			"%s returned non-JSON response (%s): %s",
			operationName,
			resp.Status,
			truncateRecoverArchivedChannelsDetail(string(body), 200),
		)
	}

	if len(graphQLResponse.Errors) > 0 {
		messages := lo.FilterMap(graphQLResponse.Errors, func(item struct {
			Message string `json:"message"`
		}, _ int) (string, bool) {
			message := strings.TrimSpace(item.Message)
			return message, message != ""
		})

		errText := strings.Join(messages, "; ")
		if errText == "" {
			errText = truncateRecoverArchivedChannelsDetail(string(body), 200)
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("%s failed with status %s: %s", operationName, resp.Status, errText)
		}

		return fmt.Errorf("%s failed: %s", operationName, errText)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf(
			"%s failed with status %s: %s",
			operationName,
			resp.Status,
			truncateRecoverArchivedChannelsDetail(string(body), 200),
		)
	}

	if target == nil {
		return nil
	}

	if len(graphQLResponse.Data) == 0 || string(graphQLResponse.Data) == "null" {
		return fmt.Errorf("%s returned empty data", operationName)
	}

	if err := json.Unmarshal(graphQLResponse.Data, target); err != nil {
		return fmt.Errorf("failed to decode %s response data: %w", operationName, err)
	}

	return nil
}

func channelGUIDStrings(ids []int) []string {
	return lo.Map(ids, func(id int, _ int) string {
		return channelGUIDString(id)
	})
}

func channelGUIDString(id int) string {
	return fmt.Sprintf("gid://axonhub/Channel/%d", id)
}

func encodeRecoverArchivedChannelsProxy(proxy *httpclient.ProxyConfig) map[string]any {
	if proxy == nil {
		return nil
	}

	result := map[string]any{
		"type": strings.ToUpper(string(proxy.Type)),
	}
	if proxy.URL != "" {
		result["url"] = proxy.URL
	}
	if proxy.Username != "" {
		result["username"] = proxy.Username
	}
	if proxy.Password != "" {
		result["password"] = proxy.Password
	}

	return result
}
