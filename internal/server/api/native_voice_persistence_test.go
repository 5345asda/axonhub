package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/enttest"
	entrequest "github.com/looplj/axonhub/internal/ent/request"
	entrequestexecution "github.com/looplj/axonhub/internal/ent/requestexecution"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/voice"
)

func TestServeNativeVoiceHTTPPersistsFailedBusinessAttempt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := ent.NewContext(authz.WithTestBypass(context.Background()), enttest.NewEntClient(t, "sqlite3", "file:native_voice_persistence?mode=memory&_fk=0"))
	client := ent.FromContext(ctx)
	t.Cleanup(func() { _ = client.Close() })

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":1004,"status_msg":"invalid voice"}}`))
	}))
	defer upstream.Close()

	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2A)
	require.True(t, ok)
	channelRow, err := client.Channel.Create().
		SetName("native voice persistence").
		SetType(channel.TypeMinimax).
		SetStatus(channel.StatusEnabled).
		SetBaseURL(upstream.URL).
		SetSupportedModels([]string{"speech-2.8-hd"}).
		SetDefaultTestModel("speech-2.8-hd").
		SetCredentials(objects.ChannelCredentials{APIKey: "provider-key"}).
		SetEndpoints([]objects.ChannelEndpoint{{
			APIFormat: protocol.APIFormat,
			Path:      protocol.Path,
			BaseURL:   upstream.URL,
			Transport: protocol.Transport,
		}}).
		Save(ctx)
	require.NoError(t, err)

	requestService := newNativeVoicePersistenceRequestService(t, client)
	channelValue := &biz.Channel{Channel: channelRow}
	selector := voice.NewCandidateSelector(func() []*biz.Channel { return []*biz.Channel{channelValue} }, nil)

	body := `{"model":"speech-2.8-hd","text":"hello"}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/t2a_v2", strings.NewReader(body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Authorization", "Bearer downstream-key")

	serveNativeVoiceHTTP(c, selector, nil, voice.NewNativeHTTPRelay(nil), requestService)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"base_resp":{"status_code":1004,"status_msg":"invalid voice"}}`, w.Body.String())

	requests, err := client.Request.Query().All(ctx)
	require.NoError(t, err)
	require.Len(t, requests, 1)
	require.Equal(t, entrequest.StatusFailed, requests[0].Status)
	require.Equal(t, protocol.APIFormat, requests[0].Format)
	require.Equal(t, "speech-2.8-hd", requests[0].ModelID)
	require.JSONEq(t, body, string(requests[0].RequestBody))
	require.Equal(t, "******", nativeVoiceStoredHeader(t, requests[0].RequestHeaders, "Authorization"))

	executions, err := client.RequestExecution.Query().All(ctx)
	require.NoError(t, err)
	require.Len(t, executions, 1)
	require.Equal(t, entrequestexecution.StatusFailed, executions[0].Status)
	require.NotNil(t, executions[0].MetricsLatencyMs)
	require.Equal(t, channelRow.ID, executions[0].ChannelID)
	require.Equal(t, protocol.APIFormat, executions[0].Format)
	require.Equal(t, "speech-2.8-hd", executions[0].ModelID)
	require.Equal(t, upstream.URL+protocol.Path, executions[0].RequestURL)
	require.NotNil(t, executions[0].ResponseStatusCode)
	require.Equal(t, http.StatusOK, *executions[0].ResponseStatusCode)
	require.Contains(t, executions[0].ErrorMessage, "status_code=1004")
	require.Contains(t, executions[0].ErrorMessage, "invalid voice")
	require.JSONEq(t, body, string(executions[0].RequestBody))
	require.Equal(t, "******", nativeVoiceStoredHeader(t, executions[0].RequestHeaders, "Authorization"))
}

func TestServeNativeVoiceHTTPPersistsTransportFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := ent.NewContext(authz.WithTestBypass(context.Background()), enttest.NewEntClient(t, "sqlite3", "file:native_voice_persistence_transport_failure?mode=memory&_fk=0"))
	client := ent.FromContext(ctx)
	t.Cleanup(func() { _ = client.Close() })

	upstream := httptest.NewServer(http.NotFoundHandler())
	upstreamURL := upstream.URL
	upstream.Close()

	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2A)
	require.True(t, ok)
	channelRow, err := client.Channel.Create().
		SetName("native voice transport failure").
		SetType(channel.TypeMinimax).
		SetStatus(channel.StatusEnabled).
		SetBaseURL(upstreamURL).
		SetSupportedModels([]string{"speech-2.8-hd"}).
		SetDefaultTestModel("speech-2.8-hd").
		SetCredentials(objects.ChannelCredentials{APIKey: "provider-key"}).
		SetEndpoints([]objects.ChannelEndpoint{{
			APIFormat: protocol.APIFormat,
			Path:      protocol.Path,
			BaseURL:   upstreamURL,
			Transport: protocol.Transport,
		}}).
		Save(ctx)
	require.NoError(t, err)

	requestService := newNativeVoicePersistenceRequestService(t, client)
	selector := voice.NewCandidateSelector(func() []*biz.Channel {
		return []*biz.Channel{{Channel: channelRow}}
	}, nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd","text":"hello"}`)).WithContext(ctx)

	serveNativeVoiceHTTP(c, selector, nil, voice.NewNativeHTTPRelay(nil), requestService)

	require.Equal(t, http.StatusBadGateway, w.Code)
	require.Contains(t, w.Body.String(), "native voice upstream request failed")

	requests, err := client.Request.Query().All(ctx)
	require.NoError(t, err)
	require.Len(t, requests, 1)
	require.Equal(t, entrequest.StatusFailed, requests[0].Status)

	executions, err := client.RequestExecution.Query().All(ctx)
	require.NoError(t, err)
	require.Len(t, executions, 1)
	require.Equal(t, entrequestexecution.StatusFailed, executions[0].Status)
	require.Equal(t, channelRow.ID, executions[0].ChannelID)
}

func TestServeNativeVoiceHTTPWritesBeforePersistingExecutions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := ent.NewContext(authz.WithTestBypass(context.Background()), enttest.NewEntClient(t, "sqlite3", "file:native_voice_persistence_first_write?mode=memory&_fk=0"))
	client := ent.FromContext(ctx)
	t.Cleanup(func() { _ = client.Close() })

	persistStarted := make(chan struct{})
	releasePersist := make(chan struct{})
	client.RequestExecution.Use(func(next ent.Mutator) ent.Mutator {
		return ent.MutateFunc(func(ctx context.Context, mutation ent.Mutation) (ent.Value, error) {
			if mutation.Op().Is(ent.OpCreate) {
				close(persistStarted)
				<-releasePersist
			}
			return next.Mutate(ctx, mutation)
		})
	})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":0},"data":{"audio":"00"}}`))
	}))
	defer upstream.Close()

	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2A)
	require.True(t, ok)
	channelRow, err := client.Channel.Create().
		SetName("native voice first write").
		SetType(channel.TypeMinimax).
		SetStatus(channel.StatusEnabled).
		SetBaseURL(upstream.URL).
		SetSupportedModels([]string{"speech-2.8-hd"}).
		SetDefaultTestModel("speech-2.8-hd").
		SetCredentials(objects.ChannelCredentials{APIKey: "provider-key"}).
		SetEndpoints([]objects.ChannelEndpoint{{
			APIFormat: protocol.APIFormat,
			Path:      protocol.Path,
			BaseURL:   upstream.URL,
			Transport: protocol.Transport,
		}}).
		Save(ctx)
	require.NoError(t, err)

	requestService := newNativeVoicePersistenceRequestService(t, client)
	selector := voice.NewCandidateSelector(func() []*biz.Channel {
		return []*biz.Channel{{Channel: channelRow}}
	}, nil)
	w := newNativeVoiceFirstWriteRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd","text":"hello"}`)).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		serveNativeVoiceHTTP(c, selector, nil, voice.NewNativeHTTPRelay(nil), requestService)
		close(done)
	}()

	select {
	case <-w.firstWrite:
	case <-time.After(500 * time.Millisecond):
		close(releasePersist)
		<-done
		require.Fail(t, "execution persistence blocked the first native response byte")
	}

	select {
	case <-persistStarted:
	case <-time.After(500 * time.Millisecond):
		close(releasePersist)
		<-done
		require.Fail(t, "expected native execution persistence")
	}
	close(releasePersist)
	<-done
	require.Equal(t, http.StatusOK, w.Code)
}

func TestNativeVoicePersistedURLStripsCredentialsAndQuery(t *testing.T) {
	stored := nativeVoicePersistedURL("https://user:password@provider.test/v1/t2a_v2?token=provider-secret&model=speech-2.8-hd")

	require.Equal(t, "https://provider.test/v1/t2a_v2", stored)
}

func TestNativeVoicePersistenceMasksLegacyProviderCredentials(t *testing.T) {
	headers := nativeVoiceMaskedHeaders(http.Header{
		"Authorization":          {"Bearer downstream-key"},
		"X-Api-App-Key":          {"app-key"},
		"X-Api-Access-Key":       {"access-key"},
		"X-Api-Resource-Id":      {"resource-id"},
		"X-Api-Connect-Id":       {"connect-id"},
		"WWW-Authenticate":       {`Bearer realm="provider"`},
		"X-Non-Sensitive-Header": {"keep"},
		"x-api-app-key":          {"lowercase-app-key"},
	})

	for _, key := range []string{"Authorization", "X-Api-App-Key", "X-Api-Access-Key", "X-Api-Resource-Id", "X-Api-Connect-Id", "WWW-Authenticate"} {
		require.Equal(t, "******", headers.Get(key), key)
	}
	require.Len(t, headers.Values("X-Api-App-Key"), 1)
	require.Equal(t, "keep", headers.Get("X-Non-Sensitive-Header"))
}

func TestNativeVoicePersistedErrorStripsURLCredentialsAndQuery(t *testing.T) {
	message := nativeVoicePersistedError(errors.New("dial https://user:password@provider.test/v1/t2a_v2?signature=provider-secret failed"))

	require.Equal(t, "dial https://provider.test/v1/t2a_v2 failed", message)
}

func TestNativeVoiceRecorderCompletesWithoutFinalRelayResult(t *testing.T) {
	ctx := ent.NewContext(authz.WithTestBypass(context.Background()), enttest.NewEntClient(t, "sqlite3", "file:native_voice_persistence_missing_result?mode=memory&_fk=0"))
	client := ent.FromContext(ctx)
	t.Cleanup(func() { _ = client.Close() })

	requestService := newNativeVoicePersistenceRequestService(t, client)
	request := httptest.NewRequest(http.MethodPost, "/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`)).WithContext(ctx)
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2A)
	require.True(t, ok)
	recorder := newNativeVoiceRequestRecorder(requestService, protocol, "speech-2.8-hd", false, request, []byte(`{"model":"speech-2.8-hd"}`), time.Now())
	require.NotNil(t, recorder)

	recorder.finish(ctx, nil)

	stored, err := client.Request.Query().Only(ctx)
	require.NoError(t, err)
	require.Equal(t, entrequest.StatusCompleted, stored.Status)
}

func newNativeVoicePersistenceRequestService(t *testing.T, client *ent.Client) *biz.RequestService {
	t.Helper()

	cacheConfig := xcache.Config{Mode: xcache.ModeMemory}
	systemService := biz.NewSystemService(biz.SystemServiceParams{CacheConfig: cacheConfig, Ent: client})
	require.NoError(t, systemService.SetStoragePolicy(authz.WithTestBypass(ent.NewContext(context.Background(), client)), &biz.StoragePolicy{
		StoreRequestBody:  true,
		StoreResponseBody: true,
	}))
	dataStorageService := &biz.DataStorageService{
		AbstractService: &biz.AbstractService{},
		SystemService:   systemService,
		Cache:           xcache.NewFromConfig[ent.DataStorage](cacheConfig),
	}
	channelService := biz.NewChannelServiceForTest(client)
	usageLogService := biz.NewUsageLogService(client, systemService, channelService)

	return biz.NewRequestService(client, cacheConfig, systemService, usageLogService, dataStorageService, biz.NewLiveStreamRegistry())
}

func nativeVoiceStoredHeader(t *testing.T, raw []byte, key string) string {
	t.Helper()
	var headers http.Header
	require.NoError(t, json.Unmarshal(raw, &headers))
	return headers.Get(key)
}

type nativeVoiceFirstWriteRecorder struct {
	*httptest.ResponseRecorder
	firstWrite chan struct{}
	once       sync.Once
}

func newNativeVoiceFirstWriteRecorder() *nativeVoiceFirstWriteRecorder {
	return &nativeVoiceFirstWriteRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		firstWrite:       make(chan struct{}),
	}
}

func (w *nativeVoiceFirstWriteRecorder) WriteHeader(code int) {
	w.once.Do(func() { close(w.firstWrite) })
	w.ResponseRecorder.WriteHeader(code)
}

func (w *nativeVoiceFirstWriteRecorder) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.firstWrite) })
	return w.ResponseRecorder.Write(body)
}

func (*nativeVoiceFirstWriteRecorder) Flush() {}
