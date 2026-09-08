package voice

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm/httpclient"
)

func TestNativeHTTPRelayStripsDownstreamCredentialsAndPreservesPayload(t *testing.T) {
	body := []byte(`{"model":"speech-2.8-hd","text":"hello"}`)
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/t2a_v2", r.URL.Path)
		require.Equal(t, "1", r.URL.Query().Get("trace"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Equal(t, "Bearer provider-key", r.Header.Get("Authorization"))
		require.Equal(t, "keep", r.Header.Get("X-Trace"))
		require.Empty(t, r.Header.Get("X-Api-Key"))
		require.Empty(t, r.Header.Get("X-Api-App-Key"))
		require.Empty(t, r.Header.Get("X-Api-Resource-Id"))
		require.Empty(t, r.Header.Get("Cookie"))
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "kept")
		w.Header().Set("Authorization", "upstream-secret")
		w.Header().Set("Set-Cookie", "provider-secret=1")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(objects.NativeVoiceAPIFormatMiniMaxT2A)
	require.True(t, ok)
	target := NativeRelayTarget{
		Channel: &biz.Channel{Channel: &ent.Channel{
			ID:     1,
			Name:   "minimax",
			Type:   channel.TypeMinimax,
			Status: channel.StatusEnabled,
			Credentials: objects.ChannelCredentials{
				APIKey: "provider-key",
			},
		}},
		Endpoint: objects.ChannelEndpoint{
			APIFormat: protocol.APIFormat,
			Path:      protocol.Path,
			BaseURL:   server.URL,
		},
		Protocol: protocol,
	}

	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2?trace=1", bytes.NewReader(body))
	inbound.Header.Set("Content-Type", "application/json")
	inbound.Header.Set("Authorization", "Bearer downstream-key")
	inbound.Header.Set("X-Api-Key", "downstream-key")
	inbound.Header.Set("X-Api-App-Key", "downstream-app")
	inbound.Header.Set("X-Api-Resource-Id", "downstream-resource")
	inbound.Header.Set("Cookie", "session=downstream")
	inbound.Header.Set("X-Trace", "keep")
	w := httptest.NewRecorder()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Authorization", "stale-downstream-value")

	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, []NativeRelayTarget{target})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, `{"ok":true}`, w.Body.String())
	require.Equal(t, "kept", w.Header().Get("X-Upstream"))
	require.Empty(t, w.Header().Get("Authorization"))
	require.Empty(t, w.Header().Get("Set-Cookie"))
	require.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, body, gotBody)
}

func TestNativeHTTPRelayInjectsDoubaoV3Credentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "provider-key", r.Header.Get("X-Api-Key"))
		require.Equal(t, "resource-id", r.Header.Get("X-Api-Resource-Id"))
		require.Empty(t, r.Header.Get("Authorization"))
		require.Empty(t, r.Header.Get("X-Api-App-Key"))
		require.Empty(t, r.Header.Get("X-Api-Access-Key"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	target := nativeHTTPTestTarget(t, server.URL, objects.NativeVoiceAPIFormatDoubaoTTS)
	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`))
	inbound.Header.Set("X-Api-Key", "downstream-key")
	inbound.Header.Set("X-Api-App-Key", "downstream-app")
	inbound.Header.Set("X-Api-Access-Key", "downstream-access")
	inbound.Header.Set("Authorization", "Bearer downstream-key")
	w := httptest.NewRecorder()

	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, []NativeRelayTarget{target})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, `{"ok":true}`, w.Body.String())
}

func TestNativeHTTPRelayFiltersConnectionScopedRequestHeaders(t *testing.T) {
	filtered := filterNativeHTTPRelayRequestHeaders(http.Header{
		"Connection":       []string{"X-Client-Hop, Another-Hop"},
		"X-Client-Hop":     []string{"remove"},
		"Another-Hop":      []string{"remove"},
		"Proxy-Connection": []string{"remove"},
		"X-Keep":           []string{"keep"},
	})

	require.Empty(t, filtered.Get("Connection"))
	require.Empty(t, filtered.Get("X-Client-Hop"))
	require.Empty(t, filtered.Get("Another-Hop"))
	require.Empty(t, filtered.Get("Proxy-Connection"))
	require.Equal(t, "keep", filtered.Get("X-Keep"))
}

func TestNativeHTTPRelayDoesNotCopyConnectionScopedResponseHeaders(t *testing.T) {
	destination := http.Header{
		"Connection":      []string{"X-Existing-Hop"},
		"X-Existing-Hop":  []string{"remove"},
		"X-Existing-Keep": []string{"keep"},
	}
	source := http.Header{
		"Connection":       []string{"X-Upstream-Hop"},
		"X-Upstream-Hop":   []string{"remove"},
		"Proxy-Connection": []string{"remove"},
		"X-Upstream-Keep":  []string{"keep"},
	}

	copyNativeHTTPRelayResponseHeaders(destination, source)

	require.Empty(t, destination.Get("Connection"))
	require.Empty(t, destination.Get("X-Existing-Hop"))
	require.Empty(t, destination.Get("X-Upstream-Hop"))
	require.Empty(t, destination.Get("Proxy-Connection"))
	require.Equal(t, "keep", destination.Get("X-Existing-Keep"))
	require.Equal(t, "keep", destination.Get("X-Upstream-Keep"))
}

func TestNativeHTTPRelayPreservesCompressedNonMiniMaxResponse(t *testing.T) {
	payload := []byte(`{"audio":"raw"}`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	rawResponse := compressed.Bytes()

	var acceptEncoding string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acceptEncoding = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(rawResponse)
	}))
	defer upstream.Close()

	target := nativeHTTPTestTarget(t, upstream.URL, objects.NativeVoiceAPIFormatDoubaoTTS)
	w := httptest.NewRecorder()
	err = NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`)),
		[]NativeRelayTarget{target})

	require.NoError(t, err)
	require.Equal(t, "identity", acceptEncoding)
	require.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	require.Equal(t, rawResponse, w.Body.Bytes())
}

func TestNativeHTTPRelayForwardsNonRetryableUpstreamError(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"provider-secret"}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer second.Close()

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`)),
		[]NativeRelayTarget{
			nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatDoubaoTTS),
			nativeHTTPTestTarget(t, second.URL, objects.NativeVoiceAPIFormatDoubaoTTS),
		})

	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, `{"error":"provider-secret"}`, w.Body.String())
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(0), secondCalls.Load())
}

func TestNativeHTTPRelayRetriesRetryableUpstreamResponseBeforeCommit(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer second.Close()

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`)),
		[]NativeRelayTarget{
			nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatDoubaoTTS),
			nativeHTTPTestTarget(t, second.URL, objects.NativeVoiceAPIFormatDoubaoTTS),
		})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, `{"ok":true}`, w.Body.String())
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
}

func TestNativeHTTPRelayForwardsFinalRetryableUpstreamError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
	}))
	defer upstream.Close()

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`)),
		[]NativeRelayTarget{nativeHTTPTestTarget(t, upstream.URL, objects.NativeVoiceAPIFormatDoubaoTTS)})

	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, `{"error":"temporarily unavailable"}`, w.Body.String())
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestNativeHTTPRelayForwardsLastRetryableResponseAfterLaterTransportFailure(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream-Error", "preserve-me")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
	}))
	defer first.Close()

	second := nativeHTTPTestTarget(t, "http://second.test", objects.NativeVoiceAPIFormatDoubaoTTS)
	second.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("second target transport failure")
	})})

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`)),
		[]NativeRelayTarget{
			nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatDoubaoTTS),
			second,
		})

	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, `{"error":"temporarily unavailable"}`, w.Body.String())
	require.Equal(t, "preserve-me", w.Header().Get("X-Upstream-Error"))
}

func TestNativeHTTPRelayForwardsLastBusinessResponseAfterLaterTransportFailure(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":2013,"status_msg":"invalid params"}}`))
	}))
	defer first.Close()

	second := nativeHTTPTestTarget(t, "http://second.test", objects.NativeVoiceAPIFormatMiniMaxT2A)
	second.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("second target transport failure")
	})})

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`)),
		[]NativeRelayTarget{
			nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
			second,
		})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"base_resp":{"status_code":2013,"status_msg":"invalid params"}}`, w.Body.String())
}

func TestNativeHTTPRelayRejectsRedirectBeforeProviderCredentialsCanLeak(t *testing.T) {
	var redirectedCalls, fallbackCalls atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectTarget.Close()

	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer provider-key", r.Header.Get("Authorization"))
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	defer redirecting.Close()

	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackCalls.Add(1)
		require.Equal(t, "Bearer provider-key", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer fallback.Close()

	targets := []NativeRelayTarget{
		nativeHTTPTestTarget(t, redirecting.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
		nativeHTTPTestTarget(t, fallback.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
	}
	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`)),
		targets)

	require.NoError(t, err)
	require.Equal(t, int32(0), redirectedCalls.Load())
	require.Equal(t, int32(1), fallbackCalls.Load())
	require.Equal(t, `{"ok":true}`, w.Body.String())
}

func TestNativeHTTPRelayKeepsTransportFailureOpaque(t *testing.T) {
	target := nativeHTTPTestTarget(t, "http://upstream.test", objects.NativeVoiceAPIFormatMiniMaxT2A)
	target.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("failed to reach https://provider.test/v1/t2a_v2?signature=client-secret")
	})})

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2?signature=client-secret&trace=1", strings.NewReader(`{"model":"speech-2.8-hd"}`)),
		[]NativeRelayTarget{target})

	require.ErrorIs(t, err, errNativeRelayUpstream)
	require.NotContains(t, err.Error(), "signature=")
	require.NotContains(t, err.Error(), "client-secret")
	require.Empty(t, w.Body.Bytes())
}

func TestNativeHTTPRelayRetriesPreCommitBusinessFailure(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":1004,"status_msg":"quota"}}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":0,"status_msg":"success"},"data":{"audio":"00"}}`))
	}))
	defer second.Close()

	targets := []NativeRelayTarget{
		nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
		nativeHTTPTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
	}
	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`))
	inbound.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, targets)

	require.NoError(t, err)
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	require.Contains(t, w.Body.String(), `"status_code":0`)
}

func TestNativeHTTPRelayForwardsFinalMiniMaxBusinessFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":2013,"status_msg":"invalid params"}}`))
	}))
	defer upstream.Close()

	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`))
	w := httptest.NewRecorder()

	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound,
		[]NativeRelayTarget{nativeHTTPTestTarget(t, upstream.URL, objects.NativeVoiceAPIFormatMiniMaxT2A)})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"base_resp":{"status_code":2013,"status_msg":"invalid params"}}`, w.Body.String())
}

func TestNativeHTTPRelayReportsFinalBusinessFailureToObserver(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":1004,"status_msg":"quota"}}`))
	}))
	defer upstream.Close()

	observer := &nativeRelayObserverCapture{}
	ctx := WithNativeRelayObserver(context.Background(), observer)
	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(ctx, w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`)),
		[]NativeRelayTarget{nativeHTTPTestTarget(t, upstream.URL, objects.NativeVoiceAPIFormatMiniMaxT2A)})

	require.NoError(t, err)
	require.Len(t, observer.attempts, 1)
	require.Len(t, observer.results, 1)
	require.Equal(t, http.StatusOK, observer.results[0].StatusCode)
	require.ErrorContains(t, observer.results[0].Err, "status_code=1004")
	require.False(t, observer.results[0].Retry)
}

func TestNativeHTTPRelayRetriesCompressedMiniMaxBusinessFailure(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		compressed := gzip.NewWriter(w)
		_, err := compressed.Write([]byte(`{"base_resp":{"status_code":1004,"status_msg":"quota"}}`))
		require.NoError(t, err)
		require.NoError(t, compressed.Close())
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"base_resp":{"status_code":0,"status_msg":"success"}}`))
	}))
	defer second.Close()

	targets := []NativeRelayTarget{
		nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
		nativeHTTPTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
	}
	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"model":"speech-2.8-hd"}`))
	inbound.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, targets)

	require.NoError(t, err)
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	require.Contains(t, w.Body.String(), `"status_code":0`)
}

func TestNativeHTTPRelayStreamsMiniMaxSSEBeforeUpstreamEOF(t *testing.T) {
	release := make(chan struct{})
	streamBody := &blockingReadBody{
		first:   []byte("data: {\"data\":{\"audio\":\"00\"},\"base_resp\":{\"status_code\":0}}\n\n"),
		release: release,
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       streamBody,
		}, nil
	})}
	target := nativeHTTPTestTarget(t, "http://upstream.test", objects.NativeVoiceAPIFormatMiniMaxT2A)
	target.Channel.HTTPClient = httpclient.NewHttpClientWithClient(client)
	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"stream":true}`))
	inbound.Header.Set("Content-Type", "application/json")
	w := newCaptureResponseWriter()

	done := make(chan error, 1)
	go func() {
		done <- NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, []NativeRelayTarget{target})
	}()

	select {
	case <-w.firstWrite:
	case <-time.After(500 * time.Millisecond):
		close(release)
		require.Fail(t, "relay waited for the complete MiniMax SSE response before forwarding the first event")
	}
	close(release)
	require.NoError(t, <-done)
	require.Contains(t, w.body.String(), "data: ")
}

func TestNativeHTTPRelayRetriesMiniMaxStreamingBusinessFailureBeforeCommit(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"base_resp\":{\"status_code\":1004,\"status_msg\":\"quota\"}}\n\n"))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"base_resp\":{\"status_code\":0,\"status_msg\":\"success\"}}\n\n"))
	}))
	defer second.Close()

	targets := []NativeRelayTarget{
		nativeHTTPTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
		nativeHTTPTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2A),
	}
	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"stream":true}`))
	inbound.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, targets)

	require.NoError(t, err)
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	require.Contains(t, w.Body.String(), `"status_code":0`)
}

func TestNativeHTTPRelayRetriesMiniMaxJSONStreamingBusinessFailureBeforeCommit(t *testing.T) {
	release := make(chan struct{})
	first := nativeHTTPTestTarget(t, "http://first.test", objects.NativeVoiceAPIFormatMiniMaxT2A)
	first.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: &blockingReadBody{
				first:   []byte(`{"base_resp":{"status_code":1004,"status_msg":"quota"}}`),
				release: release,
			},
		}, nil
	})})
	second := nativeHTTPTestTarget(t, "http://second.test", objects.NativeVoiceAPIFormatMiniMaxT2A)
	second.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"base_resp":{"status_code":0}}`)),
		}, nil
	})})

	w := httptest.NewRecorder()
	done := make(chan error, 1)
	go func() {
		done <- NewNativeHTTPRelay(nil).Relay(context.Background(), w,
			httptest.NewRequest(http.MethodPost, "http://axonhub.test/v1/t2a_v2", strings.NewReader(`{"stream":true}`)),
			[]NativeRelayTarget{first, second})
	}()

	var relayErr error
	select {
	case relayErr = <-done:
	case <-time.After(500 * time.Millisecond):
		close(release)
		require.Fail(t, "relay waited for the complete JSON stream before inspecting base_resp")
		relayErr = <-done
	}
	require.NoError(t, relayErr)
	require.Contains(t, w.Body.String(), `"status_code":0`)
}

func TestNativeHTTPRelayDoesNotRetryAfterResponseCommit(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &errorAfterFirstReadBody{},
		}, nil
	})}
	target := nativeHTTPTestTarget(t, "http://upstream.test", objects.NativeVoiceAPIFormatDoubaoTTS)
	target.Channel.HTTPClient = httpclient.NewHttpClientWithClient(client)
	second := nativeHTTPTestTarget(t, "http://unused.test", objects.NativeVoiceAPIFormatDoubaoTTS)

	inbound := httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`))
	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w, inbound, []NativeRelayTarget{target, second})

	require.Error(t, err)
	require.Contains(t, err.Error(), "upstream body failed")
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, "first", w.Body.String())
}

func TestNativeHTTPRelayRetriesAfterEmptyReadBeforeResponseCommit(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := nativeHTTPTestTarget(t, "http://first.test", objects.NativeVoiceAPIFormatDoubaoTTS)
	first.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		firstCalls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &emptyThenErrorBody{},
		}, nil
	})})
	second := nativeHTTPTestTarget(t, "http://second.test", objects.NativeVoiceAPIFormatDoubaoTTS)
	second.Channel.HTTPClient = httpclient.NewHttpClientWithClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		secondCalls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("fallback")),
		}, nil
	})})

	w := httptest.NewRecorder()
	err := NewNativeHTTPRelay(nil).Relay(context.Background(), w,
		httptest.NewRequest(http.MethodPost, "http://axonhub.test/api/v3/tts/unidirectional", strings.NewReader(`{"text":"hello"}`)),
		[]NativeRelayTarget{first, second})

	require.NoError(t, err)
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	require.Equal(t, "fallback", w.Body.String())
}

func nativeHTTPTestTarget(t *testing.T, baseURL, apiFormat string) NativeRelayTarget {
	t.Helper()
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(apiFormat)
	require.True(t, ok)
	return NativeRelayTarget{
		Channel: &biz.Channel{Channel: &ent.Channel{
			ID: 1, Name: "native", Type: channel.TypeDoubao, Status: channel.StatusEnabled,
			Credentials: objects.ChannelCredentials{APIKey: "provider-key"},
		}},
		Endpoint: objects.ChannelEndpoint{
			APIFormat:  protocol.APIFormat,
			Path:       protocol.Path,
			BaseURL:    baseURL,
			ResourceID: "resource-id",
		},
		Protocol: protocol,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type nativeRelayObserverCapture struct {
	attempts []NativeRelayAttempt
	results  []NativeRelayResult
}

func (o *nativeRelayObserverCapture) OnNativeRelayAttempt(_ context.Context, attempt NativeRelayAttempt) {
	o.attempts = append(o.attempts, attempt)
}

func (o *nativeRelayObserverCapture) OnNativeRelayResult(_ context.Context, result NativeRelayResult) {
	o.results = append(o.results, result)
}

type errorAfterFirstReadBody struct {
	read bool
}

func (b *errorAfterFirstReadBody) Read(p []byte) (int, error) {
	if b.read {
		return 0, errors.New("upstream body failed")
	}
	b.read = true
	return copy(p, "first"), nil
}

func (b *errorAfterFirstReadBody) Close() error { return nil }

type emptyThenErrorBody struct {
	read bool
}

func (b *emptyThenErrorBody) Read([]byte) (int, error) {
	if !b.read {
		b.read = true
		return 0, nil
	}
	return 0, errors.New("upstream body failed before first byte")
}

func (b *emptyThenErrorBody) Close() error { return nil }

type blockingReadBody struct {
	first   []byte
	release <-chan struct{}
	sent    bool
}

func (b *blockingReadBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, b.first), nil
	}
	<-b.release
	return 0, io.EOF
}

func (b *blockingReadBody) Close() error { return nil }

type captureResponseWriter struct {
	header     http.Header
	statusCode int
	body       bytes.Buffer
	firstWrite chan struct{}
	once       atomic.Bool
}

func newCaptureResponseWriter() *captureResponseWriter {
	return &captureResponseWriter{header: make(http.Header), firstWrite: make(chan struct{})}
}

func (w *captureResponseWriter) Header() http.Header { return w.header }

func (w *captureResponseWriter) WriteHeader(statusCode int) { w.statusCode = statusCode }

func (w *captureResponseWriter) Write(p []byte) (int, error) {
	if w.once.CompareAndSwap(false, true) {
		close(w.firstWrite)
	}
	return w.body.Write(p)
}

func (w *captureResponseWriter) Flush() {}
