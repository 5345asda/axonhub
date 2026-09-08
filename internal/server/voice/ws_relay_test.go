package voice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

func TestNativeWebSocketRelayFailsOverBeforeDownstreamUpgrade(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer first.Close()

	var upstreamAuth atomic.Bool
	upgrader := websocket.Upgrader{}
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer provider-key" && r.Header.Get("Cookie") == "" {
			upstreamAuth.Store(true)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.WriteMessage(messageType, append([]byte("echo:"), message...))
	}))
	defer second.Close()

	relay := NewNativeWebSocketRelay(nil)
	relay.dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = relay.Relay(context.Background(), w, r, []NativeRelayTarget{
			nativeWebSocketTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
			nativeWebSocketTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 2),
		})
	}))
	defer proxy.Close()

	proxyURL := "ws" + proxy.URL[len("http"):]
	client, _, err := websocket.DefaultDialer.Dial(proxyURL+"/ws/v1/t2a_v2_bidi", http.Header{
		"Authorization": []string{"Bearer downstream-key"},
		"Cookie":        []string{"session=downstream"},
	})
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("hello")))
	messageType, message, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, messageType)
	require.Equal(t, "echo:hello", string(message))
	require.True(t, upstreamAuth.Load())
}

func TestNativeWebSocketRelayForwardsFinalUpstreamHandshakeResponse(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"first candidate"}`))
	}))
	defer first.Close()

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.Header().Set("X-Upstream-Trace", "trace-123")
		w.Header().Set("Authorization", "provider-secret")
		w.Header().Set("Set-Cookie", "provider-secret=1")
		w.Header().Set("WWW-Authenticate", `Bearer realm="provider"`)
		w.Header().Set("Connection", "X-Provider-Hop")
		w.Header().Set("X-Provider-Hop", "remove")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid provider credentials"}`))
	}))
	defer second.Close()

	relay := NewNativeWebSocketRelay(nil)
	relay.dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := relay.Relay(context.Background(), w, r, []NativeRelayTarget{
			nativeWebSocketTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
			nativeWebSocketTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 2),
		}); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
	}))
	defer proxy.Close()

	proxyURL := "ws" + proxy.URL[len("http"):]
	client, response, err := websocket.DefaultDialer.Dial(proxyURL+"/ws/v1/t2a_v2_bidi", nil)
	require.Error(t, err)
	require.Nil(t, client)
	require.NotNil(t, response)
	defer response.Body.Close()

	body, readErr := io.ReadAll(response.Body)
	require.NoError(t, readErr)
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	require.JSONEq(t, `{"error":"invalid provider credentials"}`, string(body))
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))
	require.Equal(t, "7", response.Header.Get("Retry-After"))
	require.Equal(t, "trace-123", response.Header.Get("X-Upstream-Trace"))
	require.Equal(t, `Bearer realm="provider"`, response.Header.Get("WWW-Authenticate"))
	require.Empty(t, response.Header.Get("Authorization"))
	require.Empty(t, response.Header.Get("Set-Cookie"))
	require.Empty(t, response.Header.Get("Connection"))
	require.Empty(t, response.Header.Get("X-Provider-Hop"))
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
}

func TestNativeWebSocketRelayReportsEachFailedHandshakeOnce(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"first candidate"}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"second candidate"}`))
	}))
	defer second.Close()

	observer := &nativeRelayObserverCapture{}
	inbound := httptest.NewRequest(http.MethodGet, "http://axonhub.test/ws/v1/t2a_v2_bidi", nil)
	inbound.Header.Set("Connection", "Upgrade")
	inbound.Header.Set("Upgrade", "websocket")
	w := httptest.NewRecorder()

	err := NewNativeWebSocketRelay(nil).Relay(WithNativeRelayObserver(context.Background(), observer), w, inbound, []NativeRelayTarget{
		nativeWebSocketTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
		nativeWebSocketTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 2),
	})

	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Len(t, observer.attempts, 2)
	require.Len(t, observer.results, 2)
	require.Equal(t, http.StatusServiceUnavailable, observer.results[0].StatusCode)
	require.False(t, observer.results[0].Committed)
	require.Equal(t, http.StatusUnauthorized, observer.results[1].StatusCode)
	require.True(t, observer.results[1].Committed)
}

func TestNativeWebSocketRelayPreservesHandshakeResponseAfterTransportFailure(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid provider credentials"}`))
	}))
	defer first.Close()

	second := httptest.NewServer(http.NotFoundHandler())
	secondURL := second.URL
	second.Close()

	relay := NewNativeWebSocketRelay(nil)
	relay.dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := relay.Relay(context.Background(), w, r, []NativeRelayTarget{
			nativeWebSocketTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
			nativeWebSocketTestTarget(t, secondURL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 2),
		}); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
	}))
	defer proxy.Close()

	proxyURL := "ws" + proxy.URL[len("http"):]
	client, response, err := websocket.DefaultDialer.Dial(proxyURL+"/ws/v1/t2a_v2_bidi", nil)
	require.Error(t, err)
	require.Nil(t, client)
	require.NotNil(t, response)
	defer response.Body.Close()

	body, readErr := io.ReadAll(response.Body)
	require.NoError(t, readErr)
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	require.JSONEq(t, `{"error":"invalid provider credentials"}`, string(body))
}

func TestNativeWebSocketRelayKeepsTransportFailuresOpaque(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	upstreamURL := upstream.URL
	upstream.Close()

	inbound := httptest.NewRequest(http.MethodGet, "http://axonhub.test/ws/v1/t2a_v2_bidi", nil)
	inbound.Header.Set("Connection", "Upgrade")
	inbound.Header.Set("Upgrade", "websocket")
	w := httptest.NewRecorder()

	err := NewNativeWebSocketRelay(nil).Relay(context.Background(), w, inbound, []NativeRelayTarget{
		nativeWebSocketTestTarget(t, upstreamURL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
	})

	require.ErrorIs(t, err, errNativeRelayUpstream)
	require.NotContains(t, err.Error(), upstreamURL)
	require.Zero(t, w.Body.Len())
}

func TestNativeWebSocketRelayDoesNotRetryAfterApplicationFrame(t *testing.T) {
	var secondCalls atomic.Int32
	upgrader := websocket.Upgrader{}
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err == nil {
			_ = conn.WriteMessage(websocket.BinaryMessage, []byte{0x01, 0x02})
		}
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "done"))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer second.Close()

	relay := NewNativeWebSocketRelay(nil)
	relay.dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = relay.Relay(context.Background(), w, r, []NativeRelayTarget{
			nativeWebSocketTestTarget(t, first.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
			nativeWebSocketTestTarget(t, second.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 2),
		})
	}))
	defer proxy.Close()

	proxyURL := "ws" + proxy.URL[len("http"):]
	client, _, err := websocket.DefaultDialer.Dial(proxyURL+"/ws/v1/t2a_v2_bidi", nil)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("hello")))

	messageType, message, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, messageType)
	require.Equal(t, []byte{0x01, 0x02}, message)
	_, _, _ = client.ReadMessage()
	require.Equal(t, int32(0), secondCalls.Load())
}

func TestNativeWebSocketRelayForwardsPingWithoutControlLoop(t *testing.T) {
	upgrader := websocket.Upgrader{}
	var upstreamPings, clientPongs atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetPingHandler(func(appData string) error {
			upstreamPings.Add(1)
			return nil
		})
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(messageType, message); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()

	relay := NewNativeWebSocketRelay(nil)
	relay.dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = relay.Relay(context.Background(), w, r, []NativeRelayTarget{
			nativeWebSocketTestTarget(t, upstream.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
		})
	}))
	defer proxy.Close()

	proxyURL := "ws" + proxy.URL[len("http"):]
	client, _, err := websocket.DefaultDialer.Dial(proxyURL+"/ws/v1/t2a_v2_bidi", nil)
	require.NoError(t, err)
	defer client.Close()

	client.SetPongHandler(func(string) error {
		clientPongs.Add(1)
		return nil
	})
	require.NoError(t, client.WriteControl(websocket.PingMessage, []byte("probe"), time.Now().Add(time.Second)))
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("after-ping")))
	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	_, message, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "after-ping", string(message))
	require.Equal(t, int32(1), upstreamPings.Load())
	require.GreaterOrEqual(t, clientPongs.Load(), int32(1))
}

func TestNativeWebSocketRelayNegotiatesSubprotocol(t *testing.T) {
	const subprotocol = "native-voice-v1"
	upstreamSubprotocol := make(chan string, 1)
	upgrader := websocket.Upgrader{Subprotocols: []string{subprotocol}}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamSubprotocol <- r.Header.Get("Sec-WebSocket-Protocol")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		messageType, message, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(messageType, message)
		}
	}))
	defer upstream.Close()

	relay := NewNativeWebSocketRelay(nil)
	relay.dialer = &websocket.Dialer{HandshakeTimeout: time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = relay.Relay(context.Background(), w, r, []NativeRelayTarget{
			nativeWebSocketTestTarget(t, upstream.URL, objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1),
		})
	}))
	defer proxy.Close()

	proxyURL := "ws" + proxy.URL[len("http"):]
	client, _, err := (&websocket.Dialer{Subprotocols: []string{subprotocol}}).Dial(proxyURL+"/ws/v1/t2a_v2_bidi", nil)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, subprotocol, <-upstreamSubprotocol)
	require.Equal(t, subprotocol, client.Subprotocol())
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("hello")))
	_, message, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "hello", string(message))
}

func TestBuildNativeWebSocketURLPreservesBasePathAndQuery(t *testing.T) {
	inbound, err := url.Parse("https://axonhub.test/ws/v1/t2a_v2_bidi?model=speech-2.8-hd")
	require.NoError(t, err)
	target := nativeWebSocketTestTarget(t, "https://provider.test/prefix", objects.NativeVoiceAPIFormatMiniMaxT2ABidi, 1)

	upstream, err := buildNativeWebSocketURL(inbound, target)

	require.NoError(t, err)
	require.Equal(t, "wss", upstream.Scheme)
	require.Equal(t, "/prefix/ws/v1/t2a_v2_bidi", upstream.Path)
	require.Equal(t, "model=speech-2.8-hd", upstream.RawQuery)
}

func nativeWebSocketTestTarget(t *testing.T, baseURL, apiFormat string, id int) NativeRelayTarget {
	t.Helper()
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(apiFormat)
	require.True(t, ok)
	return NativeRelayTarget{
		Channel: &biz.Channel{Channel: &ent.Channel{
			ID: id, Name: "native", Type: channel.TypeMinimax, Status: channel.StatusEnabled,
			Credentials: objects.ChannelCredentials{APIKey: "provider-key"},
		}},
		Endpoint: objects.ChannelEndpoint{
			APIFormat: protocol.APIFormat,
			Path:      protocol.Path,
			BaseURL:   baseURL,
			Transport: protocol.Transport,
		},
		Protocol: protocol,
	}
}
