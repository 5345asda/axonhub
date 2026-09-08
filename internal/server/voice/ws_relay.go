package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/looplj/axonhub/internal/log"
)

// NativeWebSocketRelay performs exact WebSocket relay for native provider speech protocols.
type NativeWebSocketRelay struct {
	dialer    *websocket.Dialer
	admission *NativeRelayAdmission
}

// NewNativeWebSocketRelay constructs a provider-native WebSocket relay.
func NewNativeWebSocketRelay(admission *NativeRelayAdmission) *NativeWebSocketRelay {
	return &NativeWebSocketRelay{
		admission: admission,
		dialer: &websocket.Dialer{
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  10 * time.Second,
			EnableCompression: true,
		},
	}
}

// Relay establishes an upstream connection before upgrading downstream, then
// forwards frames in both directions. Retry is permitted only while upstream
// handshakes are still failing; once a provider WebSocket is accepted, the
// session stays on that connection.
func (r *NativeWebSocketRelay) Relay(ctx context.Context, w http.ResponseWriter, inbound *http.Request, targets []NativeRelayTarget) error {
	if inbound == nil {
		return errors.New("native websocket relay requires an inbound request")
	}
	if len(targets) == 0 {
		return errors.New("native websocket relay requires at least one target")
	}
	if !websocket.IsWebSocketUpgrade(inbound) {
		return errors.New("native websocket relay requires a websocket upgrade request")
	}

	var lastErr error
	var upstreamConn *websocket.Conn
	var upstreamSlot *nativeRelayAdmissionSlot
	for _, target := range targets {
		if target.Channel == nil {
			lastErr = errors.New("native websocket relay target is missing channel")
			continue
		}
		slot, admissionErr := r.admission.acquire(ctx, target.Channel)
		if admissionErr != nil {
			lastErr = fmt.Errorf("native voice channel admission failed: %w", admissionErr)
			continue
		}
		conn, err := r.dialUpstream(ctx, inbound, target)
		if err != nil {
			slot.release()
			lastErr = err
			continue
		}
		upstreamConn = conn
		upstreamSlot = slot
		break
	}

	if upstreamConn == nil {
		if lastErr == nil {
			lastErr = errors.New("native websocket relay exhausted targets")
		}
		return nativeRelayPublicError(lastErr)
	}
	defer upstreamConn.Close()
	defer upstreamSlot.release()

	responseHeader := make(http.Header)
	if subprotocol := upstreamConn.Subprotocol(); subprotocol != "" {
		// The provider selected this value from the client-offered protocols.
		// Echo the exact result so the downstream handshake stays transparent.
		responseHeader.Set("Sec-WebSocket-Protocol", subprotocol)
	}
	downstreamConn, err := new(websocket.Upgrader).Upgrade(w, inbound, responseHeader)
	if err != nil {
		return fmt.Errorf("failed to upgrade downstream websocket: %w", err)
	}
	defer downstreamConn.Close()

	if err := r.proxyConnections(ctx, downstreamConn, upstreamConn); err != nil {
		return err
	}
	return nil
}

func (r *NativeWebSocketRelay) dialUpstream(ctx context.Context, inbound *http.Request, target NativeRelayTarget) (*websocket.Conn, error) {
	if inbound == nil || inbound.URL == nil {
		return nil, errors.New("native websocket relay request URL is required")
	}

	upstreamURL, err := buildNativeWebSocketURL(inbound.URL, target)
	if err != nil {
		return nil, err
	}

	requestHeader := filterNativeWebSocketRelayHeaders(inbound.Header)
	credentialHeaders := defaultNativeVoiceCredentialHeaders(ctx, target.Channel, target.Protocol, target.Endpoint)
	for key, values := range credentialHeaders {
		requestHeader.Del(key)
		for _, value := range values {
			requestHeader.Add(key, value)
		}
	}

	dialer := r.dialerForTarget(target)
	conn, resp, err := dialer.DialContext(ctx, upstreamURL.String(), requestHeader)
	if err != nil {
		r.admission.observeHTTPResponse(target.Channel, resp)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, fmt.Errorf("failed to dial native websocket upstream: %w", err)
	}

	return conn, nil
}

func (r *NativeWebSocketRelay) dialerForTarget(target NativeRelayTarget) *websocket.Dialer {
	base := websocket.DefaultDialer
	if r != nil && r.dialer != nil {
		cloned := *r.dialer
		base = &cloned
	}
	if target.Channel != nil && target.Channel.HTTPClient != nil {
		base.Proxy = target.Channel.HTTPClient.ProxyFunc()
		if native := target.Channel.HTTPClient.GetNativeClient(); native != nil {
			if transport, ok := native.Transport.(*http.Transport); ok && transport.TLSClientConfig != nil {
				base.TLSClientConfig = transport.TLSClientConfig.Clone()
			}
		}
	}
	return base
}

func buildNativeWebSocketURL(inbound *url.URL, target NativeRelayTarget) (*url.URL, error) {
	if inbound == nil {
		return nil, errors.New("native websocket request URL is required")
	}

	upstreamURL, err := buildNativeUpstreamURL(target.Endpoint.BaseURL, target.Protocol.Path, inbound.RawQuery)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(upstreamURL.Scheme) {
	case "http":
		upstreamURL.Scheme = "ws"
	case "https":
		upstreamURL.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("native websocket upstream base URL must use http(s) or ws(s): %s", target.Endpoint.BaseURL)
	}
	return upstreamURL, nil
}

func filterNativeWebSocketRelayHeaders(src http.Header) http.Header {
	dst := filterNativeHTTPRelayRequestHeaders(src)
	for key := range dst {
		canonical := http.CanonicalHeaderKey(key)
		if strings.HasPrefix(canonical, "Sec-Websocket-") && canonical != "Sec-Websocket-Protocol" {
			delete(dst, key)
		}
	}
	return dst
}

func (r *NativeWebSocketRelay) proxyConnections(ctx context.Context, downstream, upstream *websocket.Conn) error {
	done := make(chan error, 2)
	var downstreamWriteMu sync.Mutex
	var upstreamWriteMu sync.Mutex
	closeBoth := func() {
		_ = downstream.Close()
		_ = upstream.Close()
	}

	setupNativeWebSocketControlRelay(downstream, &downstreamWriteMu, upstream, &upstreamWriteMu, true, false)
	setupNativeWebSocketControlRelay(upstream, &upstreamWriteMu, downstream, &downstreamWriteMu, false, true)

	go func() {
		defer func() {
			if recover() != nil {
				log.Error(ctx, "panic in native websocket relay", log.String("direction", "upstream-to-downstream"))
				done <- errors.New("native websocket relay panicked")
			}
		}()
		done <- copyNativeWebSocketMessages(downstream, &downstreamWriteMu, upstream)
	}()
	go func() {
		defer func() {
			if recover() != nil {
				log.Error(ctx, "panic in native websocket relay", log.String("direction", "downstream-to-upstream"))
				done <- errors.New("native websocket relay panicked")
			}
		}()
		done <- copyNativeWebSocketMessages(upstream, &upstreamWriteMu, downstream)
	}()

	select {
	case err := <-done:
		closeBoth()
		if err != nil && !isNativeWebSocketNormalClose(err) {
			return err
		}
		return nil
	case <-ctx.Done():
		closeBoth()
		return ctx.Err()
	}
}

func setupNativeWebSocketControlRelay(
	src *websocket.Conn,
	srcWriteMu *sync.Mutex,
	dst *websocket.Conn,
	dstWriteMu *sync.Mutex,
	forwardPing bool,
	forwardPong bool,
) {
	src.SetReadLimit(32 << 20)
	src.SetPingHandler(func(appData string) error {
		if err := writeNativeWebSocketControl(src, srcWriteMu, websocket.PongMessage, []byte(appData)); err != nil {
			return err
		}
		if !forwardPing {
			return nil
		}
		return writeNativeWebSocketControl(dst, dstWriteMu, websocket.PingMessage, []byte(appData))
	})
	src.SetPongHandler(func(appData string) error {
		if !forwardPong {
			return nil
		}
		return writeNativeWebSocketControl(dst, dstWriteMu, websocket.PongMessage, []byte(appData))
	})
	src.SetCloseHandler(func(code int, text string) error {
		message := websocket.FormatCloseMessage(code, text)
		_ = writeNativeWebSocketControl(dst, dstWriteMu, websocket.CloseMessage, message)
		return nil
	})
}

func copyNativeWebSocketMessages(dst *websocket.Conn, writeMu *sync.Mutex, src *websocket.Conn) error {
	for {
		messageType, message, err := src.ReadMessage()
		if err != nil {
			if isNativeWebSocketNormalClose(err) {
				return nil
			}
			return err
		}
		if err := writeNativeWebSocketMessage(dst, writeMu, messageType, message); err != nil {
			return err
		}
	}
}

func writeNativeWebSocketMessage(dst *websocket.Conn, writeMu *sync.Mutex, messageType int, payload []byte) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	return dst.WriteMessage(messageType, payload)
}

func writeNativeWebSocketControl(dst *websocket.Conn, writeMu *sync.Mutex, messageType int, payload []byte) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	return dst.WriteControl(messageType, payload, time.Now().Add(time.Second))
}

func isNativeWebSocketNormalClose(err error) bool {
	return websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, context.Canceled)
}
