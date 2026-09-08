package voice

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm/httpclient"
)

func defaultNativeVoiceCredentialHeaders(
	ctx context.Context,
	ch *biz.Channel,
	protocol objects.NativeVoiceProtocol,
	endpoint objects.ChannelEndpoint,
) http.Header {
	headers := make(http.Header)
	if ch == nil {
		return headers
	}

	apiKey := strings.TrimSpace(ch.SelectAPIKey(ctx))

	switch protocol.AuthMode {
	case objects.NativeVoiceAuthMiniMax, objects.NativeVoiceAuthDashScope:
		if apiKey != "" {
			headers.Set("Authorization", "Bearer "+apiKey)
		}
	case objects.NativeVoiceAuthDoubaoV3:
		if apiKey != "" {
			headers.Set("X-Api-Key", apiKey)
		}
		if resourceID := strings.TrimSpace(endpoint.ResourceID); resourceID != "" {
			headers.Set("X-Api-Resource-Id", resourceID)
		}
	}

	return headers
}

// NativeHTTPRelay performs exact-path HTTP, SSE and chunked relay for native
// provider speech endpoints.
type NativeHTTPRelay struct {
	admission *NativeRelayAdmission
}

// NativeRelayTarget represents one ordered candidate.
type NativeRelayTarget struct {
	Channel  *biz.Channel
	Endpoint objects.ChannelEndpoint
	Protocol objects.NativeVoiceProtocol
}

// NewNativeHTTPRelay builds a provider-native HTTP relay.
func NewNativeHTTPRelay(admission *NativeRelayAdmission) *NativeHTTPRelay {
	return &NativeHTTPRelay{admission: admission}
}

// Relay forwards a request to ordered same-protocol targets. It retries only
// before the first downstream byte is committed.
func (r *NativeHTTPRelay) Relay(ctx context.Context, w http.ResponseWriter, inbound *http.Request, targets []NativeRelayTarget) error {
	if inbound == nil {
		return errors.New("native voice relay requires an inbound request")
	}
	if len(targets) == 0 {
		return errors.New("native voice relay requires at least one target")
	}

	var (
		body []byte
		err  error
	)
	if inbound.Body != nil {
		body, err = io.ReadAll(inbound.Body)
		if err != nil {
			return nativeRelayPublicError(err)
		}
	}
	streamingRequest := nativeVoiceRequestIsStreaming(body)

	var (
		lastErr         error
		lastResponse    *nativeHTTPResponseSnapshot
		nativeAttemptID int
	)
	for index, target := range targets {
		if target.Channel == nil {
			lastErr = errors.New("native voice relay target is missing channel")
			continue
		}
		slot, admissionErr := r.admission.acquire(ctx, target.Channel)
		if admissionErr != nil {
			lastErr = fmt.Errorf("native voice channel admission failed: %w", admissionErr)
			continue
		}

		client := nativeHTTPClientForChannel(target.Channel)

		outboundReq, buildErr := r.buildNativeHTTPRelayRequest(ctx, inbound, body, target)
		if buildErr != nil {
			slot.release()
			lastErr = buildErr
			continue
		}

		nativeAttemptID++
		attemptStartedAt := time.Now()
		notifyNativeRelayAttempt(ctx, NativeRelayAttempt{
			ID:             nativeAttemptID,
			Target:         target,
			URL:            outboundReq.URL.String(),
			RequestHeaders: outboundReq.Header.Clone(),
			StartedAt:      attemptStartedAt,
		})

		resp, respErr := client.Do(outboundReq)
		if respErr != nil {
			statusCode := 0
			var responseHeaders http.Header
			if resp != nil {
				statusCode = resp.StatusCode
				responseHeaders = resp.Header.Clone()
			}
			notifyNativeRelayResult(ctx, NativeRelayResult{
				ID:              nativeAttemptID,
				StatusCode:      statusCode,
				ResponseHeaders: responseHeaders,
				Err:             respErr,
				Retry:           index < len(targets)-1,
				Duration:        time.Since(attemptStartedAt),
			})
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			slot.release()
			lastErr = respErr
			continue
		}

		r.admission.observeHTTPResponse(target.Channel, resp)
		retry, attemptErr := classifyNativeHTTPResponse(resp, target, streamingRequest)
		if retry {
			if !nativeHTTPResponseCanBeForwarded(resp, attemptErr) {
				notifyNativeRelayResult(ctx, NativeRelayResult{
					ID:              nativeAttemptID,
					StatusCode:      resp.StatusCode,
					ResponseHeaders: resp.Header.Clone(),
					Err:             attemptErr,
					Retry:           index < len(targets)-1,
					Duration:        time.Since(attemptStartedAt),
				})
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
				slot.release()
				lastErr = attemptErr
				continue
			}
			response := captureNativeHTTPResponse(resp, nativeAttemptID, attemptStartedAt, attemptErr)
			if lastResponse != nil {
				notifyNativeRelayResult(ctx, lastResponse.result(true, false, lastResponse.attempt.ResponseBytes, nil))
				lastResponse.close()
			}
			lastResponse = response
			if lastResponse == nil {
				lastErr = errors.New("native voice upstream response is nil")
				slot.release()
				continue
			}
			slot.release()
			lastErr = attemptErr
			continue
		}

		committed, responseBytes, writeErr := writeNativeHTTPResponse(w, resp.StatusCode, resp.Header, resp.Body)
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		slot.release()

		if writeErr != nil && !committed {
			notifyNativeRelayResult(ctx, NativeRelayResult{
				ID:              nativeAttemptID,
				StatusCode:      resp.StatusCode,
				ResponseHeaders: resp.Header.Clone(),
				ResponseBytes:   responseBytes,
				Err:             writeErr,
				Retry:           index < len(targets)-1,
				Duration:        time.Since(attemptStartedAt),
			})
			lastErr = writeErr
			continue
		}

		if lastResponse != nil {
			notifyNativeRelayResult(ctx, lastResponse.result(true, false, lastResponse.attempt.ResponseBytes, nil))
			lastResponse.close()
			lastResponse = nil
		}
		notifyNativeRelayResult(ctx, NativeRelayResult{
			ID:              nativeAttemptID,
			StatusCode:      resp.StatusCode,
			ResponseHeaders: resp.Header.Clone(),
			ResponseBytes:   responseBytes,
			Err:             writeErr,
			Committed:       committed,
			Duration:        time.Since(attemptStartedAt),
		})
		if writeErr != nil {
			return writeErr
		}
		return nil
	}

	if lastResponse != nil {
		committed, responseBytes, writeErr := lastResponse.writeTo(w)
		result := lastResponse.result(false, committed, responseBytes, writeErr)
		lastResponse.close()
		notifyNativeRelayResult(ctx, result)
		if writeErr != nil {
			if committed {
				return writeErr
			}
			return nativeRelayPublicError(writeErr)
		}
		return nil
	}

	if lastErr == nil {
		lastErr = errors.New("native voice relay exhausted targets")
	}

	return nativeRelayPublicError(lastErr)
}

func (r *NativeHTTPRelay) buildNativeHTTPRelayRequest(
	ctx context.Context,
	inbound *http.Request,
	body []byte,
	target NativeRelayTarget,
) (*http.Request, error) {
	upstreamURL, err := buildNativeUpstreamURL(target.Endpoint.BaseURL, target.Protocol.Path, inbound.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, inbound.Method, upstreamURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header = filterNativeHTTPRelayRequestHeaders(inbound.Header)
	if req.Header.Get("Accept-Encoding") == "" {
		// Prevent net/http from silently decoding a provider response before the
		// relay can preserve its native bytes and Content-Encoding header.
		req.Header.Set("Accept-Encoding", "identity")
	}

	credentialHeaders := defaultNativeVoiceCredentialHeaders(ctx, target.Channel, target.Protocol, target.Endpoint)
	for key, values := range credentialHeaders {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	return req, nil
}

func nativeHTTPClientForChannel(ch *biz.Channel) *http.Client {
	client := http.DefaultClient
	if ch != nil && ch.HTTPClient != nil {
		if native := ch.HTTPClient.GetNativeClient(); native != nil {
			client = native
		}
	}

	// Provider credentials are injected after the AxonHub2 boundary. Following
	// a redirect could send them to another authority, so every native attempt
	// fails over instead.
	cloned := *client
	cloned.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("native voice upstream redirect rejected")
	}
	return &cloned
}

func filterNativeHTTPRelayRequestHeaders(src http.Header) http.Header {
	dst := make(http.Header, len(src))
	connectionHeaders := nativeHTTPConnectionHeaderNames(src)
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if isNativeHTTPRelayStrippedRequestHeader(canonical) || nativeHTTPHeaderNamedByConnection(connectionHeaders, canonical) {
			continue
		}
		dst[canonical] = append([]string(nil), values...)
	}
	return dst
}

func isNativeHTTPRelayStrippedRequestHeader(key string) bool {
	if isNativeHTTPRelayCredentialHeader(key) {
		return true
	}

	switch key {
	case "Host",
		"Content-Length",
		"Transfer-Encoding",
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Connection",
		"Te",
		"Trailer",
		"Upgrade":
		return true
	default:
		return false
	}
}

func isNativeHTTPRelayResponseHopByHopHeader(key string) bool {
	switch key {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}

// nativeHTTPConnectionHeaderNames returns connection-scoped fields named by
// Connection, which HTTP proxies must remove in addition to the fixed list.
func nativeHTTPConnectionHeaderNames(headers http.Header) map[string]struct{} {
	result := make(map[string]struct{})
	for _, value := range headers.Values("Connection") {
		for name := range strings.SplitSeq(value, ",") {
			if canonical := http.CanonicalHeaderKey(strings.TrimSpace(name)); canonical != "" {
				result[canonical] = struct{}{}
			}
		}
	}
	return result
}

func nativeHTTPHeaderNamedByConnection(names map[string]struct{}, key string) bool {
	_, ok := names[http.CanonicalHeaderKey(key)]
	return ok
}

func isNativeHTTPRelayCredentialHeader(key string) bool {
	switch key {
	case "Authorization",
		"Proxy-Authorization",
		"API-Key",
		"Api-Key",
		"X-Api-Key",
		"X-Api-Secret",
		"X-Api-App-Key",
		"X-Api-Access-Key",
		"X-Api-Resource-Id",
		"X-Api-Connect-Id",
		"X-Api-Token",
		"X-Goog-Api-Key",
		"X-Google-Api-Key",
		"Cookie",
		"Set-Cookie":
		return true
	default:
		return false
	}
}

func classifyNativeHTTPResponse(resp *http.Response, target NativeRelayTarget, streamingRequest bool) (bool, error) {
	if resp == nil {
		return true, errors.New("native voice upstream response is nil")
	}
	if resp.Body == nil {
		resp.Body = io.NopCloser(strings.NewReader(""))
	}

	if resp.StatusCode >= http.StatusBadRequest && httpclient.IsHTTPStatusCodeRetryable(resp.StatusCode) {
		return true, fmt.Errorf("native voice upstream returned retryable status %d", resp.StatusCode)
	}

	if target.Protocol.InspectBusinessStatus {
		if err := inspectNativeHTTPBusinessStatus(resp, streamingRequest); err != nil {
			return true, err
		}
	}

	return false, nil
}

func nativeHTTPResponseCanBeForwarded(resp *http.Response, err error) bool {
	if resp == nil {
		return false
	}
	if resp.StatusCode >= http.StatusBadRequest && httpclient.IsHTTPStatusCodeRetryable(resp.StatusCode) {
		return true
	}
	var businessErr *nativeHTTPBusinessStatusError
	return errors.As(err, &businessErr)
}

func captureNativeHTTPResponse(resp *http.Response, attemptID int, startedAt time.Time, err error) *nativeHTTPResponseSnapshot {
	if resp == nil {
		return nil
	}
	body := resp.Body
	if body == nil {
		body = io.NopCloser(strings.NewReader(""))
	}
	resp.Body = nil
	return &nativeHTTPResponseSnapshot{
		statusCode: resp.StatusCode,
		header:     resp.Header.Clone(),
		body:       body,
		attempt: NativeRelayResult{
			ID:              attemptID,
			StatusCode:      resp.StatusCode,
			ResponseHeaders: resp.Header.Clone(),
			ResponseBytes:   nativeHTTPResponseContentLength(resp),
			Err:             err,
			Duration:        time.Since(startedAt),
		},
	}
}

func nativeHTTPResponseContentLength(resp *http.Response) int64 {
	if resp == nil || resp.ContentLength < 0 {
		return 0
	}
	return resp.ContentLength
}

type nativeHTTPResponseSnapshot struct {
	statusCode int
	header     http.Header
	body       io.ReadCloser
	attempt    NativeRelayResult
}

func (s *nativeHTTPResponseSnapshot) close() {
	if s == nil || s.body == nil {
		return
	}
	_ = s.body.Close()
	s.body = nil
}

func (s *nativeHTTPResponseSnapshot) writeTo(w http.ResponseWriter) (bool, int64, error) {
	if s == nil {
		return false, 0, errors.New("native voice response snapshot is nil")
	}
	return writeNativeHTTPResponse(w, s.statusCode, s.header, s.body)
}

func (s *nativeHTTPResponseSnapshot) result(retry, committed bool, responseBytes int64, err error) NativeRelayResult {
	if s == nil {
		return NativeRelayResult{Err: err, Retry: retry, Committed: committed, ResponseBytes: responseBytes}
	}
	result := s.attempt
	result.Retry = retry
	result.Committed = committed
	result.ResponseBytes = responseBytes
	if err != nil {
		result.Err = err
	}
	return result
}

func writeNativeHTTPResponse(w http.ResponseWriter, statusCode int, header http.Header, body io.Reader) (bool, int64, error) {
	if w == nil {
		return false, 0, errors.New("native voice downstream writer is nil")
	}
	if body == nil {
		body = strings.NewReader("")
	}

	firstChunk, rest, readErr := readFirstChunk(body)
	if readErr != nil {
		return false, 0, readErr
	}

	copyNativeHTTPRelayResponseHeaders(w.Header(), header)
	w.WriteHeader(statusCode)
	committed := true
	var responseBytes int64

	if len(firstChunk) > 0 {
		written, err := w.Write(firstChunk)
		responseBytes += int64(written)
		if err != nil {
			return committed, responseBytes, err
		}
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	if rest != nil {
		written, err := io.Copy(flushWriter{w: w}, rest)
		responseBytes += written
		if err != nil {
			return committed, responseBytes, err
		}
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	return committed, responseBytes, nil
}

func readFirstChunk(body io.Reader) ([]byte, io.Reader, error) {
	if body == nil {
		return nil, nil, nil
	}

	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			return append([]byte(nil), buf[:n]...), body, nil
		}
		if err == io.EOF {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
	}
}

func copyNativeHTTPRelayResponseHeaders(dst, src http.Header) {
	sourceConnectionHeaders := nativeHTTPConnectionHeaderNames(src)
	destinationConnectionHeaders := nativeHTTPConnectionHeaderNames(dst)
	for key := range dst {
		canonical := http.CanonicalHeaderKey(key)
		if isNativeHTTPRelayResponseHopByHopHeader(canonical) ||
			isNativeHTTPRelayCredentialHeader(canonical) ||
			nativeHTTPHeaderNamedByConnection(sourceConnectionHeaders, canonical) ||
			nativeHTTPHeaderNamedByConnection(destinationConnectionHeaders, canonical) {
			dst.Del(key)
		}
	}
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if isNativeHTTPRelayResponseHopByHopHeader(canonical) ||
			isNativeHTTPRelayCredentialHeader(canonical) ||
			nativeHTTPHeaderNamedByConnection(sourceConnectionHeaders, canonical) {
			continue
		}
		dst.Del(canonical)
		dst[canonical] = append([]string(nil), values...)
	}
}

func nativeVoiceRequestIsStreaming(body []byte) bool {
	if len(body) == 0 {
		return false
	}

	var payload struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &payload) == nil && payload.Stream
}

func inspectNativeHTTPBusinessStatus(resp *http.Response, streamingRequest bool) error {
	if resp == nil {
		return errors.New("native voice business inspection requires a response")
	}

	if streamingRequest || strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		return inspectNativeHTTPStreamingBusinessStatus(resp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read business inspection body: %w", err)
	}
	resp.Body = &nativeHTTPReadCloser{Reader: bytes.NewReader(body), Closer: resp.Body}
	inspectionBody, err := nativeHTTPDecodedBusinessBody(body, resp.Header.Get("Content-Encoding"))
	if err != nil {
		return fmt.Errorf("failed to decode business inspection body: %w", err)
	}
	return inspectNativeHTTPBusinessPayload(inspectionBody, false)
}

const nativeVoiceBusinessInspectLimit = 64 << 10

func inspectNativeHTTPStreamingBusinessStatus(resp *http.Response) error {
	original := resp.Body
	var rawPrefix bytes.Buffer
	rawReader := io.TeeReader(original, &rawPrefix)
	inspectionReader := rawReader
	var gzipReader *gzip.Reader
	if nativeHTTPContentEncodingIsGzip(resp.Header.Get("Content-Encoding")) {
		var err error
		gzipReader, err = gzip.NewReader(rawReader)
		if err != nil {
			return fmt.Errorf("failed to initialize gzip business inspection: %w", err)
		}
		defer gzipReader.Close()
		inspectionReader = gzipReader
	}
	defer func() {
		resp.Body = &nativeHTTPReadCloser{
			Reader: io.MultiReader(bytes.NewReader(rawPrefix.Bytes()), original),
			Closer: original,
		}
	}()

	var prefix bytes.Buffer
	for prefix.Len() < nativeVoiceBusinessInspectLimit {
		chunk := make([]byte, 4096)
		n, err := inspectionReader.Read(chunk)
		if n > 0 {
			_, _ = prefix.Write(chunk[:n])
			if event, ok := firstNativeHTTPEvent(prefix.Bytes()); ok {
				return inspectNativeHTTPBusinessPayload(event, true)
			}
			if value, ok := firstNativeHTTPJSONValue(prefix.Bytes()); ok {
				return inspectNativeHTTPBusinessPayload(value, false)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read streaming business inspection body: %w", err)
		}
	}

	return inspectNativeHTTPBusinessPayload(prefix.Bytes(), strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream"))
}

func nativeHTTPContentEncodingIsGzip(value string) bool {
	encodings := strings.Split(value, ",")
	return len(encodings) == 1 && strings.EqualFold(strings.TrimSpace(encodings[0]), "gzip")
}

func nativeHTTPDecodedBusinessBody(body []byte, contentEncoding string) ([]byte, error) {
	if !nativeHTTPContentEncodingIsGzip(contentEncoding) {
		return body, nil
	}

	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func firstNativeHTTPEvent(body []byte) ([]byte, bool) {
	for len(body) > 0 {
		end, delimiterLen := -1, 0
		for _, delimiter := range [][]byte{[]byte("\n\n"), []byte("\r\n\r\n")} {
			if index := bytes.Index(body, delimiter); index >= 0 && (end < 0 || index < end) {
				end = index
				delimiterLen = len(delimiter)
			}
		}
		if end < 0 {
			return nil, false
		}

		event := body[:end]
		if len(nativeHTTPSSEData(event)) > 0 {
			return event, true
		}
		body = body[end+delimiterLen:]
	}
	return nil, false
}

func firstNativeHTTPJSONValue(body []byte) ([]byte, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	var value json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	return value, true
}

type nativeHTTPBusinessPayload struct {
	BaseResp *nativeHTTPBaseResp `json:"base_resp"`
}

type nativeHTTPBaseResp struct {
	StatusCode int    `json:"status_code"`
	StatusMsg  string `json:"status_msg"`
}

type nativeHTTPBusinessStatusError struct {
	statusCode int
	statusMsg  string
}

func (e *nativeHTTPBusinessStatusError) Error() string {
	if e == nil {
		return "native voice business failure"
	}
	return fmt.Sprintf("native voice business failure: status_code=%d status_msg=%s", e.statusCode, e.statusMsg)
}

func inspectNativeHTTPBusinessPayload(body []byte, sse bool) error {
	data := bytes.TrimSpace(body)
	if sse {
		data = nativeHTTPSSEData(data)
	}
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return nil
	}

	var payload nativeHTTPBusinessPayload
	if json.Unmarshal(data, &payload) == nil && payload.BaseResp != nil && payload.BaseResp.StatusCode != 0 {
		return &nativeHTTPBusinessStatusError{statusCode: payload.BaseResp.StatusCode, statusMsg: payload.BaseResp.StatusMsg}
	}
	return nil
}

func nativeHTTPSSEData(event []byte) []byte {
	lines := bytes.Split(event, []byte("\n"))
	data := make([]byte, 0, len(event))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		line = bytes.TrimSpace(line[len("data:"):])
		if len(line) == 0 {
			continue
		}
		if len(data) > 0 {
			data = append(data, '\n')
		}
		data = append(data, line...)
	}
	return bytes.TrimSpace(data)
}

type nativeHTTPReadCloser struct {
	io.Reader

	Closer io.Closer
}

func (r *nativeHTTPReadCloser) Close() error {
	if r == nil || r.Closer == nil {
		return nil
	}
	return r.Closer.Close()
}

type flushWriter struct {
	w http.ResponseWriter
}

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}
