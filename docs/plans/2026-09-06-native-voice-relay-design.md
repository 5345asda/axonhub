# Native Voice Relay Design

AxonHub2 is a protocol-preserving relay, not a speech protocol converter.
Existing channels retain provider credentials and opt into native routes by
adding exact `ChannelEndpoint` records. XiaoZhi sends the provider-native path
to AxonHub2 and receives the provider-native response unchanged.

## Data Flow

```text
XiaoZhi request
  -> existing API-key middleware
  -> exact method/path/transport registry lookup
  -> same-protocol ChannelEndpoint candidate filter
  -> existing LoadBalancer ordering
  -> provider-native HTTP or WebSocket client
```

The registry is the only owner of native path and authentication metadata.
Caller input cannot define an arbitrary path or upstream header. The relay
removes downstream credential and hop-by-hop headers, then injects the selected
channel's bearer key (Bailian/MiniMax) or `X-Api-Key` and `X-Api-Resource-Id`
(Doubao V3). A blank endpoint `BaseURL` resolves to the registry's native
provider default rather than the channel's LLM `BaseURL`; a voice proxy or
regional authority must be configured explicitly on the endpoint.

## Routing Rules

- HTTP, SSE, and chunked formats preserve method, business query, body, response
  headers, and streaming bytes. Known API-key query names are removed before
  the provider request. Upstream HTTP redirects are rejected before a provider
  credential can reach another authority.
- WebSocket formats are dialed upstream before the downstream upgrade. Text and
  binary frames are forwarded unchanged. Each ping receives a local pong;
  downstream pings are forwarded upstream, while upstream pings are answered
  locally to avoid a loop. Only upstream Pong responses are returned to the
  downstream client; downstream Pong frames are not reflected. A provider-
  selected `Sec-WebSocket-Protocol` is echoed in the downstream handshake.
  Close frames are forwarded both ways.
- Candidate fallback is confined to one registered `api_format`; native
  selection keeps the complete ordered candidate list instead of the ordinary
  LLM retry-policy `topK`, and it never crosses provider or transport.
- Bailian's shared realtime paths require an unambiguous configured format or
  explicit model protocol mapping. Names such as `asr` or `tts` are not routing
  signals.
- A native WebSocket request whose model is carried inside an opaque provider
  frame is relayed without rewriting that frame. Endpoint/model configuration
  must therefore make the candidate pool unambiguous before the frame arrives.
- A generic WebSocket `?model=` query is retained only for the upstream relay;
  it is not authoritative for candidate or Profile authorization. WebSocket
  selection remains opaque until the provider frame is accepted.
- A visible model must be an exact direct provider model. When an opaque
  WebSocket is constrained by a Profile model list, every direct model declared
  by the candidate channel must be in that list before the handshake proceeds.
  LLM aliases, prefixes, automatic trimming, and case normalization are
  intentionally outside the transparent relay contract.

## Commit Boundary

An HTTP candidate can be replaced only before downstream response bytes are
committed. MiniMax T2A HTTP treats a non-zero `base_resp.status_code` as a
pre-commit provider failure, including the first SSE event. Once an HTTP byte is
written, the session is terminal.

For WebSocket, only a failed upstream handshake is retryable. Once a provider
accepts the handshake, all later errors close that session; no audio or event is
replayed through another channel.

## Explicit Non-Goals

No new profile or speech-specific channel, settings namespace, database schema,
credential envelope, custom XiaoZhi protocol, codec conversion, asynchronous
task API, or mid-session provider migration is part of this design.

The supported path list and verification commands are maintained in
`2026-09-06-native-voice-relay.md` and the implementation registry in
`internal/objects/native_voice.go`.
