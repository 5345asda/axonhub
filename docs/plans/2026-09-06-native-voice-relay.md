# Native Voice Relay

This document is the current contract for the provider-native voice relay.

## Boundaries

- XiaoZhi authenticates to AxonHub2 with its AxonHub2 API key only.
- Provider keys stay in the existing Channel credentials JSON.
- Native routing is opt-in through existing `ChannelEndpoint` records. No new
  channel, profile, database table, migration, or credential type is added.
- Only registered runtime HTTP, SSE, chunked, and WebSocket routes are exposed.
  Asynchronous submit, query, poll, callback, file, and voice-management APIs
  are not exposed.
- The relay preserves provider-native paths, business query parameters, bodies,
  response payloads, and WebSocket frames. Known API-key query names are
  removed at the boundary; credentials come from the selected channel. It does
  not transcode, normalize audio, or infer token usage from opaque native
  payloads.

## Registered Formats

The `api_format` value is the protocol selector. The registry owns the exact
method, path, transport, upstream default, and authentication mode.

```text
bailian/asr_realtime           GET  /api-ws/v1/realtime                          websocket
bailian/asr_inference          GET  /api-ws/v1/inference                         websocket
bailian/tts_realtime           GET  /api-ws/v1/realtime                          websocket
bailian/tts_inference          GET  /api-ws/v1/inference                         websocket
bailian/tts                  POST  /api/v1/services/audio/tts/SpeechSynthesizer  http
bailian/multimodal_generation POST /api/v1/services/aigc/multimodal-generation/generation http
minimax/t2a_v2               POST /v1/t2a_v2                                    http
minimax/t2a_v2_ws             GET /ws/v1/t2a_v2                                 websocket
minimax/t2a_v2_bidi            GET /ws/v1/t2a_v2_bidi                            websocket
doubao/asr_bidi               GET /api/v3/sauc/bigmodel_async                  websocket
doubao/asr_nostream           GET /api/v3/sauc/bigmodel_nostream              websocket
doubao/asr                    GET /api/v3/sauc/bigmodel                       websocket
doubao/tts_bidi               GET /api/v3/tts/bidirection                     websocket
doubao/tts_ws                 GET /api/v3/tts/unidirectional/stream            websocket
doubao/tts                    POST /api/v3/tts/unidirectional                 http
doubao/tts_sse                POST /api/v3/tts/unidirectional/sse              http
```

The existing production MiniMax `minimax/t2a_v2` endpoint remains the same
HTTP path and bearer-key contract.

## Channel Endpoint Contract

Native records use the existing fields:

```json
{
  "api_format": "minimax/t2a_v2",
  "path": "/v1/t2a_v2",
  "base_url": "https://api.minimax.cn",
  "transport": "http",
  "resource_id": ""
}
```

`path` and `transport` must exactly match the registry. A blank `base_url`
uses the registry's native provider default; it never inherits the channel's
LLM `BaseURL`. An explicit `base_url` may select a documented region or
operator-controlled authority, but cannot contain userinfo, query, or fragment.
Doubao V3 records require `resource_id`; other native records reject it. A
blank transport is accepted only as the existing HTTP endpoint default; native
WebSocket records must be explicit.

Doubao entries use the V3 new-console credential contract: `X-Api-Key` plus
`X-Api-Resource-Id`. Legacy clients that send `X-Api-App-Key` and
`X-Api-Access-Key` and embed the provider token in binary frames are not
rewritten by this transparent relay; XiaoZhi must use the V3 header contract
for these entries.

## Selection And Failure

Enabled channels are filtered by exact `api_format`, endpoint path/transport,
channel model entries, active API-key credentials, profile scope, and Doubao
resource ID. `ModelProtocols` can explicitly force a format for a model. A
shared Bailian path is rejected when more than one configured protocol remains
possible. A single `model` query value may match an exact `ModelProtocols`
entry only to disambiguate that path; it is never used for Profile
authorization or guessed from a model-name substring.

When a model is observable before routing, native selection accepts only an
exact `SupportedModels` value. Channel model aliases, prefixes, automatic
trimming, and case normalization are LLM request transformations and are not
applied to opaque native payloads. If a WebSocket model appears only in its
first frame and the active Profile limits models, a candidate is eligible only
when every directly configured channel model belongs to that Profile. This
keeps the Profile boundary enforceable without inspecting a provider frame.
For a generic WebSocket route, a `?model=` query is retained for upstream
relay. On a shared Bailian path it may also select an exact configured
`ModelProtocols` protocol, but it is never authoritative for candidate model
or Profile authorization; selection remains opaque until the provider frame
arrives.

The existing load-balancer ordering is reused after this filtering, without the
ordinary LLM retry-policy `topK` truncation: native failover needs the complete
same-format candidate order. HTTP connect, response-status, and MiniMax
`base_resp.status_code != 0` failures may try the next same-format candidate
before downstream response bytes are written.
HTTP redirects are rejected rather than followed with injected provider
credentials. Final upstream failures return a generic gateway error, so a
business query or provider error detail cannot be reflected to XiaoZhi.
After the first response byte, no HTTP retry is allowed. WebSocket failover is
limited to upstream handshake failures; after handshake acceptance, the session
stays on that connection and application frames are never replayed elsewhere.
WebSocket ping frames receive a local pong; downstream pings are also forwarded
to the provider, while upstream pings are answered locally to avoid a control
frame loop. Only upstream Pong responses are returned to the downstream client;
downstream Pong frames are not reflected. If the provider selects a declared
`Sec-WebSocket-Protocol`, that exact value is returned in the downstream
handshake. Close frames are forwarded both ways.

## Verification

Use local HTTP and WebSocket fixtures only:

```text
go test ./internal/server/voice ./internal/server/api ./internal/objects ./internal/server/biz ./internal/server/gql -count=1
Set-Location llm; go test ./transformer/bailian ./transformer/doubao -count=1
```

Do not run a build or lint as part of this slice, and do not restart a managed
service.
