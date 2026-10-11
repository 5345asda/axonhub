package api

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/tracing"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/shared"
)

const apiKeyModelTracePersistenceTimeout = 2 * time.Second

func newAPIKeyModelTraceBinder(config tracing.Config, traceService *biz.TraceService) responsesWebSocketTraceBinder {
	if !config.APIKeyModelSticky {
		return nil
	}

	return func(ctx context.Context, request *httpclient.Request) (context.Context, error) {
		// A Responses WebSocket can process frames concurrently. The normal request
		// context carries a mutable value container, so isolate it before attaching
		// a model-specific trace to this individual response.create event.
		ctx = contexts.Clone(ctx)

		traceID, err := tracing.APIKeyModelTraceID(ctx, modelFromResponsesWebSocketRequest(request), config.APIKeyModelStickySecret)
		if err != nil || traceID == "" {
			return ctx, err
		}

		ctx = tracing.WithTraceID(ctx, traceID)
		// The upgrade request can have a client-provided trace already. Do not
		// let that stale entity win routing if the HMAC trace cannot be loaded.
		ctx = contexts.WithTrace(ctx, nil)
		ctx = shared.WithSessionID(ctx, traceID)

		projectID, ok := contexts.GetProjectID(ctx)
		if !ok || traceService == nil {
			return ctx, nil
		}

		var threadID *int
		if thread, ok := contexts.GetThread(ctx); ok && thread != nil {
			threadID = &thread.ID
		}

		// Trace persistence is auxiliary to inference. Keep the event context's
		// values, but do not let a slow database return a canceled context to the
		// orchestrator. Preserve a shorter event deadline when one is configured.
		persistCtx, cancel := apiKeyModelTracePersistenceContext(ctx)
		defer cancel()
		trace, err := traceService.GetOrCreateTrace(
			authz.WithSystemBypass(persistCtx, "responses-websocket-api-key-model-sticky"),
			projectID,
			traceID,
			threadID,
		)
		if err != nil {
			// Match the HTTP trace middleware: tracing persistence must not make a
			// valid inference request fail when storage is temporarily unavailable.
			log.Warn(ctx, "Failed to get or create Responses WebSocket trace", log.Cause(err))
			return ctx, nil
		}

		return contexts.WithTrace(ctx, trace), nil
	}
}

func apiKeyModelTracePersistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(apiKeyModelTracePersistenceTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}

	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

func modelFromResponsesWebSocketRequest(request *httpclient.Request) string {
	if request == nil || len(request.Body) == 0 {
		return ""
	}

	var payload struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		return ""
	}

	return strings.TrimSpace(payload.Model)
}
