package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/project"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/tracing"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/shared"
)

func TestAPIKeyModelTracePersistenceContextPreservesEarlierEventDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	parent, cancelParent := context.WithDeadline(context.Background(), deadline)
	t.Cleanup(cancelParent)

	persistCtx, cancelPersist := apiKeyModelTracePersistenceContext(parent)
	t.Cleanup(cancelPersist)

	persistDeadline, ok := persistCtx.Deadline()
	require.True(t, ok)
	require.True(t, persistDeadline.Equal(deadline))

	cancelParent()
	require.NoError(t, persistCtx.Err())
}

func TestAPIKeyModelTraceBinderIsolatesConcurrentResponseModels(t *testing.T) {
	binder := newAPIKeyModelTraceBinder(tracing.Config{
		APIKeyModelSticky:       true,
		APIKeyModelStickySecret: "test-sticky-secret",
	}, nil)
	parent := contexts.WithAPIKey(context.Background(), &ent.APIKey{Key: "sk-websocket-key"})
	parent = contexts.WithTrace(parent, &ent.Trace{ID: 99, TraceID: "client-provided-trace"})

	bind := func(model string) context.Context {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		request := &httpclient.Request{
			Body:       []byte(`{"model":"` + model + `"}`),
			RawRequest: req,
		}
		bound, err := binder(parent, request)
		require.NoError(t, err)
		traceID, ok := tracing.GetTraceID(bound)
		require.True(t, ok)
		_, hasTrace := contexts.GetTrace(bound)
		require.False(t, hasTrace)
		sessionID, ok := shared.GetSessionID(bound)
		require.True(t, ok)
		require.Equal(t, traceID, sessionID)
		return bound
	}

	first := bind("gpt-first")
	second := bind("gpt-second")
	firstID, _ := tracing.GetTraceID(first)
	secondID, _ := tracing.GetTraceID(second)
	require.NotEqual(t, firstID, secondID)
	_, parentHasTrace := tracing.GetTraceID(parent)
	require.False(t, parentHasTrace)
	_, parentHasSession := shared.GetSessionID(parent)
	require.False(t, parentHasSession)
	parentTrace, parentHasTrace := contexts.GetTrace(parent)
	require.True(t, parentHasTrace)
	require.Equal(t, "client-provided-trace", parentTrace.TraceID)
}

func TestAPIKeyModelTraceBinderReturnsConfigurationErrorForMissingSecret(t *testing.T) {
	binder := newAPIKeyModelTraceBinder(tracing.Config{APIKeyModelSticky: true}, nil)
	ctx := contexts.WithAPIKey(context.Background(), &ent.APIKey{Key: "sk-websocket-key"})
	request := &httpclient.Request{Body: []byte(`{"model":"gpt-test"}`)}

	bound, err := binder(ctx, request)
	require.Error(t, err)
	require.NotNil(t, bound)
}

func TestAPIKeyModelTraceBinderPersistsModelTrace(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:responses-websocket-sticky-trace?mode=memory&_fk=1")
	t.Cleanup(func() { _ = client.Close() })

	ctx := ent.NewContext(authz.WithTestBypass(context.Background()), client)
	projectEntity, err := client.Project.Create().
		SetName("responses-websocket-sticky-trace").
		SetStatus(project.StatusActive).
		Save(ctx)
	require.NoError(t, err)

	ctx = contexts.WithProjectID(ctx, projectEntity.ID)
	ctx = contexts.WithAPIKey(ctx, &ent.APIKey{Key: "sk-websocket-key"})
	binder := newAPIKeyModelTraceBinder(tracing.Config{
		APIKeyModelSticky:       true,
		APIKeyModelStickySecret: "test-sticky-secret",
	}, biz.NewTraceService(biz.TraceServiceParams{Ent: client}))

	bound, err := binder(ctx, &httpclient.Request{Body: []byte(`{"model":"gpt-test"}`)})
	require.NoError(t, err)
	traceID, ok := tracing.GetTraceID(bound)
	require.True(t, ok)
	traceEntity, ok := contexts.GetTrace(bound)
	require.True(t, ok)
	require.Equal(t, traceID, traceEntity.TraceID)
	require.Equal(t, projectEntity.ID, traceEntity.ProjectID)
}
