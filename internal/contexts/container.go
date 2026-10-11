package contexts

import (
	"context"
	"slices"
	"sync"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/request"
)

// contextContainer contains all values in the context.
type contextContainer struct {
	ProjectID     *int
	TraceID       *string
	RequestID     *string
	OperationName *string
	APIKey        *ent.APIKey
	User          *ent.User
	Source        *request.Source
	Thread        *ent.Thread
	Trace         *ent.Trace
	Errors        []error
	mu            sync.RWMutex

	// ChannelAPIKey stores the API key used for the channel request (not the user's API key)
	ChannelAPIKey *string
}

// getContainer retrieves the existing container from context, or creates a new one and stores it in the context if it doesn't exist.
func getContainer(ctx context.Context) *contextContainer {
	if container, ok := ctx.Value(containerContextKey).(*contextContainer); ok {
		return container
	}

	// If container doesn't exist, create a new one and store it in the context
	container := &contextContainer{}

	return container
}

// withContainer stores the container in the context (if not already stored).
func withContainer(ctx context.Context, container *contextContainer) context.Context {
	if ctx.Value(containerContextKey) == nil {
		return context.WithValue(ctx, containerContextKey, container)
	}

	return ctx
}

// Clone returns a context with an independent request-scoped value container.
// This is required when concurrent work derives different trace/session values
// from the same parent context, such as multiplexed WebSocket streams.
func Clone(ctx context.Context) context.Context {
	source := getContainer(ctx)
	source.mu.RLock()
	clone := &contextContainer{
		ProjectID:     source.ProjectID,
		TraceID:       source.TraceID,
		RequestID:     source.RequestID,
		OperationName: source.OperationName,
		APIKey:        source.APIKey,
		User:          source.User,
		Source:        source.Source,
		Thread:        source.Thread,
		Trace:         source.Trace,
		Errors:        slices.Clone(source.Errors),
		ChannelAPIKey: source.ChannelAPIKey,
	}
	source.mu.RUnlock()

	return context.WithValue(ctx, containerContextKey, clone)
}
