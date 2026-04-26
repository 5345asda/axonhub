package anthropic

import "context"

type AnthropicSignatureMode string

const (
	AnthropicSignatureModeTransport   AnthropicSignatureMode = "transport"
	AnthropicSignatureModePassthrough AnthropicSignatureMode = "passthrough"
)

type anthropicSignatureModeContextKey struct{}

func ContextWithSignatureMode(ctx context.Context, mode AnthropicSignatureMode) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, anthropicSignatureModeContextKey{}, mode)
}

func signatureModeFromContext(ctx context.Context) (AnthropicSignatureMode, bool) {
	if ctx == nil {
		return "", false
	}

	mode, ok := ctx.Value(anthropicSignatureModeContextKey{}).(AnthropicSignatureMode)
	if !ok {
		return "", false
	}

	switch mode {
	case AnthropicSignatureModeTransport, AnthropicSignatureModePassthrough:
		return mode, true
	default:
		return "", false
	}
}

func resolveSignatureMode(ctx context.Context, config *Config) AnthropicSignatureMode {
	if mode, ok := signatureModeFromContext(ctx); ok {
		return mode
	}

	if config != nil {
		switch config.SignatureMode {
		case AnthropicSignatureModeTransport, AnthropicSignatureModePassthrough:
			return config.SignatureMode
		}
	}

	return AnthropicSignatureModeTransport
}
