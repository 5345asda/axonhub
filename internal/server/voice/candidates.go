package voice

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
)

// ChannelProviderFn supplies the enabled channels that participate in native
// voice routing.
type ChannelProviderFn func() []*biz.Channel

// CandidateSortFn applies the existing channel load-balancer ordering after
// filtering candidates to one native protocol.
type CandidateSortFn func(ctx context.Context, candidates []*orchestrator.ChannelModelsCandidate, model string, stream bool) []*orchestrator.ChannelModelsCandidate

type CandidateRequest struct {
	APIFormat   string
	Model       string
	ResourceID  string
	Stream      bool
	OpaqueModel bool
}

type Candidate struct {
	Channel  *biz.Channel
	Endpoint objects.ChannelEndpoint
	Protocol objects.NativeVoiceProtocol
	Model    biz.ChannelModelEntry
}

type CandidateSelector struct {
	channels ChannelProviderFn
	sorter   CandidateSortFn
}

func NewCandidateSelector(channels ChannelProviderFn, sorter CandidateSortFn) *CandidateSelector {
	return &CandidateSelector{channels: channels, sorter: sorter}
}

// Select returns only candidates configured for the exact native API format.
// Fallback is therefore confined to one provider wire protocol.
func (s *CandidateSelector) Select(ctx context.Context, req CandidateRequest) ([]*Candidate, error) {
	protocol, ok := objects.NativeVoiceProtocolByAPIFormat(req.APIFormat)
	if !ok {
		return nil, fmt.Errorf("unsupported native voice api_format %q", req.APIFormat)
	}
	if s == nil || s.channels == nil {
		return nil, nil
	}

	candidates := s.collectCandidates(ctx, protocol, req)
	if len(candidates) == 0 || s.sorter == nil {
		return candidates, nil
	}

	return s.sortCandidates(ctx, candidates, req), nil
}

func (s *CandidateSelector) collectCandidates(ctx context.Context, protocol objects.NativeVoiceProtocol, req CandidateRequest) []*Candidate {
	var candidates []*Candidate
	for _, ch := range s.channels() {
		if ch == nil || ch.Channel == nil || !nativeVoiceProfileAllows(ctx, ch, req.Model, req.OpaqueModel) {
			continue
		}

		entry, ok := resolveNativeVoiceModelEntry(ch.GetModelEntries(), req.Model)
		if !ok || !channelHasNativeVoiceAuth(ch, protocol) {
			continue
		}

		for _, endpoint := range ch.ResolveEndpoints() {
			if !endpointMatches(protocol, endpoint, req) {
				continue
			}
			if forced := ch.ForcedAPIFormats(req.Model); len(forced) > 0 && !slices.Contains(forced, endpoint.APIFormat) {
				continue
			}
			candidates = append(candidates, &Candidate{
				Channel:  ch,
				Endpoint: endpoint,
				Protocol: protocol,
				Model:    entry,
			})
		}
	}
	return candidates
}

func resolveNativeVoiceModelEntry(entries map[string]biz.ChannelModelEntry, requestModel string) (biz.ChannelModelEntry, bool) {
	if requestModel == "" {
		// Native frames are relayed unchanged. When the provider declares its
		// model after the WebSocket handshake, selection must not invent one.
		return biz.ChannelModelEntry{}, true
	}

	entry, ok := entries[requestModel]
	// Aliases, prefixes, and case normalization require payload rewriting,
	// which a native relay deliberately does not perform.
	return entry, ok && entry.Source == "direct" && entry.ActualModel == requestModel
}

func endpointMatches(protocol objects.NativeVoiceProtocol, endpoint objects.ChannelEndpoint, req CandidateRequest) bool {
	if endpoint.APIFormat != protocol.APIFormat || endpoint.Path != protocol.Path {
		return false
	}
	if objects.NativeVoiceEndpointTransport(endpoint) != protocol.Transport {
		return false
	}
	return req.ResourceID == "" || strings.TrimSpace(endpoint.ResourceID) == req.ResourceID
}

func channelHasNativeVoiceAuth(ch *biz.Channel, protocol objects.NativeVoiceProtocol) bool {
	if ch == nil || ch.Channel == nil {
		return false
	}

	switch protocol.AuthMode {
	case objects.NativeVoiceAuthMiniMax, objects.NativeVoiceAuthDashScope, objects.NativeVoiceAuthDoubaoV3:
		keys := ch.GetEnabledAPIKeys()
		if len(keys) == 0 {
			keys = ch.Credentials.GetEnabledAPIKeys(ch.DisabledAPIKeys)
		}
		return len(keys) > 0
	default:
		return false
	}
}

func nativeVoiceProfileAllows(ctx context.Context, ch *biz.Channel, model string, opaqueModel bool) bool {
	apiKey, ok := contexts.GetAPIKey(ctx)
	if !ok || apiKey == nil {
		return true
	}

	if project := apiKey.Edges.Project; project != nil {
		if profile := project.GetActiveProfile(); profile != nil && !nativeVoiceChannelScopeAllows(ch, profile.ChannelIDs, profile.ChannelTags, profile.ChannelTagsMatchMode) {
			return false
		}
	}

	if profile := apiKey.GetActiveProfile(); profile != nil {
		if !nativeVoiceChannelScopeAllows(ch, profile.ChannelIDs, profile.ChannelTags, profile.ChannelTagsMatchMode) {
			return false
		}
		if len(profile.ModelIDs) > 0 {
			if model != "" {
				if !slices.Contains(profile.ModelIDs, model) {
					return false
				}
			} else if !opaqueModel || !nativeVoiceOpaqueModelProfileAllows(ch, profile.ModelIDs) {
				// The first WebSocket frame is opaque. Only a channel whose direct
				// configured models are all inside the profile can be selected before
				// the model becomes observable.
				return false
			}
		}
	}

	return true
}

func nativeVoiceOpaqueModelProfileAllows(ch *biz.Channel, models []string) bool {
	if ch == nil || ch.Channel == nil {
		return false
	}

	entries := ch.GetDirectModelEntries()
	if len(entries) == 0 {
		return false
	}
	for _, entry := range entries {
		if !slices.Contains(models, entry.ActualModel) {
			return false
		}
	}

	return true
}

func nativeVoiceChannelScopeAllows(ch *biz.Channel, ids []int, tags []string, matchMode objects.ChannelTagsMatchMode) bool {
	if ch == nil || ch.Channel == nil {
		return false
	}
	if len(ids) > 0 && !slices.Contains(ids, ch.ID) {
		return false
	}
	return len(tags) == 0 || objects.MatchChannelTags(tags, matchMode, ch.Tags)
}

func (s *CandidateSelector) sortCandidates(ctx context.Context, candidates []*Candidate, req CandidateRequest) []*Candidate {
	lbCandidates := make([]*orchestrator.ChannelModelsCandidate, 0, len(candidates))
	indexByCandidate := make(map[*orchestrator.ChannelModelsCandidate]int, len(candidates))
	for i, candidate := range candidates {
		lbCandidate := &orchestrator.ChannelModelsCandidate{
			Channel:   candidate.Channel,
			Models:    []biz.ChannelModelEntry{candidate.Model},
			APIFormat: candidate.Protocol.APIFormat,
		}
		lbCandidates = append(lbCandidates, lbCandidate)
		indexByCandidate[lbCandidate] = i
	}

	sorted := s.sorter(ctx, lbCandidates, req.Model, req.Stream)
	if len(sorted) == 0 {
		return candidates
	}

	result := make([]*Candidate, 0, len(sorted))
	seen := make(map[int]struct{}, len(sorted))
	for _, lbCandidate := range sorted {
		idx, ok := indexByCandidate[lbCandidate]
		if !ok {
			continue
		}
		if _, duplicate := seen[idx]; duplicate {
			continue
		}
		seen[idx] = struct{}{}
		result = append(result, candidates[idx])
	}

	if len(result) == 0 {
		return candidates
	}
	return result
}
