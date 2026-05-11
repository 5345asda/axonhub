package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm/httpclient"
)

type ArchivedChannelCandidate struct {
	ID               objects.GUID
	Name             string
	DefaultTestModel string
}

type ArchivedChannelLister interface {
	ListArchivedChannels(ctx context.Context) ([]ArchivedChannelCandidate, error)
}

type ArchivedChannelTester interface {
	TestChannel(ctx context.Context, channelID objects.GUID, modelID *string, proxy *httpclient.ProxyConfig) (*TestChannelResult, error)
}

type ArchivedChannelRecoverer interface {
	BulkRecoverChannels(ctx context.Context, ids []int) error
}

type ArchivedChannelDeleter interface {
	BulkDeleteChannels(ctx context.Context, ids []int) error
}

type RunArchivedChannelRecoveryInput struct {
	ModelID              *string
	Proxy                *httpclient.ProxyConfig
	DryRun               bool
	DeleteFailureReasons []string
	Concurrency          int
}

type ArchivedChannelRecoveryItem struct {
	ChannelID     objects.GUID
	Name          string
	ModelID       string
	Success       bool
	Recovered     bool
	DeleteMatched bool
	Deleted       bool
	Latency       float64
	Message       *string
	Error         *string
	FailureReason *string
}

type ArchivedChannelRecoveryResult struct {
	Total               int
	Tested              int
	Succeeded           int
	Failed              int
	Recoverable         int
	Recovered           int
	DeleteCandidates    int
	Deleted             int
	DryRun              bool
	Items               []*ArchivedChannelRecoveryItem
	FailureReasonCounts map[string]int
	DeletedReasonCounts map[string]int
}

type ArchivedChannelRecoveryRunner struct {
	lister    ArchivedChannelLister
	tester    ArchivedChannelTester
	recoverer ArchivedChannelRecoverer
	deleter   ArchivedChannelDeleter
}

func NewArchivedChannelRecoveryRunner(
	lister ArchivedChannelLister,
	tester ArchivedChannelTester,
	recoverer ArchivedChannelRecoverer,
	deleter ArchivedChannelDeleter,
) *ArchivedChannelRecoveryRunner {
	return &ArchivedChannelRecoveryRunner{
		lister:    lister,
		tester:    tester,
		recoverer: recoverer,
		deleter:   deleter,
	}
}

func (r *ArchivedChannelRecoveryRunner) Run(
	ctx context.Context,
	input RunArchivedChannelRecoveryInput,
) (*ArchivedChannelRecoveryResult, error) {
	channels, err := r.lister.ListArchivedChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list archived channels: %w", err)
	}

	result := &ArchivedChannelRecoveryResult{
		Total:               len(channels),
		DryRun:              input.DryRun,
		Items:               make([]*ArchivedChannelRecoveryItem, len(channels)),
		FailureReasonCounts: make(map[string]int),
		DeletedReasonCounts: make(map[string]int),
	}
	deleteReasons := normalizeArchivedChannelFailureReasonSet(input.DeleteFailureReasons)

	if len(channels) == 0 {
		result.Items = compactArchivedChannelRecoveryItems(result.Items)
		return result, nil
	}

	concurrency := input.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency == 1 {
		for index, candidate := range channels {
			item, outcome, err := r.executeArchivedChannelCandidate(ctx, candidate, input, deleteReasons)
			result.Items[index] = item
			mergeArchivedChannelRecoveryOutcome(result, outcome)
			if err != nil {
				result.Items = compactArchivedChannelRecoveryItems(result.Items)
				return result, err
			}
		}

		result.Items = compactArchivedChannelRecoveryItems(result.Items)
		return result, nil
	}

	if concurrency > len(channels) {
		concurrency = len(channels)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)

	var mu sync.Mutex

	for index, candidate := range channels {
		if groupCtx.Err() != nil {
			break
		}

		index := index
		candidate := candidate

		group.Go(func() error {
			item, outcome, err := r.executeArchivedChannelCandidate(groupCtx, candidate, input, deleteReasons)

			mu.Lock()
			result.Items[index] = item
			mergeArchivedChannelRecoveryOutcome(result, outcome)
			mu.Unlock()

			return err
		})
	}

	if err := group.Wait(); err != nil {
		result.Items = compactArchivedChannelRecoveryItems(result.Items)
		return result, err
	}

	result.Items = compactArchivedChannelRecoveryItems(result.Items)
	return result, nil
}

type archivedChannelRecoveryOutcome struct {
	Tested           int
	Succeeded        int
	Failed           int
	Recoverable      int
	Recovered        int
	DeleteCandidates int
	Deleted          int
	FailureReason    string
	DeletedReason    string
}

func (r *ArchivedChannelRecoveryRunner) executeArchivedChannelCandidate(
	ctx context.Context,
	candidate ArchivedChannelCandidate,
	input RunArchivedChannelRecoveryInput,
	deleteReasons map[string]struct{},
) (*ArchivedChannelRecoveryItem, archivedChannelRecoveryOutcome, error) {
	item := &ArchivedChannelRecoveryItem{
		ChannelID: candidate.ID,
		Name:      candidate.Name,
		ModelID:   resolveArchivedChannelRecoveryModelID(candidate, input.ModelID),
	}
	outcome := archivedChannelRecoveryOutcome{
		Tested: 1,
	}

	testResult, testErr := r.tester.TestChannel(ctx, candidate.ID, input.ModelID, input.Proxy)
	if testErr != nil {
		outcome.Failed = 1
		item.Error = lo.ToPtr(testErr.Error())
		item.FailureReason = lo.ToPtr(NormalizeArchivedChannelFailureReason(testErr.Error()))
		outcome.FailureReason = lo.FromPtr(item.FailureReason)
		return r.applyDeleteMatchedFailedChannel(ctx, item, candidate, input, deleteReasons, outcome)
	}

	if testResult == nil {
		outcome.Failed = 1
		item.Error = lo.ToPtr("test result is nil")
		item.FailureReason = lo.ToPtr("test result is nil")
		outcome.FailureReason = lo.FromPtr(item.FailureReason)
		return r.applyDeleteMatchedFailedChannel(ctx, item, candidate, input, deleteReasons, outcome)
	}

	item.Latency = testResult.Latency
	item.Success = testResult.Success
	item.Message = testResult.Message
	item.Error = testResult.Error

	if !testResult.Success {
		outcome.Failed = 1
		item.FailureReason = lo.ToPtr(NormalizeArchivedChannelFailureReason(lo.FromPtr(testResult.Error)))
		outcome.FailureReason = lo.FromPtr(item.FailureReason)
		return r.applyDeleteMatchedFailedChannel(ctx, item, candidate, input, deleteReasons, outcome)
	}

	outcome.Succeeded = 1
	outcome.Recoverable = 1
	if input.DryRun {
		return item, outcome, nil
	}

	if r.recoverer == nil {
		return item, outcome, fmt.Errorf("archived channel recoverer is not configured")
	}

	if err := r.recoverer.BulkRecoverChannels(ctx, []int{candidate.ID.ID}); err != nil {
		return item, outcome, fmt.Errorf("failed to recover channels: %w", err)
	}

	item.Recovered = true
	outcome.Recovered = 1

	return item, outcome, nil
}

func (r *ArchivedChannelRecoveryRunner) applyDeleteMatchedFailedChannel(
	ctx context.Context,
	item *ArchivedChannelRecoveryItem,
	candidate ArchivedChannelCandidate,
	input RunArchivedChannelRecoveryInput,
	deleteReasons map[string]struct{},
	outcome archivedChannelRecoveryOutcome,
) (*ArchivedChannelRecoveryItem, archivedChannelRecoveryOutcome, error) {
	if len(deleteReasons) == 0 {
		return item, outcome, nil
	}

	failureReason := lo.FromPtr(item.FailureReason)
	if failureReason == "" {
		return item, outcome, nil
	}

	if _, ok := deleteReasons[failureReason]; !ok {
		return item, outcome, nil
	}

	item.DeleteMatched = true
	outcome.DeleteCandidates = 1

	if input.DryRun {
		return item, outcome, nil
	}

	if r.deleter == nil {
		return item, outcome, fmt.Errorf("archived channel deleter is not configured")
	}

	if err := r.deleter.BulkDeleteChannels(ctx, []int{candidate.ID.ID}); err != nil {
		return item, outcome, fmt.Errorf("failed to delete channels: %w", err)
	}

	item.Deleted = true
	outcome.Deleted = 1
	outcome.DeletedReason = failureReason

	return item, outcome, nil
}

func normalizeArchivedChannelFailureReasonSet(reasons []string) map[string]struct{} {
	if len(reasons) == 0 {
		return map[string]struct{}{}
	}

	normalized := make(map[string]struct{}, len(reasons))
	for _, reason := range reasons {
		value := NormalizeArchivedChannelFailureReason(reason)
		if value == "" {
			continue
		}

		normalized[value] = struct{}{}
	}

	return normalized
}

func recordArchivedChannelFailureReason(counts map[string]int, reason string) {
	if strings.TrimSpace(reason) == "" {
		return
	}

	counts[reason]++
}

func resolveArchivedChannelRecoveryModelID(candidate ArchivedChannelCandidate, override *string) string {
	modelID := lo.FromPtr(override)
	if modelID != "" {
		return modelID
	}

	return candidate.DefaultTestModel
}

func mergeArchivedChannelRecoveryOutcome(
	result *ArchivedChannelRecoveryResult,
	outcome archivedChannelRecoveryOutcome,
) {
	result.Tested += outcome.Tested
	result.Succeeded += outcome.Succeeded
	result.Failed += outcome.Failed
	result.Recoverable += outcome.Recoverable
	result.Recovered += outcome.Recovered
	result.DeleteCandidates += outcome.DeleteCandidates
	result.Deleted += outcome.Deleted
	recordArchivedChannelFailureReason(result.FailureReasonCounts, outcome.FailureReason)
	recordArchivedChannelFailureReason(result.DeletedReasonCounts, outcome.DeletedReason)
}

func compactArchivedChannelRecoveryItems(items []*ArchivedChannelRecoveryItem) []*ArchivedChannelRecoveryItem {
	return lo.Filter(items, func(item *ArchivedChannelRecoveryItem, _ int) bool {
		return item != nil
	})
}
