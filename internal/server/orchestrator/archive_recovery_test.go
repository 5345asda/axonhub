package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm/httpclient"
)

func TestArchivedChannelRecoveryRunner_Run_RecoversSuccessfulChannels(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 1}, Name: "Archived 1", DefaultTestModel: "gpt-4o-mini"},
			{ID: objects.GUID{Type: "Channel", ID: 2}, Name: "Archived 2", DefaultTestModel: "gpt-4o-mini"},
			{ID: objects.GUID{Type: "Channel", ID: 3}, Name: "Archived 3", DefaultTestModel: "claude-3-5-sonnet"},
		},
	}
	tester := &fakeArchivedChannelTester{
		results: map[int]*TestChannelResult{
			1: {Success: true, Latency: 0.11, Message: lo.ToPtr("ok-1")},
			2: {Success: false, Latency: 0.22, Error: lo.ToPtr("quota exhausted")},
			3: {Success: true, Latency: 0.33, Message: lo.ToPtr("ok-3")},
		},
	}
	recoverer := &fakeArchivedChannelRecoverer{}

	deleter := &fakeArchivedChannelDeleter{}
	runner := NewArchivedChannelRecoveryRunner(lister, tester, recoverer, deleter)

	result, err := runner.Run(context.Background(), RunArchivedChannelRecoveryInput{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.Total)
	require.Equal(t, 3, result.Tested)
	require.Equal(t, 2, result.Succeeded)
	require.Equal(t, 1, result.Failed)
	require.Equal(t, 2, result.Recoverable)
	require.Equal(t, 2, result.Recovered)
	require.Equal(t, 0, result.DeleteCandidates)
	require.Equal(t, 0, result.Deleted)
	require.Equal(t, [][]int{{1}, {3}}, recoverer.calls)
	require.Empty(t, deleter.calls)

	require.Len(t, tester.calls, 3)
	require.Nil(t, tester.calls[0].ModelID)
	require.Nil(t, tester.calls[1].ModelID)
	require.Nil(t, tester.calls[2].ModelID)

	require.Len(t, result.Items, 3)
	require.True(t, result.Items[0].Success)
	require.True(t, result.Items[0].Recovered)
	require.Equal(t, "gpt-4o-mini", result.Items[0].ModelID)
	require.False(t, result.Items[1].Success)
	require.False(t, result.Items[1].Recovered)
	require.Equal(t, "quota exhausted", lo.FromPtr(result.Items[1].Error))
	require.True(t, result.Items[2].Success)
	require.True(t, result.Items[2].Recovered)
	require.Equal(t, "claude-3-5-sonnet", result.Items[2].ModelID)
}

func TestArchivedChannelRecoveryRunner_Run_DryRunSkipsRecover(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 11}, Name: "Archived 11", DefaultTestModel: "gpt-4.1"},
		},
	}
	tester := &fakeArchivedChannelTester{
		results: map[int]*TestChannelResult{
			11: {Success: true, Latency: 0.09, Message: lo.ToPtr("ok")},
		},
	}
	recoverer := &fakeArchivedChannelRecoverer{}

	deleter := &fakeArchivedChannelDeleter{}
	runner := NewArchivedChannelRecoveryRunner(lister, tester, recoverer, deleter)

	result, err := runner.Run(context.Background(), RunArchivedChannelRecoveryInput{
		DryRun: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.DryRun)
	require.Equal(t, 1, result.Succeeded)
	require.Equal(t, 1, result.Recoverable)
	require.Equal(t, 0, result.Recovered)
	require.Equal(t, 0, result.DeleteCandidates)
	require.Equal(t, 0, result.Deleted)
	require.Empty(t, recoverer.calls)
	require.Empty(t, deleter.calls)
	require.False(t, result.Items[0].Recovered)
}

func TestArchivedChannelRecoveryRunner_Run_UsesExplicitModelOverride(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 21}, Name: "Archived 21", DefaultTestModel: "gpt-4o-mini"},
		},
	}
	tester := &fakeArchivedChannelTester{
		results: map[int]*TestChannelResult{
			21: {Success: true, Latency: 0.05, Message: lo.ToPtr("ok")},
		},
	}
	recoverer := &fakeArchivedChannelRecoverer{}

	runner := NewArchivedChannelRecoveryRunner(lister, tester, recoverer, &fakeArchivedChannelDeleter{})

	result, err := runner.Run(context.Background(), RunArchivedChannelRecoveryInput{
		ModelID: lo.ToPtr("gemini-2.5-pro"),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, tester.calls, 1)
	require.Equal(t, "gemini-2.5-pro", lo.FromPtr(tester.calls[0].ModelID))
	require.Equal(t, "gemini-2.5-pro", result.Items[0].ModelID)
}

func TestArchivedChannelRecoveryRunner_Run_TreatsTesterErrorAsFailure(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 31}, Name: "Archived 31", DefaultTestModel: "gpt-4o-mini"},
			{ID: objects.GUID{Type: "Channel", ID: 32}, Name: "Archived 32", DefaultTestModel: "gpt-4o-mini"},
		},
	}
	tester := &fakeArchivedChannelTester{
		results: map[int]*TestChannelResult{
			31: {Success: true, Latency: 0.07, Message: lo.ToPtr("ok")},
		},
		errs: map[int]error{
			32: errors.New("channel not found"),
		},
	}
	recoverer := &fakeArchivedChannelRecoverer{}

	runner := NewArchivedChannelRecoveryRunner(lister, tester, recoverer, &fakeArchivedChannelDeleter{})

	result, err := runner.Run(context.Background(), RunArchivedChannelRecoveryInput{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.Succeeded)
	require.Equal(t, 1, result.Failed)
	require.Equal(t, [][]int{{31}}, recoverer.calls)
	require.Equal(t, "channel not found", lo.FromPtr(result.Items[1].Error))
	require.False(t, result.Items[1].Success)
	require.False(t, result.Items[1].Recovered)
}

func TestArchivedChannelRecoveryRunner_Run_ReturnsRecoveryError(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 41}, Name: "Archived 41", DefaultTestModel: "gpt-4o-mini"},
			{ID: objects.GUID{Type: "Channel", ID: 42}, Name: "Archived 42", DefaultTestModel: "gpt-4o-mini"},
		},
	}
	tester := &fakeArchivedChannelTester{
		results: map[int]*TestChannelResult{
			41: {Success: true, Latency: 0.04, Message: lo.ToPtr("ok")},
			42: {Success: true, Latency: 0.05, Message: lo.ToPtr("ok-2")},
		},
	}
	recoverer := &fakeArchivedChannelRecoverer{
		err: errors.New("update failed"),
	}

	runner := NewArchivedChannelRecoveryRunner(lister, tester, recoverer, &fakeArchivedChannelDeleter{})

	result, err := runner.Run(context.Background(), RunArchivedChannelRecoveryInput{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to recover channels")
	require.NotNil(t, result)
	require.Equal(t, 1, result.Recoverable)
	require.Equal(t, 0, result.Recovered)
	require.False(t, result.Items[0].Recovered)
	require.Len(t, result.Items, 1)
	require.Equal(t, [][]int{{41}}, recoverer.calls)
	require.Len(t, tester.calls, 1)
}

func TestArchivedChannelRecoveryRunner_Run_DeletesMatchedFailedChannels(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 51}, Name: "Archived 51", DefaultTestModel: "claude-3-5-sonnet"},
			{ID: objects.GUID{Type: "Channel", ID: 52}, Name: "Archived 52", DefaultTestModel: "claude-3-5-sonnet"},
			{ID: objects.GUID{Type: "Channel", ID: 53}, Name: "Archived 53", DefaultTestModel: "gpt-4o-mini"},
		},
	}
	tester := &fakeArchivedChannelTester{
		results: map[int]*TestChannelResult{
			51: {Success: false, Latency: 0.11, Error: lo.ToPtr("<!DOCTYPE html><html><head><title>This app isn&#39;t live yet</title></head><body><h1>This app isn&#39;t live yet</h1></body></html>")},
			52: {Success: false, Latency: 0.12, Error: lo.ToPtr("Forbidden")},
			53: {Success: true, Latency: 0.13, Message: lo.ToPtr("ok")},
		},
	}
	recoverer := &fakeArchivedChannelRecoverer{}
	deleter := &fakeArchivedChannelDeleter{}

	runner := NewArchivedChannelRecoveryRunner(lister, tester, recoverer, deleter)

	result, err := runner.Run(context.Background(), RunArchivedChannelRecoveryInput{
		DeleteFailureReasons: []string{"This app isn't live yet"},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.Total)
	require.Equal(t, 2, result.Failed)
	require.Equal(t, 1, result.Succeeded)
	require.Equal(t, 1, result.Recoverable)
	require.Equal(t, 1, result.Recovered)
	require.Equal(t, 1, result.DeleteCandidates)
	require.Equal(t, 1, result.Deleted)
	require.Equal(t, map[string]int{
		"This app isn't live yet": 1,
		"Forbidden":               1,
	}, result.FailureReasonCounts)
	require.Equal(t, map[string]int{
		"This app isn't live yet": 1,
	}, result.DeletedReasonCounts)
	require.Equal(t, [][]int{{53}}, recoverer.calls)
	require.Equal(t, [][]int{{51}}, deleter.calls)

	require.Len(t, result.Items, 3)
	require.True(t, result.Items[0].DeleteMatched)
	require.True(t, result.Items[0].Deleted)
	require.Equal(t, "This app isn't live yet", lo.FromPtr(result.Items[0].FailureReason))
	require.False(t, result.Items[1].DeleteMatched)
	require.False(t, result.Items[1].Deleted)
	require.Equal(t, "Forbidden", lo.FromPtr(result.Items[1].FailureReason))
	require.True(t, result.Items[2].Recovered)
}

func TestArchivedChannelRecoveryRunner_Run_UsesConfiguredConcurrency(t *testing.T) {
	t.Parallel()

	lister := &fakeArchivedChannelLister{
		channels: []ArchivedChannelCandidate{
			{ID: objects.GUID{Type: "Channel", ID: 61}, Name: "Archived 61", DefaultTestModel: "gpt-4o-mini"},
			{ID: objects.GUID{Type: "Channel", ID: 62}, Name: "Archived 62", DefaultTestModel: "gpt-4o-mini"},
		},
	}
	tester := &blockingArchivedChannelTester{
		started: make(chan int, 2),
		release: make(chan struct{}),
	}
	runner := NewArchivedChannelRecoveryRunner(lister, tester, &fakeArchivedChannelRecoverer{}, &fakeArchivedChannelDeleter{})

	done := make(chan struct{})
	var (
		result *ArchivedChannelRecoveryResult
		err    error
	)

	go func() {
		result, err = runner.Run(context.Background(), RunArchivedChannelRecoveryInput{
			DryRun:      true,
			Concurrency: 2,
		})
		close(done)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-tester.started:
		case <-time.After(time.Second):
			t.Fatal("expected both channel tests to start before release")
		}
	}

	close(tester.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected runner to finish after releasing blocked tests")
	}

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.Total)
	require.Equal(t, 2, result.Tested)
	require.Equal(t, 2, result.Succeeded)
	require.Equal(t, 2, result.Recoverable)
	require.Equal(t, 0, result.Recovered)
	require.Len(t, result.Items, 2)
}

type fakeArchivedChannelLister struct {
	channels []ArchivedChannelCandidate
	err      error
}

func (f *fakeArchivedChannelLister) ListArchivedChannels(_ context.Context) ([]ArchivedChannelCandidate, error) {
	if f.err != nil {
		return nil, f.err
	}

	return f.channels, nil
}

type fakeArchivedChannelTester struct {
	results map[int]*TestChannelResult
	errs    map[int]error
	calls   []fakeArchivedChannelTestCall
}

type fakeArchivedChannelTestCall struct {
	ChannelID objects.GUID
	ModelID   *string
}

func (f *fakeArchivedChannelTester) TestChannel(
	_ context.Context,
	channelID objects.GUID,
	modelID *string,
	_ *httpclient.ProxyConfig,
) (*TestChannelResult, error) {
	f.calls = append(f.calls, fakeArchivedChannelTestCall{
		ChannelID: channelID,
		ModelID:   modelID,
	})

	if err := f.errs[channelID.ID]; err != nil {
		return nil, err
	}

	if result := f.results[channelID.ID]; result != nil {
		return result, nil
	}

	return &TestChannelResult{Success: false, Error: lo.ToPtr("missing fake result")}, nil
}

type blockingArchivedChannelTester struct {
	started chan int
	release chan struct{}
}

func (f *blockingArchivedChannelTester) TestChannel(
	_ context.Context,
	channelID objects.GUID,
	_ *string,
	_ *httpclient.ProxyConfig,
) (*TestChannelResult, error) {
	f.started <- channelID.ID
	<-f.release

	return &TestChannelResult{Success: true, Message: lo.ToPtr("ok")}, nil
}

type fakeArchivedChannelRecoverer struct {
	calls [][]int
	err   error
}

func (f *fakeArchivedChannelRecoverer) BulkRecoverChannels(_ context.Context, ids []int) error {
	f.calls = append(f.calls, append([]int(nil), ids...))

	if f.err != nil {
		return f.err
	}

	return nil
}

type fakeArchivedChannelDeleter struct {
	calls [][]int
	err   error
}

func (f *fakeArchivedChannelDeleter) BulkDeleteChannels(_ context.Context, ids []int) error {
	f.calls = append(f.calls, append([]int(nil), ids...))

	if f.err != nil {
		return f.err
	}

	return nil
}
