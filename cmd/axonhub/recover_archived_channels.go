package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/looplj/axonhub/internal/server/orchestrator"
)

type recoverArchivedChannelsOptions struct {
	ModelID          *string
	DryRun           bool
	DeletePageErrors bool
	DeleteReasons    []string
	Endpoint         string
	Token            string
	Concurrency      int
}

const defaultRecoverArchivedChannelsConcurrency = 12

func handleRecoverArchivedChannelsCommand() {
	options, err := parseRecoverArchivedChannelsArgs(os.Args[2:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(
			os.Stderr,
			"Usage: axonhub recover-archived-channels [--dry-run] [--model MODEL] [--endpoint URL] [--token TOKEN] [--concurrency N] [--delete-page-errors] [--delete-reason REASON ...]",
		)
		os.Exit(1)
	}

	if err := runRecoverArchivedChannelsCommand(os.Stdout, options); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseRecoverArchivedChannelsArgs(args []string) (recoverArchivedChannelsOptions, error) {
	fs := flag.NewFlagSet("recover-archived-channels", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		modelID          string
		dryRun           bool
		deleteReasons    multiValueFlag
		deletePageErrors bool
		endpoint         string
		token            string
		concurrency      int
	)

	fs.StringVar(&modelID, "model", "", "override model id used to test every archived channel")
	fs.BoolVar(&dryRun, "dry-run", false, "test archived channels without recovering successful ones")
	fs.BoolVar(&deletePageErrors, "delete-page-errors", false, "delete archived channels that fail with known dead-page errors")
	fs.Var(&deleteReasons, "delete-reason", "normalized failure reason to delete after a failed test; can be repeated")
	fs.StringVar(&endpoint, "endpoint", "", "admin graphql endpoint or service base url; defaults to AXONHUB_ADMIN_GRAPHQL_ENDPOINT or AXONHUB_CHANNEL_SYNC_ENDPOINT")
	fs.StringVar(&token, "token", "", "bearer token for admin graphql; defaults to AXONHUB_CHANNEL_SYNC_TOKEN")
	fs.IntVar(&concurrency, "concurrency", defaultRecoverArchivedChannelsConcurrency, "max concurrent archived channel tests")

	if err := fs.Parse(args); err != nil {
		return recoverArchivedChannelsOptions{}, err
	}

	if fs.NArg() > 0 {
		return recoverArchivedChannelsOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	options := recoverArchivedChannelsOptions{
		DryRun:           dryRun,
		DeletePageErrors: deletePageErrors,
		DeleteReasons:    append([]string(nil), deleteReasons...),
		Endpoint:         endpoint,
		Token:            token,
		Concurrency:      concurrency,
	}
	if modelID != "" {
		options.ModelID = lo.ToPtr(modelID)
	}

	return options, nil
}

func runRecoverArchivedChannelsCommand(stdout io.Writer, options recoverArchivedChannelsOptions) error {
	runtime, err := resolveRecoverArchivedChannelsRuntime(options, os.Getenv)
	if err != nil {
		return err
	}

	client := newAxonHubAdminGraphQLClient(runtime.Endpoint, runtime.Token, nil)
	runner := orchestrator.NewArchivedChannelRecoveryRunner(client, client, client, client)

	result, err := runner.Run(context.Background(), orchestrator.RunArchivedChannelRecoveryInput{
		ModelID:              options.ModelID,
		DryRun:               options.DryRun,
		DeleteFailureReasons: resolveArchivedChannelDeleteReasons(options),
		Concurrency:          options.Concurrency,
	})
	if result != nil {
		_, _ = io.WriteString(stdout, formatRecoverArchivedChannelsResult(result))
	}
	if err != nil {
		return err
	}

	return nil
}

func formatRecoverArchivedChannelsResult(result *orchestrator.ArchivedChannelRecoveryResult) string {
	var builder strings.Builder

	builder.WriteString("Archived channel recovery\n")
	builder.WriteString(fmt.Sprintf("Total archived channels: %d\n", result.Total))
	builder.WriteString(fmt.Sprintf("Tested: %d\n", result.Tested))
	builder.WriteString(fmt.Sprintf("Succeeded: %d\n", result.Succeeded))
	builder.WriteString(fmt.Sprintf("Failed: %d\n", result.Failed))
	builder.WriteString(fmt.Sprintf("Recoverable: %d\n", result.Recoverable))
	builder.WriteString(fmt.Sprintf("Recovered: %d\n", result.Recovered))
	builder.WriteString(fmt.Sprintf("Delete candidates: %d\n", result.DeleteCandidates))
	builder.WriteString(fmt.Sprintf("Deleted: %d\n", result.Deleted))
	if result.DryRun {
		builder.WriteString("Dry run: true\n")
	}
	appendArchivedChannelReasonCounts(&builder, "Failure reasons", result.FailureReasonCounts)
	appendArchivedChannelReasonCounts(&builder, "Deleted failure reasons", result.DeletedReasonCounts)

	if len(result.Items) == 0 {
		return builder.String()
	}

	builder.WriteString("\n")
	for _, item := range result.Items {
		status := "FAILED"
		switch {
		case item.Recovered:
			status = "RECOVERED"
		case item.Deleted:
			status = "DELETED"
		case item.DeleteMatched && result.DryRun:
			status = "WOULD_DELETE"
		case item.Success && result.DryRun:
			status = "WOULD_RECOVER"
		case item.Success:
			status = "SUCCESS"
		}

		builder.WriteString(
			fmt.Sprintf(
				"[%s] id=%d name=%q model=%q latency=%.2fs",
				status,
				item.ChannelID.ID,
				item.Name,
				item.ModelID,
				item.Latency,
			),
		)

		if message := lo.FromPtr(item.Message); message != "" {
			builder.WriteString(fmt.Sprintf(" message=%q", message))
		}

		if failureReason := lo.FromPtr(item.FailureReason); failureReason != "" {
			builder.WriteString(fmt.Sprintf(" failure_reason=%q", failureReason))
		}

		if errMsg := lo.FromPtr(item.Error); errMsg != "" {
			failureReason := lo.FromPtr(item.FailureReason)
			if failureReason == "" {
				builder.WriteString(fmt.Sprintf(" error=%q", errMsg))
			} else if errMsg != failureReason {
				builder.WriteString(fmt.Sprintf(" raw_error=%q", truncateRecoverArchivedChannelsDetail(errMsg, 200)))
			}
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

func archivedChannelDeletePageReasons() []string {
	return []string{
		"This app isn't live yet",
		"Run this app to see the results here.",
		"Cannot POST /api/anthropic/v1/messages",
	}
}

func resolveArchivedChannelDeleteReasons(options recoverArchivedChannelsOptions) []string {
	reasons := make([]string, 0, len(options.DeleteReasons)+3)
	if options.DeletePageErrors {
		reasons = append(reasons, archivedChannelDeletePageReasons()...)
	}

	reasons = append(reasons, options.DeleteReasons...)

	if len(reasons) == 0 {
		return []string{}
	}

	normalized := lo.Map(reasons, func(reason string, _ int) string {
		return orchestrator.NormalizeArchivedChannelFailureReason(reason)
	})
	normalized = lo.Filter(normalized, func(reason string, _ int) bool {
		return strings.TrimSpace(reason) != ""
	})

	return lo.Uniq(normalized)
}

type multiValueFlag []string

func (f *multiValueFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *multiValueFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func appendArchivedChannelReasonCounts(builder *strings.Builder, title string, counts map[string]int) {
	if len(counts) == 0 {
		return
	}

	builder.WriteString("\n")
	builder.WriteString(title)
	builder.WriteString(":\n")

	for _, entry := range sortedArchivedChannelReasonCounts(counts) {
		builder.WriteString(fmt.Sprintf("  - %d x %s\n", entry.Count, entry.Reason))
	}
}

type archivedChannelReasonCount struct {
	Reason string
	Count  int
}

func sortedArchivedChannelReasonCounts(counts map[string]int) []archivedChannelReasonCount {
	items := lo.MapToSlice(counts, func(reason string, count int) archivedChannelReasonCount {
		return archivedChannelReasonCount{
			Reason: reason,
			Count:  count,
		}
	})

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}

		return items[i].Reason < items[j].Reason
	})

	return items
}

func truncateRecoverArchivedChannelsDetail(value string, max int) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(value, "\n", " "))
	normalized = strings.Join(strings.Fields(normalized), " ")
	if len(normalized) <= max {
		return normalized
	}

	return normalized[:max-3] + "..."
}
