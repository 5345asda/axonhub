package orchestrator

import (
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strings"
)

var (
	archiveRecoveryTitlePattern = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	archiveRecoveryH1Pattern    = regexp.MustCompile(`(?is)<h1[^>]*>(.*?)</h1>`)
	archiveRecoveryPrePattern   = regexp.MustCompile(`(?is)<pre[^>]*>(.*?)</pre>`)
	archiveRecoverySpacePattern = regexp.MustCompile(`\s+`)
)

type archivedChannelErrorEnvelope struct {
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// NormalizeArchivedChannelFailureReason converts raw channel test failures into stable, actionable reasons.
func NormalizeArchivedChannelFailureReason(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	decoded := html.UnescapeString(trimmed)

	if reason := normalizeArchivedChannelJSONFailure(decoded); reason != "" {
		return reason
	}

	if reason := normalizeArchivedChannelHTMLFailure(decoded); reason != "" {
		return reason
	}

	return collapseArchivedChannelFailureWhitespace(decoded)
}

func normalizeArchivedChannelJSONFailure(raw string) string {
	if !strings.HasPrefix(raw, "{") {
		return ""
	}

	var envelope archivedChannelErrorEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || envelope.Error == nil {
		return ""
	}

	code := strings.TrimSpace(envelope.Error.Code)
	message := strings.TrimSpace(envelope.Error.Message)

	switch {
	case code != "" && message != "":
		return fmt.Sprintf("%s - %s", code, message)
	case code != "":
		return code
	case message != "":
		return message
	default:
		return ""
	}
}

func normalizeArchivedChannelHTMLFailure(raw string) string {
	for _, pattern := range []*regexp.Regexp{
		archiveRecoveryTitlePattern,
		archiveRecoveryH1Pattern,
		archiveRecoveryPrePattern,
	} {
		matches := pattern.FindStringSubmatch(raw)
		if len(matches) < 2 {
			continue
		}

		reason := collapseArchivedChannelFailureWhitespace(html.UnescapeString(matches[1]))
		if reason != "" {
			return reason
		}
	}

	return ""
}

func collapseArchivedChannelFailureWhitespace(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	return archiveRecoverySpacePattern.ReplaceAllString(trimmed, " ")
}
