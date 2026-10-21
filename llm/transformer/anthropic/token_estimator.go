package anthropic

import (
	"math"
	"strings"
	"unicode"
)

// estimateIncompleteStreamCompletionTokens mirrors NewAPI's Claude fallback
// estimator for output that arrived before the final usage event was lost.
func estimateIncompleteStreamCompletionTokens(contentBlocks []MessageContentBlock) int64 {
	var output strings.Builder

	for _, block := range contentBlocks {
		if block.Text != nil {
			appendEstimatedOutput(&output, *block.Text)
		}

		if block.Thinking != nil {
			appendEstimatedOutput(&output, *block.Thinking)
		}

		// Tool argument deltas are model output that can be billed even when the
		// stream ends before the final Anthropic usage event arrives.
		if len(block.Input) > 0 {
			appendEstimatedOutput(&output, string(block.Input))
		}
	}

	return estimateClaudeTokens(output.String())
}

func appendEstimatedOutput(output *strings.Builder, part string) {
	if part == "" {
		return
	}

	if output.Len() > 0 {
		output.WriteByte('\n')
	}

	output.WriteString(part)
}

func estimateClaudeTokens(text string) int64 {
	if text == "" {
		return 0
	}

	const (
		wordWeight       = 1.13
		numberWeight     = 1.63
		cjkWeight        = 1.21
		symbolWeight     = 0.4
		mathSymbolWeight = 4.52
		urlDelimiter     = 1.26
		atSignWeight     = 2.82
		emojiWeight      = 2.6
		newlineWeight    = 0.89
		spaceWeight      = 0.39
	)

	type wordType int

	const (
		noWord wordType = iota
		latinWord
		numberWord
	)

	currentWordType := noWord
	var count float64

	for _, r := range text {
		switch {
		case unicode.IsSpace(r):
			currentWordType = noWord
			if r == '\n' || r == '\t' {
				count += newlineWeight
			} else {
				count += spaceWeight
			}
		case isClaudeEstimatorCJK(r):
			currentWordType = noWord
			count += cjkWeight
		case isClaudeEstimatorEmoji(r):
			currentWordType = noWord
			count += emojiWeight
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			newWordType := latinWord
			if unicode.IsNumber(r) {
				newWordType = numberWord
			}

			if currentWordType == noWord || currentWordType != newWordType {
				if newWordType == numberWord {
					count += numberWeight
				} else {
					count += wordWeight
				}
				currentWordType = newWordType
			}
		case isClaudeEstimatorMathSymbol(r):
			currentWordType = noWord
			count += mathSymbolWeight
		case r == '@':
			currentWordType = noWord
			count += atSignWeight
		case isClaudeEstimatorURLDelimiter(r):
			currentWordType = noWord
			count += urlDelimiter
		default:
			currentWordType = noWord
			count += symbolWeight
		}
	}

	return int64(math.Ceil(count))
}

func isClaudeEstimatorCJK(r rune) bool {
	return unicode.Is(unicode.Han, r) ||
		(r >= 0x3040 && r <= 0x30FF) ||
		(r >= 0xAC00 && r <= 0xD7A3)
}

func isClaudeEstimatorEmoji(r rune) bool {
	return (r >= 0x1F300 && r <= 0x1F9FF) ||
		(r >= 0x2600 && r <= 0x26FF) ||
		(r >= 0x2700 && r <= 0x27BF) ||
		(r >= 0x1FA00 && r <= 0x1FAFF)
}

func isClaudeEstimatorMathSymbol(r rune) bool {
	switch r {
	case 0x00B0, 0x00B1, 0x00B2, 0x00B3, 0x00B9, 0x00D7, 0x00F7, 0x2032, 0x2033, 0x2034:
		return true
	}

	return (r >= 0x2070 && r <= 0x207F) ||
		(r >= 0x2080 && r <= 0x208E) ||
		(r >= 0x2200 && r <= 0x22FF) ||
		(r >= 0x2A00 && r <= 0x2AFF) ||
		(r >= 0x1D400 && r <= 0x1D7FF)
}

func isClaudeEstimatorURLDelimiter(r rune) bool {
	switch r {
	case '/', ':', '?', '&', '=', ';', '#', '%':
		return true
	default:
		return false
	}
}
