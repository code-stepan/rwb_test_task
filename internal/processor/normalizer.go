package processor

import (
	"strings"
	"unicode"
)

func NormalizeQuery(q string) string {
	q = strings.ToLower(strings.TrimSpace(q))
	if q == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(q))
	lastSpace := true

	for _, r := range q {
		if unicode.IsSpace(r) {
			if !lastSpace {
				b.WriteRune(' ')
				lastSpace = true
			}
		} else {
			b.WriteRune(r)
			lastSpace = false
		}
	}
	return strings.TrimSpace(b.String())
}
