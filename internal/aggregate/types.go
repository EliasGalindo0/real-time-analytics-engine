package aggregate

import (
	"crypto/sha1"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// Event is the ingestion payload after decoding/validation.
// Tags are part of the series key (high cardinality risk!).
type Event struct {
	Name  string            `json:"name"`
	Value float64           `json:"value"`
	TS    time.Time         `json:"ts"`
	Tags  map[string]string `json:"tags,omitempty"`
}

// SeriesKey is a stable identifier for a metric series.
// We canonicalize tags, then hash to keep keys compact.
type SeriesKey string

func MakeSeriesKey(name string, tags map[string]string) SeriesKey {
	if len(tags) == 0 {
		return SeriesKey(name)
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.Grow(len(name) + len(keys)*16)
	b.WriteString(name)
	b.WriteByte('|')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(tags[k])
	}

	sum := sha1.Sum([]byte(b.String()))
	return SeriesKey(name + "|" + hex.EncodeToString(sum[:8])) // 64-bit prefix
}

