package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// HumanReadableIntsMax joins a serials of inits into a smart one like 1-3 5 ... for human readable.
func HumanReadableIntsMax(max int, ids ...int) string {
	if len(ids) <= max {
		return HumanReadableInts(ids...)
	}

	return HumanReadableInts(ids[:max]...) + " ..."
}

// HumanReadableInts joins a serials of inits into a smart one like 1-3 5 7-10 for human readable.
func HumanReadableInts(ids ...int) string {
	sort.Ints(ids)

	s := ""
	start := 0
	last := 0

	var sSb25 strings.Builder
	for i, v := range ids {
		if i == 0 {
			start = v
			last = v
			s = strconv.Itoa(v)

			continue
		}

		if last+1 == v {
			last = v

			continue
		}

		if last > start {
			sSb25.WriteString(fmt.Sprintf("-%d", last))
		}

		sSb25.WriteString(fmt.Sprintf(" %d", v))
		start = v
		last = v
	}
	s += sSb25.String()

	if last != start {
		s += fmt.Sprintf("-%d", last)
	}

	return s
}
