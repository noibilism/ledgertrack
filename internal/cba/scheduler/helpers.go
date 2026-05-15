package scheduler

import "time"

func normalizeScheduleDate(when time.Time) time.Time {
	return when.UTC().Truncate(24 * time.Hour)
}
