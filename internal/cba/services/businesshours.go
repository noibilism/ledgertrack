package services

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // calendars name IANA zones; do not depend on the host having them

	"github.com/formancehq/ledger/internal/cba/models"
)

// maxBusinessDaysWalked bounds the walk: 8760 hours at one open hour a day, five days a week.
const maxBusinessDaysWalked = 20000

func parseClock(raw string) (hour, minute int, err error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		return 0, 0, fmt.Errorf("time %q must be HH:MM", raw)
	}
	hour, err1 := strconv.Atoi(parts[0])
	minute, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, 0, fmt.Errorf("time %q must be HH:MM", raw)
	}
	return hour, minute, nil
}

// AddBusinessHours returns the moment `hours` business hours after `start`. Only time inside the
// calendar's opening window, on its working days and outside its holidays, counts. A start outside
// the window begins counting at the next opening.
func AddBusinessHours(start time.Time, hours int, cal *models.BusinessCalendar) (time.Time, error) {
	loc, err := time.LoadLocation(cal.Timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("calendar %s: unknown timezone %q", cal.Code, cal.Timezone)
	}
	openH, openM, err := parseClock(cal.OpenTime)
	if err != nil {
		return time.Time{}, err
	}
	closeH, closeM, err := parseClock(cal.CloseTime)
	if err != nil {
		return time.Time{}, err
	}
	working := map[time.Weekday]bool{}
	for _, d := range cal.WorkingDays {
		working[time.Weekday(d%7)] = true // ISO 7 (Sunday) maps to Go's 0
	}
	holidays := map[string]bool{}
	for _, h := range cal.Holidays {
		holidays[h] = true
	}

	remaining := time.Duration(hours) * time.Hour
	t := start.In(loc)
	for i := 0; i < maxBusinessDaysWalked; i++ {
		y, m, d := t.Date()
		if working[t.Weekday()] && !holidays[t.Format("2006-01-02")] {
			open := time.Date(y, m, d, openH, openM, 0, 0, loc)
			closing := time.Date(y, m, d, closeH, closeM, 0, 0, loc)
			if t.Before(open) {
				t = open
			}
			if t.Before(closing) {
				available := closing.Sub(t)
				if remaining <= available {
					return t.Add(remaining).UTC(), nil
				}
				remaining -= available
			}
		}
		t = time.Date(y, m, d+1, 0, 0, 0, 0, loc)
	}
	return time.Time{}, fmt.Errorf("calendar %s: no release time within %d days", cal.Code, maxBusinessDaysWalked)
}
