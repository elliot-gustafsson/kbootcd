package controller

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var EveryDay = []string{
	time.Sunday.String(),
	time.Monday.String(),
	time.Tuesday.String(),
	time.Wednesday.String(),
	time.Thursday.String(),
	time.Friday.String(),
	time.Saturday.String(),
}

type Weekdays uint32

func (t Weekdays) Contains(d time.Weekday) bool {
	if t == 0 {
		return true
	}
	return uint32(t)&(1<<uint32(d)) != 0
}

type Window struct {
	Weekdays   Weekdays
	Start, End time.Time
	Location   *time.Location
}

func (w *Window) Contains(t time.Time) bool {
	loctime := t.In(w.Location)

	// Convert all times to minutes since midnight
	startMins := w.Start.Hour()*60 + w.Start.Minute()
	endMins := w.End.Hour()*60 + w.End.Minute()
	nowMins := loctime.Hour()*60 + loctime.Minute()
	isOvernight := startMins > endMins

	// Check if the current time falls inside the allowed hours
	timeIsValid := false
	if isOvernight {
		// e.g. 22:00 to 04:00
		timeIsValid = nowMins >= startMins || nowMins <= endMins
	} else {
		// e.g. 09:00 to 17:00
		timeIsValid = nowMins >= startMins && nowMins <= endMins
	}
	if !timeIsValid {
		return false
	}

	activeDay := loctime.Weekday()

	// If it is an overnight window, and we are in the early morning hours,
	// this maintenance window actually started yesterday
	if isOvernight && nowMins <= endMins {
		activeDay = getYesterday(activeDay)
	}

	// check if the active day is in our allowed list
	return w.Weekdays.Contains(activeDay)
}

// getYesterday safely wraps Sunday (0) backwards to Saturday (6)
func getYesterday(day time.Weekday) time.Weekday {
	return (day + 6) % 7
}

func BuildTimeWindow(start, end string, days []string, timezone string) (Window, error) {
	w := Window{}

	var err error

	w.Start, err = parseTime(start)
	if err != nil {
		return w, err
	}

	w.End, err = parseTime(end)
	if err != nil {
		return w, err
	}

	if days == nil {
		days = EveryDay
	}

	var weekdays Weekdays
	for _, s := range days {
		d, err := parseWeekday(s)
		if err != nil {
			return w, err
		}
		weekdays |= 1 << uint32(d)
	}
	w.Weekdays = weekdays

	w.Location, err = time.LoadLocation(timezone)
	if err != nil {
		return w, err
	}

	return w, nil
}

func parseTime(s string) (time.Time, error) {
	fmts := []string{"15:04", "15:04:05", "15"}
	for _, f := range fmts {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}

	return time.Now(), fmt.Errorf("Invalid time format: %s", s)
}

func parseWeekday(s string) (time.Weekday, error) {
	if n, err := strconv.Atoi(s); err == nil {
		if n >= int(time.Sunday) && n <= int(time.Saturday) {
			return time.Weekday(n), nil
		}
		return time.Sunday, fmt.Errorf("invalid weekday, number out of range: %s", s)
	}

	var wd time.Weekday

	switch strings.ToLower(s) {
	case "su", "sun", "sunday":
		wd = time.Sunday
	case "mo", "mon", "monday":
		wd = time.Monday
	case "tu", "tue", "tuesday":
		wd = time.Tuesday
	case "we", "wed", "wednesday":
		wd = time.Wednesday
	case "th", "thu", "thursday":
		wd = time.Thursday
	case "fr", "fri", "friday":
		wd = time.Friday
	case "sa", "sat", "saturday":
		wd = time.Saturday
	default:
		return time.Sunday, fmt.Errorf("invalid weekday: %s", s)
	}

	return wd, nil
}
