package services

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/cba/models"
	"github.com/formancehq/ledger/internal/cba/repositories"
)

var calendarCodePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// CalendarInput is both the create and the full-replace update body. The code is fixed after creation.
type CalendarInput struct {
	Code        string   `json:"code,omitempty"`
	Name        string   `json:"name"`
	Timezone    string   `json:"timezone"`
	WorkingDays []int    `json:"working_days"`
	OpenTime    string   `json:"open_time"`
	CloseTime   string   `json:"close_time"`
	Holidays    []string `json:"holidays,omitempty"`
}

type BusinessCalendarService interface {
	CreateCalendar(context.Context, CalendarInput) (*models.BusinessCalendar, error)
	GetCalendar(context.Context, uuid.UUID) (*models.BusinessCalendar, error)
	ListCalendars(context.Context) ([]models.BusinessCalendar, error)
	// UpdateCalendar replaces everything except the code. Holds already scheduled keep their release time.
	UpdateCalendar(context.Context, uuid.UUID, CalendarInput) (*models.BusinessCalendar, error)
}

type DefaultBusinessCalendarService struct {
	repository repositories.CalendarRepository
}

func NewBusinessCalendarService(repository repositories.CalendarRepository) *DefaultBusinessCalendarService {
	return &DefaultBusinessCalendarService{repository: repository}
}

// normalize validates the input into the calendar fields it sets.
func (in CalendarInput) normalize() (*models.BusinessCalendar, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return nil, reserveInvalid("name is required")
	}
	zone := strings.TrimSpace(in.Timezone)
	if _, err := time.LoadLocation(zone); err != nil || zone == "" || strings.EqualFold(zone, "local") {
		return nil, reserveInvalid("timezone %q is not an IANA zone, for example Africa/Lagos", in.Timezone)
	}
	if len(in.WorkingDays) == 0 {
		return nil, reserveInvalid("working_days needs at least one day, 1 (Monday) to 7 (Sunday)")
	}
	seen := map[int]bool{}
	days := make([]int, 0, len(in.WorkingDays))
	for _, d := range in.WorkingDays {
		if d < 1 || d > 7 {
			return nil, reserveInvalid("working day %d must be 1 (Monday) to 7 (Sunday)", d)
		}
		if !seen[d] {
			seen[d] = true
			days = append(days, d)
		}
	}
	sort.Ints(days)

	oh, om, err := parseClock(in.OpenTime)
	if err != nil {
		return nil, reserveInvalid("open_time: %v", err)
	}
	ch, cm, err := parseClock(in.CloseTime)
	if err != nil {
		return nil, reserveInvalid("close_time: %v", err)
	}
	if (ch*60+cm)-(oh*60+om) < 60 {
		return nil, reserveInvalid("close_time must be at least one hour after open_time")
	}

	holidaySet := map[string]bool{}
	holidays := make([]string, 0, len(in.Holidays))
	for _, h := range in.Holidays {
		h = strings.TrimSpace(h)
		if _, err := time.Parse("2006-01-02", h); err != nil {
			return nil, reserveInvalid("holiday %q must be YYYY-MM-DD", h)
		}
		if !holidaySet[h] {
			holidaySet[h] = true
			holidays = append(holidays, h)
		}
	}
	sort.Strings(holidays)

	return &models.BusinessCalendar{
		Name: name, Timezone: zone, WorkingDays: days,
		OpenTime: in.OpenTime, CloseTime: in.CloseTime, Holidays: holidays,
	}, nil
}

func (s *DefaultBusinessCalendarService) CreateCalendar(ctx context.Context, in CalendarInput) (*models.BusinessCalendar, error) {
	code := strings.TrimSpace(in.Code)
	if !calendarCodePattern.MatchString(code) {
		return nil, reserveInvalid("code must match %s", calendarCodePattern)
	}
	cal, err := in.normalize()
	if err != nil {
		return nil, err
	}
	cal.Code = code
	existing, err := s.repository.ListCalendars(ctx)
	if err != nil {
		return nil, err
	}
	for _, other := range existing {
		if strings.EqualFold(other.Code, code) {
			return nil, fmt.Errorf("%w: calendar code %s is already used", ErrReserveConflict, code)
		}
	}
	if err := s.repository.CreateCalendar(ctx, cal); err != nil {
		return nil, err
	}
	return cal, nil
}

func (s *DefaultBusinessCalendarService) GetCalendar(ctx context.Context, id uuid.UUID) (*models.BusinessCalendar, error) {
	cal, err := s.repository.GetCalendar(ctx, id)
	if err != nil {
		return nil, reserveNotFound(err, "business calendar")
	}
	return cal, nil
}

func (s *DefaultBusinessCalendarService) ListCalendars(ctx context.Context) ([]models.BusinessCalendar, error) {
	return s.repository.ListCalendars(ctx)
}

func (s *DefaultBusinessCalendarService) UpdateCalendar(ctx context.Context, id uuid.UUID, in CalendarInput) (*models.BusinessCalendar, error) {
	cal, err := s.GetCalendar(ctx, id)
	if err != nil {
		return nil, err
	}
	if code := strings.TrimSpace(in.Code); code != "" && !strings.EqualFold(code, cal.Code) {
		return nil, reserveInvalid("the calendar code cannot be changed")
	}
	next, err := in.normalize()
	if err != nil {
		return nil, err
	}
	cal.Name, cal.Timezone, cal.WorkingDays = next.Name, next.Timezone, next.WorkingDays
	cal.OpenTime, cal.CloseTime, cal.Holidays = next.OpenTime, next.CloseTime, next.Holidays
	if err := s.repository.UpdateCalendar(ctx, cal); err != nil {
		return nil, err
	}
	return cal, nil
}
