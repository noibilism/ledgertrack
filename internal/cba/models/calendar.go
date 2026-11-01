package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// BusinessCalendar defines business hours: the days worked, the opening window in the calendar's
// timezone and the holidays on which nothing counts.
type BusinessCalendar struct {
	bun.BaseModel `bun:"_system.business_calendars,alias:business_calendars"`

	ID       uuid.UUID `json:"id" bun:"id,type:uuid,pk"`
	Code     string    `json:"code" bun:"code,type:varchar(64),notnull"`
	Name     string    `json:"name" bun:"name,type:varchar(255),notnull"`
	Timezone string    `json:"timezone" bun:"timezone,type:varchar(64),notnull"`
	// WorkingDays uses ISO numbers: 1 Monday to 7 Sunday.
	WorkingDays []int `json:"working_days" bun:"working_days,type:jsonb,notnull"`
	// OpenTime and CloseTime are "HH:MM" in Timezone.
	OpenTime  string `json:"open_time" bun:"open_time,type:varchar(5),notnull"`
	CloseTime string `json:"close_time" bun:"close_time,type:varchar(5),notnull"`
	// Holidays are "YYYY-MM-DD" dates in Timezone.
	Holidays  []string  `json:"holidays" bun:"holidays,type:jsonb,notnull,default:'[]'::jsonb"`
	CreatedAt time.Time `json:"created_at" bun:"created_at,type:timestamp without time zone,nullzero"`
	UpdatedAt time.Time `json:"updated_at" bun:"updated_at,type:timestamp without time zone,nullzero"`
}
