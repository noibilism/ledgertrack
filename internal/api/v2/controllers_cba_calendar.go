package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/cba/services"
)

func createBusinessCalendar(s services.BusinessCalendarService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input services.CalendarInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		cal, err := s.CreateCalendar(r.Context(), input)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Created(w, cal)
	}
}

func listBusinessCalendars(s services.BusinessCalendarService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cals, err := s.ListCalendars(r.Context())
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, map[string]any{"calendars": cals})
	}
}

func readBusinessCalendar(s services.BusinessCalendarService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		cal, err := s.GetCalendar(r.Context(), id)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, cal)
	}
}

func updateBusinessCalendar(s services.BusinessCalendarService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := feeConfigID(r)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		var input services.CalendarInput
		if !decodeStrictJSON(w, r, &input) {
			return
		}
		cal, err := s.UpdateCalendar(r.Context(), id, input)
		if err != nil {
			handleReserveError(w, r, err)
			return
		}
		api.Ok(w, cal)
	}
}
