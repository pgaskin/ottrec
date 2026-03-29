package schema

import (
	_ "embed"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.66.1 generate --template {"version":"v2","plugins":[{"local":["go","tool","protoc-gen-go"],"out":".","opt":["paths=source_relative","Mschema.proto=./schema","default_api_level=API_OPAQUE"]}]}

//go:embed schema.proto
var schema []byte

func Proto() string {
	return string(schema)
}

func ToWeekday(w time.Weekday) Weekday {
	return Weekday(w)
}

func (w Weekday) AsWeekday() time.Weekday {
	return time.Weekday(w)
}

type ClockTime int32

func MakeClockTime(hh, mm int) ClockTime {
	if hh < 0 || mm < 0 {
		return -1
	}
	return ClockTime(hh*60 + mm)
}

func (t ClockTime) IsValid() bool {
	return t >= 0
}

func (t ClockTime) Split() (d int, hh, mm int) {
	if t >= 0 {
		d = int(t / (24 * 60))
		t %= 24 * 60
		hh = int(t / 60)
		mm = int(t % 60)
	}
	return
}

func (t ClockTime) String() string {
	return t.Format(true)
}

func (t ClockTime) Format(ampm bool) string {
	if !t.IsValid() {
		return "invalid"
	}
	var b strings.Builder
	d, hh, mm := t.Split()
	ap := byte('a')
	if ampm && hh >= 12 {
		ap = 'p'
		hh -= 12
	}
	for range d {
		b.WriteByte('>')
	}
	if ampm && hh == 0 {
		b.WriteByte('1')
		b.WriteByte('2')
	} else {
		if !ampm || hh >= 10 {
			b.WriteByte('0' + byte(hh/10))
		}
		b.WriteByte('0' + byte(hh%10))
	}
	b.WriteByte(':')
	b.WriteByte('0' + byte(mm/10))
	b.WriteByte('0' + byte(mm%10))
	if ampm {
		b.WriteByte(ap)
		b.WriteByte('m')
	}
	return b.String()
}

func (t ClockTime) Norm() ClockTime {
	if t < 0 {
		t = -1
	}
	return t
}

func (t ClockTime) GoString() string {
	var b strings.Builder
	b.WriteString(reflect.TypeOf(t).String())
	if t < 0 {
		b.WriteString(strconv.FormatInt(int64(t), 10))
	} else {
		b.WriteString("(60*")
		b.WriteString(strconv.FormatInt(int64(t/60), 10))
		b.WriteString("+")
		b.WriteString(strconv.FormatInt(int64(t%60), 10))
	}
	b.WriteString(")")
	return b.String()
}

type ClockRange struct {
	Start ClockTime
	End   ClockTime
}

func MakeClockRange(hh1, mm1, hh2, mm2 int) ClockRange {
	r := ClockRange{
		Start: MakeClockTime(hh1, mm1),
		End:   MakeClockTime(hh2, mm2),
	}
	if r.End < r.Start {
		r.End += 24 * 60
	}
	return r
}

func (r ClockRange) IsValid() bool {
	return r.Start.IsValid() && r.End.IsValid() && r.Start < r.End
}

func (r ClockRange) String() string {
	return r.Format(true)
}

func (r ClockRange) Format(ampm bool) string {
	if !r.IsValid() {
		return "invalid"
	}
	x := r.Start.Format(ampm)
	y := r.End.Format(ampm)
	if r.End-r.Start < 24*60 && r.Start < 24*60 {
		if y[0] == '>' {
			y = y[1:]
		}
		if ampm && x[len(x)-2] == y[len(y)-2] {
			x = x[:len(x)-2]
		}
	}
	return x + " - " + y
}

func (r ClockRange) Overlaps(o ClockRange) bool {
	return r.IsValid() && r.Start <= o.End && o.Start <= r.End
}

// Date represents any combination of Weekday/Year/Month/Day as an integer in
// the form YYYYMMDDW, YYYY is the zero-padded year, MM is the zero-padded month
// starting at Jan=1, DD is the zero-padded day, and W is the weekday starting
// at Sun=1. Any component may be zero. It is sortable and will be ordered
// naturally.
type Date int32

// MakeDate makes a new Date. If w is negative, it is unspecified. If y/m/d are
// zero, they are unspecified.
func MakeDate(year int, month time.Month, day int, wkday time.Weekday) Date {
	var x Date
	if year = min(year, 9999); year > 0 {
		x += Date(year * 1_00_00_0)
	}
	if month = min(month, 99); month > 0 {
		x += Date(month * 1_00_0)
	}
	if day = min(day, 99); day > 0 {
		x += Date(day * 1_0)
	}
	if wkday = min(wkday+1, 9); wkday > 0 {
		x += Date(wkday * 1)
	}
	return x
}

// MakeDateFromGo makes a fully-populated (year, month, day, weekday) [Date]
// from a Go [time.Time]. If t is [time.Time.IsZero], the [Date] is also
// [Date.IsZero].
func MakeDateFromGo(t time.Time) Date {
	year, month, day := t.Date()
	return MakeDate(year, month, day, t.Weekday())
}

// IsZero returns true if d is zero.
func (d Date) IsZero() bool {
	return d == 0
}

// IsValid returns true if d is non-zero and the specified components are valid
// together.
func (d Date) IsValid() bool {
	if d < 0 || d >= 9999_99_99_9 {
		return false
	}
	if d.IsZero() {
		return false
	}
	var (
		wkday, hasWkday = d.Weekday()
		year, hasYear   = d.Year()
		month, hasMonth = d.Month()
		day, hasDay     = d.Day()
	)
	if !hasWkday && wkday != 0 {
		return false // wkday specified but invalid
	}
	if !hasYear && year != 0 {
		return false // year specified but invalid
	}
	if !hasMonth && month != 0 {
		return false // month specified but invalid
	}
	if !hasDay && day != 0 {
		return false // day specified but invalid
	}
	if hasMonth {
		if hasYear {
			if day > daysInMonth(year, month) {
				return false // day out of range for month
			}
		} else if day > daysInMonth(2024, month) { // leap year for max feb days
			return false // day out of range for month
		}
	}
	if hasYear && hasMonth && hasDay && hasWkday {
		if wkday != time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Weekday() {
			return false // invalid weekday for year/month/day
		}
	}
	return true
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// Year returns the year component, if specified.
func (d Date) Year() (int, bool) {
	if d > 0 {
		if year := d / 1_00_00_0 % 1_0000; year != 0 {
			return int(year), true
		}
	}
	return 0, false
}

// Month returns the month component, if specified.
func (d Date) Month() (time.Month, bool) {
	if d > 0 {
		if month := d / 1_00_0 % 1_00; month != 0 {
			if month >= 1 && month <= 12 {
				return time.Month(month), true
			}
			return time.Month(month), false
		}
	}
	return 0, false
}

// Day returns the day component, if specified.
func (d Date) Day() (int, bool) {
	if d > 0 {
		if day := d / 1_0 % 1_00; day != 0 {
			if day >= 1 && day <= 31 {
				return int(day), true
			}
			return int(day), false
		}
	}
	return 0, false
}

// Day returns the weekday component, if specified.
func (d Date) Weekday() (time.Weekday, bool) {
	if d > 0 {
		if wkday := d / 1 % 1_0; wkday != 0 {
			if wkday >= 1 && wkday <= 7 {
				return time.Weekday(wkday - 1), true
			}
			return time.Weekday(wkday), false
		}
	}
	return 0, false
}

func (d Date) String() string {
	if d.IsZero() {
		return ""
	}
	var b strings.Builder
	var (
		wkday, hasWkday = d.Weekday()
		year, hasYear   = d.Year()
		month, hasMonth = d.Month()
		day, hasDay     = d.Day()
	)
	if hasWkday {
		b.WriteString(wkday.String())
	}
	if hasMonth {
		if hasWkday {
			b.WriteString(", ")
		}
		b.WriteString(month.String())
		if hasDay || hasYear {
			b.WriteString(" ")
		}
		if hasDay {
			b.WriteString(strconv.Itoa(day))
		}
	}
	if hasYear {
		if hasWkday || (hasMonth && hasDay) {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Itoa(year))
	}
	return b.String()
}

func (d Date) GoString() string {
	var b strings.Builder
	b.WriteString(reflect.TypeOf(d).String())
	b.WriteString("(")
	if d <= 0 {
		b.WriteString(strconv.FormatInt(int64(d), 10))
	} else {
		var (
			c []byte
			n int
		)
		for x := d; x != 0; x /= 10 {
			switch n {
			case 1, 3, 5:
				c = append(c, '_')
			}
			c = append(c, '0'+byte(x%10))
			n++
		}
		slices.Reverse(c)
		b.Write(c)
	}
	b.WriteString(")")
	return b.String()
}

// GoTime returns a Go time value for the date. If the date is invalid or not a
// complete date with year/month/day, ok is false. If the date is zero, the
// corresponding time is zero.
func (d Date) GoTime(loc *time.Location) (time.Time, bool) {
	if d.IsZero() {
		return time.Time{}, true
	}
	if !d.IsValid() {
		return time.Time{}, false
	}
	year, ok := d.Year()
	if !ok {
		return time.Time{}, false
	}
	month, ok := d.Month()
	if !ok {
		return time.Time{}, false
	}
	day, ok := d.Day()
	if !ok {
		return time.Time{}, false
	}
	return time.Date(year, month, day, 0, 0, 0, 0, loc), true
}

// DateRange is an inclusive range of dates. Either side may be zero.
type DateRange struct {
	From Date
	To   Date
}

func (d DateRange) String() string {
	var b strings.Builder
	if hasFrom, hasTo := !d.From.IsZero(), !d.To.IsZero(); hasFrom || hasTo {
		if d.From == d.To {
			return d.From.String()
		}
		switch {
		case hasFrom && !hasTo:
			b.WriteString("starting ")
		case !hasFrom && hasTo:
			b.WriteString("until ")
		}
		if hasFrom {
			if d.From.IsValid() {
				b.WriteString(d.From.String())
			} else {
				b.WriteString("<invalid>")
			}
		}
		if hasFrom && hasTo {
			b.WriteString(" to ")
		}
		if hasTo {
			if d.To.IsValid() {
				b.WriteString(d.To.String())
			} else {
				b.WriteString("<invalid>")
			}
		}
	}
	return b.String()
}

// GoTime returns Go time values for the date range. If either side is invalid
// or not a complete date with year/month/day, ok is false. If a side is open,
// the corresponding time is zero. For convenience, the end time is 1 nanosecond
// before the next day (unlike if you called [Date.GoTime] on the From value
// directly).
func (d DateRange) GoTime(loc *time.Location) (from time.Time, to time.Time, ok bool) {
	from, fromOK := d.From.GoTime(loc)
	to, toOK := d.To.GoTime(loc)
	if toOK && !to.IsZero() {
		to.AddDate(0, 0, 1).Add(-time.Nanosecond)
	}
	return from, to, fromOK && toOK
}

func (tr *TimeRange) AsXParsed() (w time.Weekday, r ClockRange, ok bool) {
	ok = true
	if tr.HasXWkday() {
		w = time.Weekday(tr.GetXWkday())
	} else {
		ok = false
	}
	if tr.HasXStart() {
		r.Start = ClockTime(tr.GetXStart())
	} else {
		r.Start = -1
		ok = false
	}
	if tr.HasXEnd() {
		r.End = ClockTime(tr.GetXEnd())
	} else {
		r.End = -1
		ok = false
	}
	return
}

func (s *Schedule) AsXParsedDate() (d DateRange, ok bool) {
	ok = true
	if s.HasXFrom() {
		d.From = Date(s.GetXFrom())
	} else {
		d.From = -1
		ok = false
	}
	if s.HasXTo() {
		d.To = Date(s.GetXTo())
	} else {
		d.To = -1
		ok = false
	}
	return
}
