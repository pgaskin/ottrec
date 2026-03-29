package main

import (
	"bytes"
	"cmp"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"iter"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/expr-lang/expr"
	"github.com/pgaskin/ottrec/schema"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestNormalizeText(t *testing.T) {
	for _, tc := range []struct {
		A, B string
		N, L bool
	}{
		{"", "", true, false},
		{"test\ntest", "test\ntest", true, false},
		{"  test\n  \u00a0\u00a0test\u2013  ", "test\n test-", true, false},
		{"  test\n  \u00a0\u00a0test\u2013  ", "test test-", false, false},
		{"  SDFsk jdnfks   jwERMwe   rkjwn   ", "sdfsk jdnfks jwermwe rkjwn", false, true},
		// TODO: more tests
	} {
		if c := normalizeText(tc.A, tc.N, tc.L); c != tc.B {
			t.Errorf("normalize %q (lower=%t): expected %q, got %q", tc.A, tc.L, tc.B, c)
		}
	}
}

func TestParseClockRange(t *testing.T) {
	for _, tc := range []struct {
		A, B string
	}{
		// invalid
		{"", ""},                  // empty
		{"1:00am", ""},            // TODO: should we parse this as a zero-length range?
		{"1:00pm", ""},            // TODO: same
		{"noon-noon", ""},         // two-component zero length range
		{"01:00-01:00", ""},       // two-component zero length range
		{"123-456", ""},           // invalid hour range
		{"1h00am-2h00pm", ""},     // french time with am/pm
		{"001:00-2:00", ""},       // hour too long
		{"01:000-2:00", ""},       // minute too long
		{"1pm,2pm", ""},           // not a range
		{"1pm 2pm", ""},           // not a range
		{"0", ""},                 // single number
		{"12", ""},                // single number
		{"99:00-02:00", ""},       // invalid hour
		{"02:00-99:00", ""},       // invalid hour
		{"02:00-a9:00", ""},       // invalid hour
		{"01:99-02:00", ""},       // invalid minute
		{"02:00-01:99", ""},       // invalid minute
		{"02:00-01:a9", ""},       // invalid minute
		{"02:30-", ""},            // open range
		{"-02:30", ""},            // open range
		{"2:00am-99:00", ""},      // misc
		{"01:00-02:00-03:00", ""}, // more than two components

		// valid 24h
		{"00:00-23:59", "00:00 - 23:59"},
		{"05:00-17:00", "05:00 - 17:00"},
		{"05-17", "05:00 - 17:00"},
		{"1-3", "01:00 - 03:00"},

		// valid 12h
		{"3:12am-11:23am", "03:12 - 11:23"},
		{"3:12pm-11:23pm", "15:12 - 23:23"},
		{"12:34am-5:43pm", "00:34 - 17:43"},
		{"12am-12pm", "00:00 - 12:00"},
		{"12pm-12am", "12:00 - 00:00"},
		{"03:00am-05:00am", "03:00 - 05:00"},
		{"03:00pm-05:00pm", "15:00 - 17:00"},

		// valid french
		{"0h00-1h00", "00:00 - 01:00"},
		{"00h00-1h00", "00:00 - 01:00"},
		{"5h12-23h15", "05:12 - 23:15"},

		// valid military
		{"0000-0100", "00:00 - 01:00"},
		{"0512-2315", "05:12 - 23:15"},

		// special
		{"noon-midnight", "12:00 - 00:00"},

		// special implies am/pm
		{"midnight - noon", "00:00 - 12:00"},
		{"1:00 - noon", "01:00 - 12:00"},
		{"1:00 am - noon", "01:00 - 12:00"},
		{"7:30-noon", "07:30 - 12:00"},
		{"noon-7:30", ""}, // ambiguous
		{"noon-7:30 pm", "12:00 - 19:30"},
		{"11:45 - 1pm", "11:45 - 13:00"},

		// next-day logic
		{"12:59-4:00am", "12:59 - 04:00"},
		{"12:59-4:00pm", "12:59 - 16:00"},
		{"3:30am-2:30pm", "03:30 - 14:30"},

		// am/pm assumption and next-day logic, h2>h1
		{"3-5", "03:00 - 05:00"},
		{"3-5am", "03:00 - 05:00"},
		{"3am-5", ""},
		{"3-5pm", "15:00 - 17:00"},
		{"3pm-5", ""},
		{"3am-5pm", "03:00 - 17:00"},
		{"3pm-5am", "15:00 - 05:00"},

		// am/pm assumption and next-day logic, h1>h2
		{"5-3", "05:00 - 03:00"},
		{"5-3am", "05:00 - 03:00"},
		{"5am-3", ""},
		{"5-3pm", "05:00 - 15:00"},
		{"5pm-3", ""},
		{"5am-3pm", "05:00 - 15:00"},
		{"5pm-3am", "17:00 - 03:00"},

		// misc ambiguous mixed 24h/12h
		{"23:03-5pm", ""},
		{"5pm-23:03", ""},
		{"noon-6:00", ""},
		{"noon-06:00", ""},
		{"6:00-noon", "06:00 - 12:00"},
		{"06:00-noon", "06:00 - 12:00"},
		{"23:00-noon", ""},

		// noon/midnight ambiguous time exception
		{"noon-12:30", "12:00 - 12:30"},
		{"noon-1", "12:00 - 13:00"},
		{"noon-1:30", "12:00 - 13:30"},
		{"noon-2:30", "12:00 - 14:30"},
		{"noon-3", ""}, // limit it to 3h for now
		{"noon-11", ""},
		{"noon-11:59", ""},
		{"midnight-12:30", "00:00 - 00:30"},
		{"midnight-1", "00:00 - 01:00"},
		{"midnight-1:30", "00:00 - 01:30"},
		{"midnight-2:30", "00:00 - 02:30"},
		{"midnight-3", ""}, // limit it to 3h for now
		{"midnight-11", ""},
		{"midnight-11:59", ""},

		// misc special
		{"noon-12:55pm", "12:00 - 12:55"},
		{"midnight-12:55am", "00:00 - 00:55"},

		// misc important somewhat ambiguous cases (the meaning of these must not be changed)
		{"midnight-noon", "00:00 - 12:00"},
		{"noon-midnight", "12:00 - 00:00"},
		{"00:30-noon", "00:30 - 12:00"},
		{"1-noon", "01:00 - 12:00"},
		{"8-noon", "08:00 - 12:00"},
		{"noon-8pm", "12:00 - 20:00"},
		{"noon-1am", "12:00 - 01:00"},
		{"noon-8am", "12:00 - 08:00"},
		{"3am-5pm", "03:00 - 17:00"},
		{"3-5pm", "15:00 - 17:00"},
		{"6-5pm", "06:00 - 17:00"},
		{"6pm-5pm", "18:00 - 17:00"},
		{"12-5pm", "12:00 - 17:00"},
		{"13-5pm", ""},
		{"12-5am", "12:00 - 05:00"},
		{"4-5am", "04:00 - 05:00"},
		{"5-5am", ""},
		{"5-5:30am", "05:00 - 05:30"},
		{"5-5pm", ""},
		{"5-5:30pm", "17:00 - 17:30"},
		{"10:00-10:30pm", "22:00 - 22:30"},
		{"11-10:30pm", "11:00 - 22:30"},
		{"8-10pm", "20:00 - 22:00"},
		{"10-11pm", "22:00 - 23:00"},
		{"2-11pm", "14:00 - 23:00"},
		{"1-11pm", "13:00 - 23:00"},
		{"11-6pm", "11:00 - 18:00"},
		{"11-5pm", "11:00 - 17:00"},
		{"12-12:50pm", "12:00 - 12:50"},
		{"11-11:50pm", "23:00 - 23:50"},
		{"10-10:50pm", "22:00 - 22:50"},
		{"9-9:50pm", "21:00 - 21:50"},
		{"8-8:50pm", "20:00 - 20:50"},
		{"7-7:50pm", "19:00 - 19:50"},
		{"6-6:50pm", "18:00 - 18:50"},
		{"5-5:50pm", "17:00 - 17:50"},
		{"1-1:50pm", "13:00 - 13:50"},
		{"12-12:50am", "00:00 - 00:50"},
		{"7-7:50am", "07:00 - 07:50"},
		{"1-1:50am", "01:00 - 01:50"},

		// typo correction for extraneous separators
		{"01:00-02:00", "01:00 - 02:00"},
		{"01:00--02:00", "01:00 - 02:00"},
		{"01:00- -02:00", "01:00 - 02:00"},
		{"01:00 - - 02:00", "01:00 - 02:00"},
		{"01:00 - x - 02:00", ""},
		{"01:00 to 02:00 am", "01:00 - 02:00"},
		{"01:00 - to 02:00 am", "01:00 - 02:00"},
		{"01:00 to - 02:00 am", "01:00 - 02:00"},
		{"01:00 - to - 02:00 am", "01:00 - 02:00"},
		{"01:00 to - to 02:00 am", "01:00 - 02:00"},

		// typo correction for duplicate am/pm suffixes
		{"3:12am-11:23am am", "03:12 - 11:23"},
		{"3:12pm-11:23pm pm", "15:12 - 23:23"},
		{"3:12am am-11:23am am", "03:12 - 11:23"},
		{"3:12pm pm-11:23pm pm", "15:12 - 23:23"},
		{"3:12am am-11:23am", "03:12 - 11:23"},
		{"3:12pm pm-11:23pm", "15:12 - 23:23"},
		{"12:34am am-5:43pm pm", "00:34 - 17:43"},
		{"12:34amam-5:43pmpm", "00:34 - 17:43"},
		{"12:34aam-5:43ppm", ""},
		{"12:34aa-5:43pp", ""},
		{"1:00 am am - noon", "01:00 - 12:00"},
		{"1:00 am - noon pm", ""},

		// text normalization
		{"  \x1b1:00pm \u2013\n  \u00a02:\u200b00\x00 am", "13:00 - 02:00"},
		{"Noon - Midnight", "12:00 - 00:00"},
		{"Noon to Midnight", "12:00 - 00:00"},
	} {
		c, ok := parseClockRange(tc.A)
		if tc.B == "" {
			if ok {
				t.Errorf("parse %q: expected error, got %q (%#v)", tc.A, c.Format(false), c)
			}
			continue
		}
		if !ok {
			t.Errorf("parse %q: unexpected error", tc.A)
			continue
		}
		if s := c.Format(false); tc.B != s {
			t.Errorf("parse %q: expected %q, got %q (%#v)", tc.A, tc.B, s, c)
		}
		if c.Start >= 24*60 {
			t.Errorf("parse %q: start time should be in current day", tc.A)
		}
		if c.End >= 2*24*60 {
			t.Errorf("parse %q: start time should be before end of next day", tc.A)
		}
	}
}

var saveExhaustiveClockRange = flag.Bool("save-exhaustive-clock-range", false, `go test -run='^TestParseClockRangeExhaustive$' ./scraper/ -save-exhaustive-clock-range`)

func TestParseClockRangeExhaustive(t *testing.T) {
	// the full range of times in most formats (intended to cover all important
	// logic, but not formatting normalization)
	times := func(yield func(string) bool) {
		if !yield("noon") || !yield("midnight") {
			return
		}
		for hour := range 24 {
			for minute := range 60 {
				// don't bother trying every single minute, but at least get the main ones
				if minute%5 != 0 && minute != 1 && minute != 59 {
					continue
				}
				// this will be accepted as valid for both 12 and 24h
				str := []byte{
					'0' + byte(hour/10),
					'0' + byte(hour%10),
					':',
					'0' + byte(minute/10),
					'0' + byte(minute%10),
				}
				if str[0] == '0' {
					str = str[1:]
				}
				if !yield(string(str)) {
					return
				}
				if !yield(string(append(str, 'a', 'm'))) {
					return
				}
				if !yield(string(append(str, 'p', 'm'))) {
					return
				}
			}
		}
	}
	var out bytes.Buffer
	for lhs := range times {
		for rhs := range times {
			s := lhs + "-" + rhs
			r, ok := parseClockRange(s)
			if !ok {
				fmt.Fprintf(&out, "%-16s ERR\n", s)
			} else {
				fmt.Fprintf(&out, "%-16s %s\n", s, r.Format(false))
			}
		}
	}
	nbuf := out.Bytes()
	if *saveExhaustiveClockRange {
		if err := os.WriteFile("clockrange.txt", nbuf, 0644); err != nil {
			panic(err)
		}
	}
	if obuf, err := os.ReadFile("clockrange.txt"); err == nil {
		next1, stop1 := iter.Pull(bytes.Lines(obuf))
		defer stop1()
		next2, stop2 := iter.Pull(bytes.Lines(nbuf))
		defer stop2()
		for {
			line1, ok1 := next1()
			line2, ok2 := next2()
			if !ok1 && !ok2 {
				break
			}
			if !ok1 || !ok2 {
				panic("line count mismatch")
			}
			if !bytes.Equal(line1, line2) {
				t.Errorf("diff:\n\told: %s\n\tnew: %s", line1[:len(line1)-1], line2[:len(line2)-1])
			}
		}
	}
	sha := sha1.Sum(nbuf)
	if hex.EncodeToString(sha[:]) != "13a861e0e6905512d39fb0b36053c424e1056c5e" {
		t.Errorf("changed: %x", sha)
	}
}

func TestParseDateRange(t *testing.T) {
	for _, tc := range []struct {
		S        string // delimit prefix/range with {}
		From, To schema.Date
	}{
		// a representative sample of real examples
		//
		// printf '%s:data.json\n' $(git -C data log --pretty=format:%H data.json) | xargs git show | jq -r '.facilities[].scheduleGroups[].schedules[].caption' | sort -u
		{"Alexander Community Centre - racquet sports", 0, 0},
		{"Bearbrook Outdoor Pool - swim and aquafit{ - }August 30 to September 1", 8_30_0, 9_01_0},
		{"Bearbrook Outdoor Pool - swimming{ - }August 2 to 4", 8_02_0, 8_04_0},
		{"Bearbrook Outdoor Pool - swimming{ - }Tuesday, July 1", 7_01_3, 7_01_3},
		{"Beaverbrook outdoor pool - swim{ - }June 14 to 29", 6_14_0, 6_29_0},
		{"Bob MacQuarrie Recreation Complex - Orléans - group fitness{ - }starting September 8", 9_08_0, 0_0},
		{"Bob MacQuarrie Recreation Complex-Orléans - Skating{ - }September 3, 2025 to March 29, 2026", 2025_09_03_0, 2026_03_29_0},
		{"Canterbury Pool - all drop-ins{ - }July 1", 7_01_0, 7_01_0},
		{"Diane Deans Greenboro Community Centre - weight and cardio room{ - }until June 29", 0, 6_29_0},
		{"Kanata Seniors Centre - weekly drop-in activities{ - }June, July, August", 0, 0},
		{"Nepean Seniors Centre", 0, 0},
		{"Nepean Sportsplex - racquet sports{ - }May 17 to 19", 5_17_0, 5_19_0},
		{"Plant Recreation Centre - group fitness{ - }Monday, August 25 to Friday, August 29", 8_25_2, 8_29_6},
		{"Ray Friel Recreation Complex - skating - Labour Day", 0, 0},
		{"Walter Baker Sports Centre - Weight and cardio room", 0, 0},
		{"Plant Recreation Centre - all drop-ins{ - }August 30 and 31", 8_30_0, 8_31_0},

		// special case (needs alternate regexp)
		{"Lowertown Pool - March Break drop-in{ - }March 14 to 22", 3_14_0, 3_22_0},

		// synthetic test cases
		{"test{ - }dummy January 1", 0, 0},
		{"test{ - }until January 1", 0, 1_01_0},
		{"test{ - }January", 0, 0},
		{"test{ - }January - January", 0, 0},
		{"test{ - }January, January", 0, 0},
		{"test{ - }January 1 and 1", 0, 0},
		{"test{ - }January 1 and 2", 1_01_0, 1_02_0},
		{"test{ - }January 1 and 3", 0, 0},
		{"test{ - }Tuesday", 0, 0},
		{"test{ - }Tuesday - Thursday", 0, 0},
		{"test{ - }until February 29, 2001", 0, 0},
		{"test{ - }until February 28, 20aa", 0, 0},
		{"test{ - }until January 1 February", 0, 0},

		// synthetic test cases (alternate regexp)
		{"test - March test-thing{ - }until January 1", 0, 1_01_0},
		{"test - March test-thing{ - }January 1 and 2", 1_01_0, 1_02_0},
		{"test - March test-thing{ - }January 1, 2026 to December 31, 2026", 2026_01_01_0, 2026_12_31_0},
		{"test - March test-thing{ - }January 1 to 31", 1_01_0, 1_31_0},
		// TODO: more
	} {
		tcP, sep, _ := strings.Cut(tc.S, "{")
		sep, tcR, _ := strings.Cut(sep, "}")
		tc.S = strings.ReplaceAll(strings.ReplaceAll(tc.S, "{", ""), "}", "")
		_ = sep

		if tc.S == "" && cmp.Or(tcP, tcR) != "" {
			panic("invalid test case")
		}
		if tcR == "" && cmp.Or(tc.From, tc.To) != 0 {
			panic("invalid test case")
		}
		if tcR == "" && tc.S != tcP {
			panic("invalid test case")
		}
		prefix, dates, ok := cutDateRange(tc.S)
		if !ok {
			if tcR != "" {
				t.Errorf("expected cut(%q) to match, got none", tc.S)
			}
			continue
		}
		if tcR == "" {
			t.Errorf("expected cut(%q) to not match, got (%q, %q)", tc.S, prefix, dates)
			continue
		}
		if tcP != prefix || tcR != dates {
			t.Errorf("expected cut(%q) to be (%q, %q), got (%q, %q)", tc.S, tcP, tcR, prefix, dates)
			continue
		}
		r, ok := parseDateRange(dates)
		if !ok {
			if tc.From != 0 || tc.To != 0 {
				t.Errorf("expected parse(%q) to succeed", dates)
			}
			continue
		}
		if tc.From == 0 && tc.To == 0 {
			t.Errorf("expected parse(%q) to fail, got %q(%#v,%#v)", dates, r.String(), r.From, r.To)
		}
		if tc.From != r.From || tc.To != r.To {
			t.Errorf("expected parse(%q) to be %q(%#v,%#v), got %q(%#v,%#v)", dates, schema.DateRange{From: tc.From, To: tc.To}.String(), tc.From, tc.To, r.String(), r.From, r.To)
		}
		if r.From != 0 {
			if _, ok := r.From.Month(); !ok {
				t.Errorf("bad invariant: parseDateRange should have a month set on from")
			}
			if _, ok := r.From.Day(); !ok {
				t.Errorf("bad invariant: parseDateRange should have a day set on from")
			}
		}
		if r.To != 0 {
			if _, ok := r.To.Month(); !ok {
				t.Errorf("bad invariant: parseDateRange should have a month set on to")
			}
			if _, ok := r.To.Day(); !ok {
				t.Errorf("bad invariant: parseDateRange should have a day set on to")
			}
		}
	}
}

func TestParseLooseDate(t *testing.T) {
	for _, tc := range []struct {
		S string
		D schema.Date
	}{
		{"Monday, October 1", 10_01_2},
		{"Monday, October 6, 2025", 2025_10_06_2},
		{"October 6, 2025", 2025_10_06_0},
		{"October 6 2025", 2025_10_06_0},
		{"6 October 2025", 2025_10_06_0},
		{"6 October 2025 Monday", 2025_10_06_2},
		{"Mon 6 October 2025", 2025_10_06_2},
		{"Mon 6 Oct 2025", 2025_10_06_2},
		{"Mon 06 Oct 2025", 2025_10_06_2},
		{"mON 06 OCT\u00a02025", 2025_10_06_2},
		{"29 October 2025", 2025_10_29_0},
		{"29 October", 10_29_0},
		{"29 Oct", 10_29_0},
		{"Monday, October, 2025", 2025_10_00_2},
		{"October, 2025", 2025_10_00_0},
		{"Monday 2025", 2025_00_00_2},

		{"sun", 1},
		{"mon", 2},
		{"tue", 3},
		{"wed", 4},
		{"thu", 5},
		{"fri", 6},
		{"sat", 7},

		{"jan", 1_00_0},
		{"feb", 2_00_0},
		{"mar", 3_00_0},
		{"apr", 4_00_0},
		{"may", 5_00_0},
		{"jun", 6_00_0},
		{"jul", 7_00_0},
		{"aug", 8_00_0},
		{"sep", 9_00_0},
		{"oct", 10_00_0},
		{"nov", 11_00_0},
		{"dec", 12_00_0},

		{"Monday Mon, October 6, 2025", 0},  // duplicate weekday
		{"Monday, October Oct 6, 2025", 0},  // duplicate monthv
		{"Monday, October 06 6, 2025", 0},   // duplicate day
		{"Monday, October 6, 2025 2025", 0}, // duplicate year
		{"Monday, October 1, 2025", 0},      // wrong weekday
		{"Mon 6 Oc 2025", 0},                // month too short
		{"Mo 6 Oct 2025", 0},                // weekday too short
		{"Mon 006 Oct 2025", 0},             // day too long
		{"Mon 0006 Oct 2025", 0},            // day too long

		// TODO: more
	} {
		d, ok := parseLooseDate(tc.S)
		if !ok {
			if tc.D != 0 {
				t.Errorf("parse %q: unexpected error", tc.S)
			}
			continue
		}
		if tc.D == 0 {
			t.Errorf("parse %q: expected error", tc.S)
			continue
		}
		if tc.D != d {
			t.Errorf("parse %q: expected %#v, got %#v", tc.S, tc.D, d)
			continue
		}
	}
}

func TestCleanActivityName(t *testing.T) {
	for _, tc := range [][]string{
		// age min
		{"example 123 test 15+",
			"example 123 test 15+",
			"example 123 (15+) test",
			"example 123 15+ test",
			"example 123 test 15 +",
			"15 + example 123 test",
			"15+ example 123 test"},
		{"example 15+ test 16+", // ambiguous
			"example 15+ test 16+"},
		{"example - test 15+", // separator collapsing
			"example - 15+ - test",
			"example - 15+ test",
			"example - test - 15+",
			"example 15+ - test",
			"example (15 +) - test"},

		// reservation requirement
		{"example",
			"example *reservations not required",
			"example *reservation not required",
			"example *reservations are not required",
			"example *reservation is not required",
			"example *reservation is required",
			"example *reservations are required",
			"example *requires reservations",
			"example *requires reservation",
			"example *reservations required",
			"example *reservation required",
			"example *  RESERVATION    ReQuIrEd.    "},
		{"example *reservations not required. blah blah blah", // unrecognized suffix, don't remove
			"example *reservations not required. blah blah blah"},

		// reduced capacity
		{"test - reduced capacity",
			"test - reduced capacity",
			"test reduced capacity",
			"test reduced capacity  ",
			"reduced capacity - test",
			"reduced capacity test",
			"reduced test"},
		{"test reduced capacity not", // not at start/end
			"test reduced capacity not"},

		// combined
		{"test 5+ - reduced capacity",
			"reduced 5+ test",
			"reduced 5+ test *reservation not required",
			"test (5+) - reduced capacity",
			"test (5+) - reduced capacity  * reservations required"},
	} {
		for _, x := range tc[1:] {
			if y := cleanActivityName(x); y != tc[0] {
				t.Errorf("clean(%q) != %q, got %q", x, tc[0], y)
			}
		}
	}
	t.Run("Actual", func(t *testing.T) {
		for _, tc := range []struct{ A, B string }{
			// actual activity names from April to September 2025
			//
			// note: update these tests as the cleanup is improved
			//
			// printf '%s:data.json\n' $(git -C data log --pretty=format:%H data.json) | xargs git show | jq -r '.facilities[].scheduleGroups[].schedules[].activities[].label' | sort -u
			{"18+ Pick-up Hockey", "pick-up hockey 18+"},
			{"18+ Pick-up hockey", "pick-up hockey 18+"},
			{"20 - 20 - 20", "20 - 20 - 20"},
			{"20/20/20", "20/20/20"},
			{"50 + Swim", "swim 50+"},
			{"50 + Vitality", "vitality 50+"},
			{"50 + swim", "swim 50+"},
			{"50+ Swim", "swim 50+"},
			{"50+ Vitality", "vitality 50+"},
			{"50+ skate", "skate 50+"},
			{"50+ skating", "skate 50+"},
			{"50+ swim", "swim 50+"},
			{"50+ vitality", "vitality 50+"},
			{"500", "500"},
			{"Adult (18+) skating", "adult skate 18+"},
			{"Adult 18+ skating", "adult skate 18+"},
			{"Adult skate", "adult skate"},
			{"Adult skating", "adult skate"},
			{"Alternate needs swim", "alternate needs swim"},
			{"Alternate needs swim *Reservations not required", "alternate needs swim"},
			{"Alternate needs swim *Reservations required", "alternate needs swim"},
			{"Aqua - general", "aqua - general"},
			{"Aqua General - Deep", "aqua general - deep"},
			{"Aqua General - Shallow", "aqua general - shallow"},
			{"Aqua general", "aqua general"},
			{"Aqua general - 25m pool shallow", "aqua general - 25m pool shallow"},
			{"Aqua general - Shallow/Deep combo", "aqua general - shallow/deep combo"},
			{"Aqua general - deep", "aqua general - deep"},
			{"Aqua general - shallow", "aqua general - shallow"},
			{"Aqua general - shallow/deep combo", "aqua general - shallow/deep combo"},
			{"Aqua general - therapeutic pool", "aqua general - therapeutic pool"},
			{"Aqua general deep", "aqua general deep"},
			{"Aqua general shallow", "aqua general shallow"},
			{"Aqua general – shallow", "aqua general - shallow"},
			{"Aqua general – therapeutic pool", "aqua general - therapeutic pool"},
			{"Aqua lite", "aqua lite"},
			{"Aqua lite - 25m pool", "aqua lite - 25m pool"},
			{"Aqua lite - 25m pool shallow", "aqua lite - 25m pool shallow"},
			{"Aqua lite - therapeutic pool", "aqua lite - therapeutic pool"},
			{"Aqua lite - warm pool", "aqua lite - warm pool"},
			{"Aqua lite – therapeutic pool", "aqua lite - therapeutic pool"},
			{"Aqua therapy", "aqua therapy"},
			{"Aquafit", "aquafit"},
			{"Aquafit - Deep", "aquafit - deep"},
			{"Aquafit - Shallow", "aquafit - shallow"},
			{"Aquafit - Zumba®", "aquafit - zumba"},
			{"Aquafit - deep", "aquafit - deep"},
			{"Aquafit - general", "aquafit - general"},
			{"Aquafit - general - 25m pool", "aquafit - general - 25m pool"},
			{"Aquafit - general deep", "aquafit - general deep"},
			{"Aquafit - general shallow", "aquafit - general shallow"},
			{"Aquafit - lite", "aquafit - lite"},
			{"Aquafit - shallow", "aquafit - shallow"},
			{"Aquafit Lite", "aquafit lite"},
			{"Aquafit general", "aquafit general"},
			{"Aquafit general - Deep", "aquafit general - deep"},
			{"Aquafit general - Shallow", "aquafit general - shallow"},
			{"Aquafit general - deep", "aquafit general - deep"},
			{"Aquafit general - shallow", "aquafit general - shallow"},
			{"Aquafit general deep", "aquafit general deep"},
			{"Aquafit general shallow", "aquafit general shallow"},
			{"Aquafit general shallow and deep - 50m pool", "aquafit general shallow and deep - 50m pool"},
			{"Aquafit lite", "aquafit lite"},
			{"Aquafit – general deep", "aquafit - general deep"},
			{"Aquafit – general shallow", "aquafit - general shallow"},
			{"Aqualite - 25m pool", "aqua lite - 25m pool"},
			{"Art studio", "art studio"},
			{"Badminton", "badminton"},
			{"Badminton - 16+", "badminton 16+"},
			{"Badminton - Feather advanced - adult", "badminton - feather advanced - adult"},
			{"Badminton - Feather advanced – adult", "badminton - feather advanced - adult"},
			{"Badminton - Plastics advanced - adult", "badminton - plastics advanced - adult"},
			{"Badminton - Plastics advanced – adult", "badminton - plastics advanced - adult"},
			{"Badminton - adult", "badminton - adult"},
			{"Badminton - family", "badminton - family"},
			{"Badminton - family (parent with child)", "badminton - family (parent with child)"},
			{"Badminton - parent with child", "badminton - parent with child"},
			{"Badminton - youth", "badminton - youth"},
			{"Badminton 16 +", "badminton 16+"},
			{"Badminton 16+", "badminton 16+"},
			{"Badminton doubles - adult", "badminton doubles - adult"},
			{"Badminton doubles - all ages", "badminton doubles - all ages"},
			{"Badminton doubles - family", "badminton doubles - family"},
			{"Badminton family - parent with child", "badminton family - parent with child"},
			{"Badminton – adult", "badminton - adult"},
			{"Balance and stability (Heartwise) - older adult", "balance and stability (heartwise) - older adult"},
			{"Balance and strength (Heartwise) - older adult", "balance and strength (heartwise) - older adult"},
			{"Ball hockey - 50+", "ball hockey 50+"},
			{"Ball hockey - child", "ball hockey - child"},
			{"Basketball", "basketball"},
			{"Basketball - Adult", "basketball - adult"},
			{"Basketball - Child", "basketball - child"},
			{"Basketball - Youth", "basketball - youth"},
			{"Basketball - adult", "basketball - adult"},
			{"Basketball - child", "basketball - child"},
			{"Basketball - child (ages 8-12)", "basketball - child (ages 8-12)"},
			{"Basketball - family", "basketball - family"},
			{"Basketball - family (children ages 6 to 14)", "basketball - family (children ages 6 to 14)"},
			{"Basketball - family (parent with child)", "basketball - family (parent with child)"},
			{"Basketball - older youth", "basketball - older youth"},
			{"Basketball - youth", "basketball - youth"},
			{"Basketball - youth (13 to 14 years)", "basketball - youth (13 to 14 years)"},
			{"Basketball - youth (15 to 17 years)", "basketball - youth (15 to 17 years)"},
			{"Basketball - youth (ages 13-17)", "basketball - youth (ages 13-17)"},
			{"Basketball 12+", "basketball 12+"},
			{"Basketball 16+", "basketball 16+"},
			{"Basketball Family - parent with child", "basketball family - parent with child"},
			{"Basketball – child (ages 8-12)", "basketball - child (ages 8-12)"},
			{"Basketball – youth (ages 13-17)", "basketball - youth (ages 13-17)"},
			{"Bid Euchre", "bid euchre"},
			{"Billiards", "billiards"},
			{"Book club", "book club"},
			{"Bootcamp", "bootcamp"},
			{"Bridge", "bridge"},
			{"Canasta", "canasta"},
			{"Cardio", "cardio"},
			{"Cardio 50+", "cardio 50+"},
			{"Cardio 50+ *Reservations not required.", "cardio 50+"},
			{"Cardio and Strength", "cardio and strength"},
			{"Cardio and Strength - older adult", "cardio and strength - older adult"},
			{"Cardio and Strength 50+", "cardio and strength 50+"},
			{"Cardio and Strength 50+ *Reservations not required.", "cardio and strength 50+"},
			{"Cardio and strength", "cardio and strength"},
			{"Cardio and strength - 50+", "cardio and strength 50+"},
			{"Cardio and strength - older adult", "cardio and strength - older adult"},
			{"Cardio and strength - older adult lite", "cardio and strength - older adult lite"},
			{"Cardio and strength 50+", "cardio and strength 50+"},
			{"Cardio and strength lite - older adult", "cardio and strength lite - older adult"},
			{"Cardio and strength – older adult", "cardio and strength - older adult"},
			{"Cardio and weight room", "cardio and weight room"},
			{"Carpet bowling", "carpet bowling"},
			{"Child Hockey (ages 6 to 12)", "child hockey (ages 6 to 12)"},
			{"Child hockey (6 to 12 years)", "child hockey (6 to 12 years)"},
			{"Chronic pain", "chronic pain"},
			{"Contract Bridge", "contract bridge"},
			{"Core Conditioning", "core conditioning"},
			{"Core Conditioning 50+", "core conditioning 50+"},
			{"Core conditioning", "core conditioning"},
			{"Core conditioning - 50+", "core conditioning 50+"},
			{"Craft club", "craft club"},
			{"Crafts", "crafts"},
			{"Creative crafts", "creative crafts"},
			{"Cribbage", "cribbage"},
			{"Dance", "dance"},
			{"Dance (Zumba)", "dance (zumba)"},
			{"Discussion group", "discussion group"},
			{"Dog swim", "dog swim"},
			{"Dominoes", "dominoes"},
			{"Drums Alive", "drums alive"},
			{"Drums Alive®", "drums alive"},
			{"Drums Alive® 50+", "drums alive 50+"},
			{"Duplicate Bridge", "duplicate bridge"},
			{"Duplicate bridge", "duplicate bridge"},
			{"Euchre", "euchre"},
			{"Family Skate *Reservations not required.", "family skate"},
			{"Family skate", "family skate"},
			{"Family skating", "family skate"},
			{"Family swim", "family swim"},
			{"Figure skating", "figure skate"},
			{"Figure skating - 18+", "figure skate 18+"},
			{"Fun Bridge", "fun bridge"},
			{"Fun bridge", "fun bridge"},
			{"Geriatric Jazz Band", "geriatric jazz band"},
			{"HIIT", "hiit"},
			{"Half Lane Swim", "half lane swim"},
			{"Half lane swim", "half lane swim"},
			{"Half lane swim *Reservations not required.", "half lane swim"},
			{"Hockey - child (6 to 12 years)", "hockey - child (6 to 12 years)"},
			{"Hockey 18+", "hockey 18+"},
			{"Hockey 35+", "hockey 35+"},
			{"Hot tub and Steam Room", "hot tub and steam room"},
			{"Hot tub and sauna", "hot tub and sauna"},
			{"Hot tub and steam room", "hot tub and steam room"},
			{"Indoor Cycling", "indoor cycling"},
			{"Indoor cycling", "indoor cycling"},
			{"Indoor cycling *Requires reservations", "indoor cycling"},
			{"Indoor cycling *Reservations required", "indoor cycling"},
			{"Instructional bridge", "instructional bridge"},
			{"Intervals", "intervals"},
			{"Kindergym", "kindergym"},
			{"Kindergym *Reservations not required", "kindergym"},
			{"Kindergym *Reservations not required.", "kindergym"},
			{"Lane Swim", "lane swim"},
			{"Lane Swim *Reservations not required.", "lane swim"},
			{"Lane Swim - reduced capacity", "lane swim - reduced capacity"},
			{"Lane Swim – reduced capacity", "lane swim - reduced capacity"},
			{"Lane swim", "lane swim"},
			{"Lane swim (shared pool)", "lane swim (shared pool)"},
			{"Lane swim *Reservations not required.", "lane swim"},
			{"Lane swim - 25m pool", "lane swim - 25m pool"},
			{"Lane swim - 25m pool, reduced capacity", "lane swim - 25m pool, - reduced capacity"},
			{"Lane swim - 50m long course", "lane swim - 50m long course"},
			{"Lane swim - 50m short course", "lane swim - 50m short course"},
			{"Lane swim - long course", "lane swim - long course"},
			{"Lane swim - reduced", "lane swim - reduced capacity"},
			{"Lane swim - reduced capacity", "lane swim - reduced capacity"},
			{"Lane swim - reduced capacity (shared pool) *Reservations not required.", "lane swim - reduced capacity (shared pool)"},
			{"Lane swim - reduced capacity *Reservations not required.", "lane swim - reduced capacity"},
			{"Lane swim - shared pool", "lane swim - shared pool"},
			{"Lane swim - short course", "lane swim - short course"},
			{"Mah Jong", "mah jong"},
			{"Movies", "movies"},
			{"Open Gym", "open gym"},
			{"Open Gym - family", "open gym - family"},
			{"Open Gym - preschool (4 to 6 years old)", "open gym - preschool (4 to 6 years old)"},
			{"Open Gym - youth", "open gym - youth"},
			{"Open Gym- youth", "open gym- youth"},
			{"Open gym", "open gym"},
			{"Open gym *Reservations not required", "open gym"},
			{"Open gym *Reservations not required.", "open gym"},
			{"Open gym - child (7 to 12 years old)", "open gym - child (7 to 12 years old)"},
			{"Open gym - child (ages 6 to 12)", "open gym - child (ages 6 to 12)"},
			{"Open gym - child (ages 6 to 14)", "open gym - child (ages 6 to 14)"},
			{"Open gym - child (children ages 6 to 12)", "open gym - child (children ages 6 to 12)"},
			{"Open gym - child (children ages 6 to 14)", "open gym - child (children ages 6 to 14)"},
			{"Open gym - family", "open gym - family"},
			{"Open gym - family (children ages 6 to 12)", "open gym - family (children ages 6 to 12)"},
			{"Open gym - family (children ages 6 to 14)", "open gym - family (children ages 6 to 14)"},
			{"Open gym - family *Reservations not required", "open gym - family"},
			{"Open gym - preschool (4 to 6 years old)", "open gym - preschool (4 to 6 years old)"},
			{"Open gym - youth", "open gym - youth"},
			{"Open gym - youth (13 to 17 years old)", "open gym - youth (13 to 17 years old)"},
			{"Open gym – child (7 to 12 years old)", "open gym - child (7 to 12 years old)"},
			{"Open gym – preschool (4 to 6 years old)", "open gym - preschool (4 to 6 years old)"},
			{"Open gym – youth (13 to 17 years old)", "open gym - youth (13 to 17 years old)"},
			{"Open paint studio", "open paint studio"},
			{"Outdoor bootcamp", "outdoor bootcamp"},
			{"Pick Up Hockey 18+", "pick-up hockey 18+"},
			{"Pick-Up Hockey 18+", "pick-up hockey 18+"},
			{"Pick-Up Hockey 35+", "pick-up hockey 35+"},
			{"Pick-Up Hockey 50+", "pick-up hockey 50+"},
			{"Pick-up Hockey 18+", "pick-up hockey 18+"},
			{"Pick-up Hockey 35+", "pick-up hockey 35+"},
			{"Pick-up hockey 18+", "pick-up hockey 18+"},
			{"Pick-up hockey 35+", "pick-up hockey 35+"},
			{"Pick-up hockey 35+ *Reservations required.", "pick-up hockey 35+"},
			{"Pickleball", "pickleball"},
			{"Pickleball - Rotations", "pickleball - rotations"},
			{"Pickleball - adult", "pickleball - adult"},
			{"Pickleball - adult - intermediate", "pickleball - adult - intermediate"},
			{"Pickleball - family", "pickleball - family"},
			{"Pickleball - family (parent with child)", "pickleball - family (parent with child)"},
			{"Pickleball - intermediate", "pickleball - intermediate"},
			{"Pickleball - rotations", "pickleball - rotations"},
			{"Pickleball 16+", "pickleball 16+"},
			{"Pickleball 18+ - recreational play", "pickleball - recreational play 18+"},
			{"Pickleball 50+", "pickleball 50+"},
			{"Pickleball family - parent with child", "pickleball family - parent with child"},
			{"Pickleball intermediate 16+", "pickleball intermediate 16+"},
			{"Pilates", "pilates"},
			{"Pilates *Reservations not required.", "pilates"},
			{"Pilates 50 +", "pilates 50+"},
			{"Pilates 50+", "pilates 50+"},
			{"Preschool Swim", "preschool swim"},
			{"Preschool Swim *Reservations not required.", "preschool swim"},
			{"Preschool swim", "preschool swim"},
			{"Preschool swim (shared pool)", "preschool swim (shared pool)"},
			{"Preschool swim *Reservations not required", "preschool swim"},
			{"Preschool swim *Reservations not required.", "preschool swim"},
			{"Preschool swim - 25m pool", "preschool swim - 25m pool"},
			{"Preschool swim - 25m pool shallow", "preschool swim - 25m pool shallow"},
			{"Preschool swim - children's pool", "preschool swim - children's pool"},
			{"Preschool swim - leisure pool only *Reservations not required.", "preschool swim - leisure pool only"},
			{"Preschool swim - therapeutic pool", "preschool swim - therapeutic pool"},
			{"Public Skate *Reservations not required.", "public skate"},
			{"Public Skate 50+ *Reservations not required.", "public skate 50+"},
			{"Public Skating", "public skate"},
			{"Public Swim", "public swim"},
			{"Public Swim *Reservations not required.", "public swim"},
			{"Public Swim - reduced capacity (shared pool) *Reservations not required.", "public swim - reduced capacity (shared pool)"},
			{"Public Swim - reduced capacity *Reservations not required.", "public swim - reduced capacity"},
			{"Public Swim with WIBIT *Reservations not required.", "public swim with wibit"},
			{"Public skate", "public skate"},
			{"Public skate 50+", "public skate 50+"},
			{"Public skating", "public skate"},
			{"Public swim", "public swim"},
			{"Public swim *Reservations not required", "public swim"},
			{"Public swim *Reservations not required.", "public swim"},
			{"Public swim - 25 metre pool only", "public swim - 25 metre pool only"},
			{"Public swim - 25m and deep end", "public swim - 25m and deep end"},
			{"Public swim - 25m pool", "public swim - 25m pool"},
			{"Public swim - 25m pool shallow", "public swim - 25m pool shallow"},
			{"Public swim - leisure pool only", "public swim - leisure pool only"},
			{"Public swim - leisure pool only *Reservations not required.", "public swim - leisure pool only"},
			{"Public swim - reduced capacity *Reservations not required.", "public swim - reduced capacity"},
			{"Public swim - therapeutic pool", "public swim - therapeutic pool"},
			{"Public swim - women's only", "public swim - women's only"},
			{"Public swim with WIBIT *Reservations not required.", "public swim with wibit"},
			{"Public swim with no slide *Reservations not required", "public swim with no slide"},
			{"Public swim with slide *Reservations not required", "public swim with slide"},
			{"Public swim women's only", "public swim women's only"},
			{"Public swim women’s only", "public swim women's only"},
			{"Public swim – therapeutic pool", "public swim - therapeutic pool"},
			{"Pétanque Atout", "pétanque atout"},
			{"Qigong", "qigong"},
			{"Racquetball - courts 4, 6, and 8", "racquetball court 4, 6, and 8"},
			{"Reduced lane swim", "lane swim - reduced capacity"},
			{"Reduced lane swim *Reservations not required.", "lane swim - reduced capacity"},
			{"Ringette", "ringette"},
			{"Ringette (10 to 14 years)", "ringette (10 to 14 years)"},
			{"Ringette (12 to 16 years)", "ringette (12 to 16 years)"},
			{"Rock Climbing 5+", "rock climbing 5+"},
			{"Rock climbing 5+", "rock climbing 5+"},
			{"Rockwall", "rockwall"},
			{"Roller hockey - adult", "roller hockey - adult"},
			{"Roller hockey - youth", "roller hockey - youth"},
			{"Roller-skating", "roller-skate"},
			{"Rollerblade - all ages", "rollerblade - all ages"},
			{"Sauna", "sauna"},
			{"Sauna *Reservations not required.", "sauna"},
			{"Sauna *reservations not required", "sauna"},
			{"Scrabble", "scrabble"},
			{"Short mat bowling", "short mat bowling"},
			{"Shuffleboard", "shuffleboard"},
			{"Sixty Six", "sixty six"},
			{"Skating 50+", "skate 50+"},
			{"Snooker/billiards", "snooker/billiards"},
			{"Soccer (youth)", "soccer (youth)"},
			{"Soccer - youth", "soccer - youth"},
			{"Social board games", "social board games"},
			{"Sounds of music", "sounds of music"},
			{"Speed skating", "speed skate"},
			{"Sport conditioning", "sports conditioning"},
			{"Sports conditioning", "sports conditioning"},
			{"Squash - courts 1, 2, 3, 4", "squash court 1, 2, 3, 4"},
			{"Squash Court 1, 2, 3 and 4", "squash court 1, 2, 3 and 4"},
			{"Squash Courts 2, 3, 4, and 5", "squash courts 2, 3, 4, and 5"},
			{"Squash courts 1, 2, 3, 5, 7 and 9", "squash courts 1, 2, 3, 5, 7 and 9"},
			{"Squash courts 2, 3, 4, 5", "squash courts 2, 3, 4, 5"},
			{"Squash courts 2,3,4 and 5", "squash courts 2,3,4 and 5"},
			{"Squash courts 2,3,4,5", "squash courts 2,3,4,5"},
			{"Step and strength", "step and strength"},
			{"Stick and Puck - preschool and child", "stick and puck - preschool and child"},
			{"Stick and Puck - preschool and child (3 to 12 years)", "stick and puck - preschool and child (3 to 12 years)"},
			{"Stick and Puck - youth and adult", "stick and puck - youth and adult"},
			{"Stick and Puck youth and adult (ages 13+)", "stick and puck youth and adult 13+"},
			{"Stick and puck - preschool and child", "stick and puck - preschool and child"},
			{"Stick and puck - youth and adult", "stick and puck - youth and adult"},
			{"Strength", "strength"},
			{"Strength - 50+", "strength 50+"},
			{"Strength 50+", "strength 50+"},
			{"Strength Circuit", "strength circuit"},
			{"Strength Circuit 50+", "strength circuit 50+"},
			{"Strength TMC -older adult", "strength tmc -older adult"},
			{"Strength and Balance - older adult", "strength and balance - older adult"},
			{"Strength and balance - older adult", "strength and balance - older adult"},
			{"Strength and balance 50+", "strength and balance 50+"},
			{"Stretch and Strength", "stretch and strength"},
			{"Stretch and Strength 50+ *Reservations not required.", "stretch and strength 50+"},
			{"Stretch and strength", "stretch and strength"},
			{"Stretch and strength - older adult", "stretch and strength - older adult"},
			{"Stretch and strength 50+", "stretch and strength 50+"},
			{"TMC", "tmc"},
			{"TMC - older adult", "tmc - older adult"},
			{"TRX", "trx"},
			{"Table Tennis - Adult", "table tennis - adult"},
			{"Table Tennis - adult", "table tennis - adult"},
			{"Table tennis", "table tennis"},
			{"Table tennis - adult", "table tennis - adult"},
			{"Tai Chi", "tai chi"},
			{"Tea and chat", "tea and chat"},
			{"The Groove Method®", "the groove method"},
			{"Ukelele", "ukelele"},
			{"Ukulele", "ukulele"},
			{"Volleyball", "volleyball"},
			{"Volleyball - adult", "volleyball - adult"},
			{"Volleyball - adult *Reservations not required", "volleyball - adult"},
			{"Volleyball - family", "volleyball - family"},
			{"Volleyball - youth", "volleyball - youth"},
			{"Volleyball - youth (13 to 17 years)", "volleyball - youth (13 to 17 years)"},
			{"Volleyball - youth *Reservations not required", "volleyball - youth"},
			{"Volleyball 16+", "volleyball 16+"},
			{"Walking club", "walking club"},
			{"Wave Swim", "wave swim"},
			{"Wave swim", "wave swim"},
			{"Weight and Cardio Room", "weight and cardio room"},
			{"Weight and Cardio room", "weight and cardio room"},
			{"Weight and cardio room", "weight and cardio room"},
			{"Weight and cardio room *Reservations not required", "weight and cardio room"},
			{"Weight and cardio room *Reservations not required.", "weight and cardio room"},
			{"Whirlpool", "whirlpool"},
			{"Women's only public swim", "women's only public swim"},
			{"Women's only swim", "women's only swim"},
			{"Women’s only public swim", "women's only public swim"},
			{"Women’s only swim", "women's only swim"},
			{"Yin Yoga", "yin yoga"},
			{"Yoga", "yoga"},
			{"Yoga - QiGong", "yoga - qigong"},
			{"Yoga - Yin", "yoga - yin"},
			{"Yoga - older adult", "yoga - older adult"},
			{"Yoga 50 +", "yoga 50+"},
			{"Yoga 50+", "yoga 50+"},
			{"Yoga Tune Up®", "yoga tune up"},
			{"Youth hockey (13 to 17 years)", "youth hockey (13 to 17 years)"},
			{"Zumba", "zumba"},
			{"Zumba / Dance", "zumba / dance"},
			{"Zumba Gold", "zumba gold"},
			{"Zumba Toning", "zumba toning"},
			{"Zumba toning", "zumba toning"},
			{"Zumba ® Toning", "zumba toning"},
			{"Zumba®", "zumba"},
			{"Zumba® Gold", "zumba gold"},
			{"Zumba® Gold 50 +", "zumba gold 50+"},
			{"Zumba® Gold 50+", "zumba gold 50+"},
		} {
			c := cleanActivityName(tc.A)
			if c != tc.B {
				t.Errorf("clean(%q) != %q, got %q", tc.A, tc.B, c)
			}
		}
	})
}

func TestMatchDomain(t *testing.T) {
	for _, tc := range [][]string{
		{".example.com",
			"example.com",
			"test.example.com",
			"EXAMPLE.com",
			"Test.Example.Com",
			"example.com.",
			"test.example.com.",
			"-",
			"-test.com",
			"-example.com.test",
			"-test.example.com.test"},
		{"example.com",
			"example.com",
			"EXAMPLE.com",
			"example.com.",
			"-",
			"-test.example.com",
			"-Test.Example.Com",
			"-test.example.com.",
			"-test.com",
			"-example.com.test",
			"-test.example.com.test"},
		{"",
			"example.com",
			"EXAMPLE.com",
			"example.com.",
			"",
			"test.example.com",
			"Test.Example.Com",
			"test.example.com.",
			"test.com",
			"example.com.test",
			"test.example.com.test"},
	} {
		for _, s := range tc {
			s, not := strings.CutPrefix(s, "-")
			if matchDomain(tc[0], &url.URL{Host: s}) != !not {
				t.Errorf("match(%q, %q) != %t", tc[0], s, !not)
			}
		}
	}
}

//go:embed schedule_test.html
var scheduleTestHTML []byte

func TestScrapeSchedule(t *testing.T) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(scheduleTestHTML))
	if err != nil {
		panic(fmt.Errorf("parse test html: %w", err))
	}
	for i, tc := range doc.Find("x-test").EachIter() {
		facilityName := tc.AttrOr("data-facility-name", "")
		if facilityName == "" {
			panic("test case must include facility name")
		}

		table := tc.Find("table")
		if table.Length() != 1 {
			panic("test case must contain exactly one table")
		}

		caption := table.Find("caption").Text()

		msg, _ := scrapeSchedule(table, facilityName)

		buf, err := protojson.MarshalOptions{
			UseProtoNames: true,
			AllowPartial:  true,
		}.Marshal(msg)
		if err != nil {
			panic(fmt.Errorf("marshal protojson: %w", err))
		}

		var obj map[string]any
		if err := json.Unmarshal(buf, &obj); err != nil {
			panic(fmt.Errorf("unmarshal protojson: %w", err))
		}

		asserts := tc.Find("x-assert")

		t.Logf("test %d: schedule %q: %d asserts", i, caption, asserts.Length())

		for _, assert := range asserts.EachIter() {
			src := assert.Text()
			title := assert.AttrOr("title", "")
			prog, err := expr.Compile(src)
			if err != nil {
				panic(fmt.Errorf("compile assert %q: %w", src, err))
			}
			if res, err := expr.Run(prog, map[string]any{
				"schedule": obj,
				"clocktime": func(hh, mm int) int {
					return int(schema.MakeClockTime(hh, mm))
				},
			}); err != nil {
				t.Log(string(buf))
				t.Errorf("test %d: schedule %q: assert %q: failed to evaluate: %v", i, caption, cmp.Or(title, src), err)
			} else if res != true {
				t.Log(string(buf))
				t.Errorf("test %d: schedule %q: assert %q: failed: result: %v", i, caption, cmp.Or(title, src), res)
			}
		}
	}
}
