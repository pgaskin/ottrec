package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"net/http/cookiejar"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/pgaskin/ottrec/internal/httpcache"
	"github.com/pgaskin/ottrec/internal/zyte"
	"github.com/pgaskin/ottrec/schema"
	textpbfmt "github.com/protocolbuffers/txtpbfmt/parser"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	Scrape       = flag.Bool("scrape", false, "parse data from pages")
	ExportProto  = flag.String("export.proto", "", "write proto to this file")
	ExportPB     = flag.String("export.pb", "", "write binpb to this file")
	ExportTextPB = flag.String("export.textpb", "", "write textpb to this file")
	ExportJSON   = flag.String("export.json", "", "write json to this file")
	ExportPretty = flag.Bool("export.pretty", false, "prettify output (-json -textpb)")

	Cache              = flag.String("cache", "", "cache pages in the specified directory")
	CachePurgeListing  = flag.Bool("cache.purge.listing", false, "remove cached facility listing")
	CachePurgeFacility = flag.Bool("cache.purge.facility", false, "remove cached facility pages")
	CachePurgeGeocode  = flag.Bool("cache.purge.geocode", false, "remove cached geocoding data")

	Fetch     = flag.Bool("fetch", false, "fetch uncached pages")
	FetchZyte = flag.Int("fetch.zyte", 0, "use zyte, allowing the specified number of paid requests (set ZYTE_APIKEY)")

	Geocodio = flag.Bool("geocodio", false, "use geocodio for geocoding (set GEOCODIO_APIKEY)")

	ScraperSecret  = os.Getenv("OTTCA_SCRAPER_SECRET")
	GeocodioAPIKey = os.Getenv("GEOCODIO_APIKEY")
	ZyteAPIKey     = os.Getenv("ZYTE_APIKEY")
)

func defaultUserAgent() string {
	var ua strings.Builder
	ua.WriteString("ottawa-rec-scraper-bot/0.1")
	if ghRepo := os.Getenv("GITHUB_REPOSITORY"); ghRepo != "" {
		ghHost := cmp.Or(os.Getenv("GITHUB_SERVER_URL"), "https://github.com")
		if _, x, ok := strings.Cut(ghHost, "://"); ok {
			ghHost = x
		}
		ua.WriteString(" (")
		ua.WriteString(ghHost)
		ua.WriteString("/")
		ua.WriteString(ghRepo)
		ua.WriteString(")")
	} else {
		ua.WriteString(" (dev)")
	}
	return ua.String()
}

func main() {
	flag.Parse()

	if b, _ := strconv.ParseBool(os.Getenv("OTTREC_DEBUG_HTTP")); b {
		next := http.DefaultTransport
		http.DefaultTransport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			fmt.Println()
			fmt.Println()
			fmt.Println()
			buf, err := httputil.DumpRequestOut(r, true)
			if err != nil {
				return nil, err
			}
			os.Stdout.Write(buf)
			fmt.Println()
			resp, err := next.RoundTrip(r)
			if err != nil {
				return nil, err
			}
			buf, err = httputil.DumpResponse(resp, true)
			if err != nil {
				return nil, err
			}
			os.Stdout.Write(buf)
			fmt.Println()
			fmt.Println()
			fmt.Println()
			return resp, nil
		})
	}

	// use zyte for some requests
	if *FetchZyte > 0 {
		next := &zyte.Transport{
			APIKey: ZyteAPIKey,
			Limit:  zyte.FixedLimit(*FetchZyte),
			Retry: func(ctx context.Context, tries, code int) bool {
				if tries >= 3 {
					return false
				}
				slog.Warn("zyte temporary error, retrying in a second")
				return true
			},
			FollowRedirect: true,
			Next:           http.DefaultTransport,
		}
		http.DefaultTransport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if matchDomain(".ottawa.ca", r.URL) {
				r2 := *r
				r2.Header = r.Header.Clone()
				r2.Header.Del("User-Agent")
				r2.Header.Del("Cookie")
				r2.Header.Del("X-Scraper-Secret")
				r = &r2
				return next.RoundTrip(r)
			}
			return next.Next.RoundTrip(r)
		})
	}

	// apply rate limits if not cached
	http.DefaultTransport = rateLimitRoundTripper(http.DefaultTransport, ".ottawa.ca", rate.NewLimiter(rate.Every(time.Second*2), 1))
	http.DefaultTransport = rateLimitRoundTripper(http.DefaultTransport, "api.geocod.io", rate.NewLimiter(rate.Every(time.Minute/1000), 1))

	// cache responses
	redactor := new(httpcache.Redactor)
	cache := &httpcache.Transport{
		Path:             *Cache,
		Fallback:         !*Fetch, // for backwards compat with old caches
		RequestRedactor:  redactor,
		ResponseRedactor: redactor,
	}
	if *Fetch {
		cache.Next = http.DefaultTransport
	}
	http.DefaultTransport = cache

	// add secrets
	if ScraperSecret != "" {
		header := "X-Scraper-Secret"
		http.DefaultTransport = headerRoundTripper(http.DefaultTransport, ".ottawa.ca", header, ScraperSecret)
		redactor.RedactRequestHeader(header, 4)
	}
	if GeocodioAPIKey != "" {
		header := "Authorization"
		http.DefaultTransport = headerRoundTripper(http.DefaultTransport, "api.geocod.io", header, "Bearer "+GeocodioAPIKey)
		redactor.RedactRequestHeader(header, 4)
	}

	// add user agent
	if ua := defaultUserAgent(); ua != "" {
		http.DefaultTransport = headerRoundTripper(http.DefaultTransport, "", "User-Agent", ua)
	}

	// set up the default http client
	http.DefaultClient.Transport = http.DefaultTransport
	http.DefaultClient.Jar, _ = cookiejar.New(nil)

	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

const (
	CacheCategoryListing  = "listing"
	CacheCategoryFacility = "facility"
	CacheCategoryGeocode  = "geocode"
)

func run(ctx context.Context) error {
	if *Cache != "" {
		slog.Info("using cache dir", "path", *Cache)
		if err := os.Mkdir(*Cache, 0777); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create cache dir: %w", err)
		}
		if err := os.WriteFile(filepath.Join(*Cache, ".gitattributes"), []byte("* -text\n"), 0666); err != nil { // no line ending conversions
			return fmt.Errorf("write cache dir gitattributes: %w", err)
		}
		var purge []string
		if *CachePurgeListing {
			slog.Info("purging cached facility listing")
			purge = append(purge, CacheCategoryListing)
		}
		if *CachePurgeFacility {
			slog.Info("purging cached facility pages")
			purge = append(purge, CacheCategoryFacility)
		}
		if *CachePurgeGeocode {
			slog.Info("purging cached geocoding data")
			purge = append(purge, CacheCategoryGeocode)
		}
		if err := httpcache.Purge(*Cache, purge...); err != nil {
			return fmt.Errorf("purge cache: %w", err)
		}
	}
	if *Fetch {
		slog.Info("will fetch data", "ua", defaultUserAgent())
	} else {
		if *Cache == "" {
			slog.Warn("no cache dir specified")
		}
		slog.Info("only using cached data")
	}
	if *Scrape {
		slog.Info("will parse data")
	} else {
		slog.Info("will not parse data")
	}
	if !*Geocodio {
		slog.Info("will geocode addresses with geocodio")
	} else {
		slog.Warn("will not geocode addresses")
	}
	if *FetchZyte > 0 {
		slog.Info("will fetch data using zyte", "limit", *FetchZyte)
	} else if ScraperSecret != "" {
		slog.Info("will fetch data using scraper secret")
	}
	var (
		data       schema.Data_builder
		geoAttrib  = map[string]struct{}{}
		listing    = "https://ottawa.ca/en/recreation-and-parks/facilities/place-listing"
		cur        = listing
		facilities int
	)
	for cur != "" {
		doc, _, err := fetchPage(ctx, CacheCategoryListing, cur)
		if err != nil {
			return err
		}

		content, err := scrapeMainContentBlock(doc)
		if err != nil {
			return err
		}

		nextURL, err := scrapePagerNext(doc, content)
		if err != nil {
			return err
		}

		if err := scrapePlaceListings(doc, content, func(u *url.URL, name, address string) error {
			var facility schema.Facility_builder
			facility.Name = name
			facility.Address = address
			facility.Source = schema.Source_builder{
				Url: u.String(),
			}.Build()
			facilities++

			if !*Geocodio {
				// skip geocoding
			} else if lng, lat, attrib, hasLngLat, err := geocode(ctx, address); err != nil {
				slog.Warn("failed to geocode place", "name", name, "address", address, "error", err)
				facility.XErrors = append(facility.XErrors, fmt.Sprintf("failed to resolve address: %v", err))
			} else if hasLngLat {
				facility.XLnglat = schema.LngLat_builder{
					Lat: float32(lat),
					Lng: float32(lng),
				}.Build()
				if attrib != "" {
					geoAttrib[attrib] = struct{}{}
				}
			}

			doc, date, err := fetchPage(ctx, CacheCategoryFacility, u.String())
			if err != nil {
				slog.Warn("failed to fetch place", "name", name, "error", err)
				facility.XErrors = append(facility.XErrors, fmt.Sprintf("failed to fetch data: %v", err))
				data.Facilities = append(data.Facilities, facility.Build())
				return nil
			} else {
				slog.Info("got place", "name", name)
			}
			if !date.IsZero() {
				facility.Source.SetXDate(timestamppb.New(date))
			}
			if !*Scrape {
				return nil
			}
			if err := func() error {
				content, err := scrapeMainContentBlock(doc)
				if err != nil {
					if tmp, err := url.Parse(cur); err == nil && !strings.EqualFold(doc.Url.Hostname(), tmp.Hostname()) {
						return fmt.Errorf("facility page %q is not a City of Ottawa webpage", doc.Url)
					}
					return err
				}

				node, err := findOne(content, `.node.node--type-place`, "place node")
				if err != nil {
					return err
				}

				if field, err := scrapeNodeField(node, "description", "text-long", false, true); err != nil {
					facility.XErrors = append(facility.XErrors, fmt.Sprintf("extract facility description: %v", err))
				} else {
					facility.Description = strings.Join(strings.Fields(field.Text()), " ")
				}

				if field, err := scrapeNodeField(node, "notification-details", "text-long", false, true); err != nil {
					facility.XErrors = append(facility.XErrors, fmt.Sprintf("extract facility notifications: %v", err))
				} else if raw, err := field.Html(); err != nil {
					facility.XErrors = append(facility.XErrors, fmt.Sprintf("extract facility notifications: %v", err))
				} else {
					facility.NotificationsHtml = raw
				}

				if field, err := scrapeNodeField(node, "hours-details", "text-long", false, true); err != nil {
					facility.XErrors = append(facility.XErrors, fmt.Sprintf("extract facility notifications: %v", err))
				} else if raw, err := field.Html(); err != nil {
					facility.XErrors = append(facility.XErrors, fmt.Sprintf("extract facility notifications: %v", err))
				} else {
					facility.SpecialHoursHtml = raw
				}

				if err := scrapeCollapseSections(node, func(label string, content *goquery.Selection) error {
					if !strings.Contains(label, "drop-in") && !strings.Contains(label, "schedule") && content.Find(`a[href*="reservation.frontdesksuite"],p:contains("schedules listed in the charts below"),th:contains("Monday")`).Length() == 0 {
						return nil // probably not a schedule group
					}
					group, xerrs := scrapeScheduleGroup(doc, facility.Name, label, content)
					facility.XErrors = append(facility.XErrors, xerrs...)
					facility.ScheduleGroups = append(facility.ScheduleGroups, group)
					return nil
				}); err != nil {
					return err
				}

				return nil
			}(); err != nil {
				facility.XErrors = append(facility.XErrors, fmt.Sprintf("failed to extract facility information: %v", err))
			}

			data.Facilities = append(data.Facilities, facility.Build())
			return nil
		}); err != nil {
			return err
		}

		if nextURL == nil {
			break
		}
		cur = nextURL.String()
	}
	if facilities < 100 {
		return fmt.Errorf("less than 100 facilities returned, something might be wrong")
	}
	if *Scrape {
		data.Attribution = append(data.Attribution, "Compiled data © Patrick Gaskin. https://github.com/pgaskin/ottrec")
		data.Attribution = append(data.Attribution, "Facility information and schedules © City of Ottawa. "+listing)
		for _, attrib := range slices.Sorted(maps.Keys(geoAttrib)) {
			data.Attribution = append(data.Attribution, "Address data "+strings.TrimPrefix(attrib, "Data "))
		}
		if err := export(data.Build()); err != nil {
			return fmt.Errorf("export: %w", err)
		}
	}
	return nil
}

func export(pb *schema.Data) error {
	if name := *ExportProto; name != "" {
		slog.Info("exporting proto", "name", name)
		if err := os.WriteFile(name, []byte(schema.Proto()), 0644); err != nil {
			return fmt.Errorf("proto: write: %w", err)
		}
	}
	if name := *ExportPB; name != "" {
		slog.Info("exporting binpb", "name", name)
		if buf, err := (proto.MarshalOptions{
			Deterministic: true,
		}).Marshal(pb); err != nil {
			return fmt.Errorf("binpb: marshal: %w", err)
		} else if err := os.WriteFile(name, buf, 0644); err != nil {
			return fmt.Errorf("binpb: write: %w", err)
		}
	}
	if name, pretty := *ExportTextPB, *ExportPretty; name != "" {
		slog.Info("exporting textpb", "name", name, "pretty", pretty)
		opt := prototext.MarshalOptions{
			Multiline:    false,
			AllowPartial: false,
			EmitASCII:    !pretty,
		}
		buf, err := opt.Marshal(pb)
		if err != nil {
			return fmt.Errorf("textpb: marshal: %w", err)
		}
		if pretty {
			buf, err = textpbfmt.FormatWithConfig(buf, textpbfmt.Config{
				ExpandAllChildren:        true,
				SkipAllColons:            true,
				AllowTripleQuotedStrings: true,
				WrapStringsAtColumn:      120,
				WrapHTMLStrings:          true,
				WrapStringsAfterNewlines: true,
			})
			if err != nil {
				return fmt.Errorf("textpb: format: %w", err)
			}
		}
		if err := os.WriteFile(name, buf, 0644); err != nil {
			return fmt.Errorf("textpb: write: %w", err)
		}
	}
	if name, pretty := *ExportJSON, *ExportPretty; name != "" {
		slog.Info("exporting json", "name", name, "pretty", pretty)
		opt := protojson.MarshalOptions{
			EmitUnpopulated:   true,
			EmitDefaultValues: true,
			Multiline:         false,
			AllowPartial:      false,
			UseEnumNumbers:    true,
			UseProtoNames:     false,
		}
		buf, err := opt.Marshal(pb)
		if err != nil {
			return fmt.Errorf("json: marshal: %w", err)
		}
		if *ExportPretty {
			var buf1 bytes.Buffer
			if err := json.Indent(&buf1, buf, "", "  "); err != nil {
				return fmt.Errorf("json: format: %w", err)
			}
			buf = buf1.Bytes()
		}
		if err := os.WriteFile(name, buf, 0644); err != nil {
			return fmt.Errorf("json: write: %w", err)
		}
	}
	return nil
}

// geocode geocodes an address using geocodio.
//
// As of 2025-09-16, geocodio works better than nominatim and
// pelias/geocode.earth:
//
//   - Nominatim has free public instances with no api key required.
//   - Pelias has a hosted instance at geocode.earth free for open-source projects.
//   - Geocodio has a free tier.
//   - Other geocoding services are expensive or do not allow storing and creating derivative works from the results.
//   - Geocodio specializes in Canada/US addresses and is the best at resolving addresses with incorrect street names or containing subdivision names.
//   - Nominatim is fine for well-formed addresses, but is overly strict and fails to geocode ones that Geocodio can.
//   - Pelias and Geocodio resolve all addresses successfully.
//   - Pelias is better than Geocodio at choosing a point near the entrance instead of somewhere on the property.
//   - For incorrect street names, Geocodio is better at resolving them based on the postal code, but Pelias just ignores the street and chooses somewhere seemingly random.
func geocode(ctx context.Context, addr string) (lng, lat float64, attrib string, ok bool, err error) {
	defer func() {
		if lat2, lng2, ok2 := overrideGeocode(addr); ok2 { // do it after the fetch so we at least have it cached for comparison later
			var old slog.Attr
			if err != nil {
				old = slog.Group("old", "ok", false, "error", err)
			} else if !ok {
				old = slog.Group("old", "ok", false)
			} else {
				old = slog.Group("old", "ok", true, "lat", lat, "lng", lng, "attrib", attrib)
			}
			slog.LogAttrs(ctx, slog.LevelInfo, "overriding geocode result", old, slog.Group("new", "lat", lat2, "lng", lng2))
			lat, lng, attrib, ok, err = lat2, lng2, "", true, nil
		}
	}()

	u := &url.URL{
		Scheme: "https",
		Host:   "api.geocod.io",
		Path:   "/v1.9/geocode",
		RawQuery: url.Values{
			"q":       {addr},
			"country": {"CA"},
		}.Encode(),
	}
	slog.Info("fetch geocodio", "url", u.String())

	resp, err := fetch(ctx, CacheCategoryGeocode, u.String())
	if err != nil {
		return 0, 0, "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var obj struct {
			Error string
		}
		if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil || obj.Error == "" {
			return 0, 0, "", false, fmt.Errorf("response status %d", resp.StatusCode)
		}
		return 0, 0, "", false, fmt.Errorf("response status %d: geocodio error: %q", resp.StatusCode, obj.Error)
	}

	var obj struct {
		Results []struct {
			Location struct {
				Lat float64
				Lng float64
			}
			Source string
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return 0, 0, "", false, fmt.Errorf("decode geocodio response: %w", err)
	}
	if len(obj.Results) != 0 {
		r := obj.Results[0]
		if r.Location.Lat == 0 || r.Location.Lng == 0 {
			return 0, 0, "", false, fmt.Errorf("decode geocodio response: missing lng/lat")
		}
		return r.Location.Lng, r.Location.Lat, "via geocodio (" + r.Source + ")", true, nil
	}
	return 0, 0, "", false, nil
}

func fetchPage(ctx context.Context, category, u string) (*goquery.Document, time.Time, error) {
	slog.Info("fetch page", "url", u, "category", category)

	resp, err := fetch(ctx, category, u)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, time.Time{}, err
	}
	doc.Url = resp.Request.URL

	if doc.Find(`#main-content, #ottux-header, meta[name='dcterms.title'], meta[content*='drupal']`).Length() == 0 {
		if h, _ := doc.Html(); strings.Contains(h, "Pardon Our Interruption") || strings.Contains(h, "showBlockPage()") || strings.Contains(h, "Request unsuccessful. Incapsula incident ID: ") {
			return nil, time.Time{}, fmt.Errorf("imperva blocked request")
		}
		return nil, time.Time{}, fmt.Errorf("page content not found, might be imperva")
	}

	date, _ := time.Parse(http.TimeFormat, resp.Header.Get("Date"))
	return doc, date, nil
}

func fetch(ctx context.Context, category, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(httpcache.CategoryContext(ctx, category), http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response status %d", resp.StatusCode)
	}
	return resp, nil
}

// resolve resolves a href from against the document.
func resolve(d *goquery.Document, href string) (*url.URL, error) {
	var err error
	u := d.Url
	if base, _ := d.Find("base").Attr("href"); base != "" {
		if u, err = u.Parse(base); err != nil {
			return nil, fmt.Errorf("parse base href %q: %w", base, err)
		}
	}
	if href != "" {
		if u, err = u.Parse(href); err != nil {
			return nil, fmt.Errorf("parse href %q: %w", href, err)
		}
	}
	return u, nil
}

func findOne(s *goquery.Selection, sel, what string) (*goquery.Selection, error) {
	if s == nil {
		return nil, fmt.Errorf("%s (%#q) not found", what, sel)
	}

	s = s.Find(sel)
	if n := s.Length(); n == 0 {
		return nil, fmt.Errorf("%s (%#q) not found", what, sel)
	} else if n > 1 {
		return nil, fmt.Errorf("multiple (%d) %s (%#q) found", n, what, sel)
	}
	return s, nil
}

// scrapeMainContentBlock extracts the main content block from a City of Ottawa
// page.
func scrapeMainContentBlock(doc *goquery.Document) (*goquery.Selection, error) {
	return findOne(doc.Selection, `#block-mainpagecontent`, "main page content wrapper")
}

// scrapePagerNext extracts the next paginated URL from a section of a City of
// Ottawa page, returning nil if there is no next page.
func scrapePagerNext(doc *goquery.Document, s *goquery.Selection) (*url.URL, error) {
	pager, err := findOne(s, `nav.pagerer-pager-basic[role="navigation"]`, "accessiblepager widget")
	if err != nil {
		return nil, err
	}

	next := pager.Find(`a[rel="next"]`)
	if n := next.Length(); n == 0 {
		if pager.Find(`a[rel="prev"]`).Length() == 0 {
			return nil, fmt.Errorf("no next or prev link found in pager")
		}
		return nil, nil
	} else if n > 1 {
		return nil, fmt.Errorf("multiple next links found (wtf)")
	}

	href := next.AttrOr("href", "")
	if href == "" {
		return nil, fmt.Errorf("href is empty")
	}
	return resolve(doc, href)
}

// scrapePlaceListings iterates over the place listings table, returning the URL
// of the next page, if any.
func scrapePlaceListings(doc *goquery.Document, s *goquery.Selection, fn func(u *url.URL, name, address string) error) error {
	view, err := findOne(s, `.view-place-listing-search`, "place listing view")
	if err != nil {
		return err
	}

	table, err := findOne(view, `table`, "place listing result table")
	if err != nil {
		return err
	}

	rows := table.Find(`tbody > tr`)
	if rows.Length() == 0 {
		return fmt.Errorf("no rows found")
	}

	for i, row := range rows.EachIter() {
		if x := func() error {
			rowTitle, err := findOne(row, `td[headers="view-title-table-column"]`, "title column")
			if err != nil {
				return err
			}

			rowURL, err := findOne(rowTitle, `a[href]`, "row link")
			if err != nil {
				return err
			}

			rowAddress, err := findOne(row, `td[headers="view-field-address-table-column"]`, "address column")
			if err != nil {
				return err
			}

			href := rowURL.AttrOr("href", "")
			if href == "" {
				return fmt.Errorf("href is empty")
			}

			u, err := resolve(doc, href)
			if err != nil {
				return err
			}

			title := normalizeText(rowTitle.Text(), false, false)
			address := normalizeText(rowAddress.Text(), true, false)

			if err := fn(u, title, address); err != nil {
				return fmt.Errorf("process %q: %w", title, err)
			}
			return nil
		}(); x != nil {
			return fmt.Errorf("row %d: %w", i+1, x)
		}
	}
	return nil
}

// scrapeCollapseSections iterates over collapse section widgets contained
// within s.
func scrapeCollapseSections(s *goquery.Selection, fn func(title string, content *goquery.Selection) error) error {
	buttons := s.Find(`[role="button"][data-toggle="collapse"][data-target]`)
	if buttons.Length() == 0 && s.Find(`div.collapse-region`).Length() != 0 {
		return fmt.Errorf("no collapse sections found, but collapse-region found")
	}
	for i, btn := range buttons.EachIter() {
		title := strings.TrimSpace(btn.Text())
		if x := func() error {
			tgt, _ := btn.Attr("data-target")

			content, err := findOne(s, tgt, "collapse section content")
			if err != nil {
				return err
			}

			if err := fn(title, content); err != nil {
				return fmt.Errorf("process %q: %w", title, err)
			}
			return nil
		}(); x != nil {
			return fmt.Errorf("section %d (%q): %w", i+1, title, x)
		}
	}
	return nil
}

// scrapeNodeField gets a node field, ensuring it is the expected type.
func scrapeNodeField(s *goquery.Selection, name, typ string, array, optional bool) (*goquery.Selection, error) {
	fields := s.Find(".field")
	if fields.Length() == 0 {
		return nil, fmt.Errorf("no fields found")
	}

	fields = fields.Filter(".field--name-field-" + name)
	if fields.Length() == 0 {
		if optional {
			return fields, nil
		}
		return nil, fmt.Errorf("field %q not found", name)
	}

	if fields.Length() > 1 {
		return nil, fmt.Errorf("multiple (%d) fields with name %q found, expected one", fields.Length(), name)
	}
	field := fields.First()

	if !field.HasClass("field--type-" + typ) {
		return nil, fmt.Errorf("field %q does not have type %q", name, typ)
	}

	var (
		items   *goquery.Selection
		isArray bool
	)
	switch {
	case field.HasClass("field__items"):
		items = field.Find(".field__item")
		isArray = true
	case field.HasClass("field__item"):
		items = field
	default:
		if tmp := field.Find(".field__items"); tmp.Length() != 0 {
			items = tmp.Find(".field__item")
			isArray = true
		} else {
			items = field.Find(".field__item")
		}
	}
	if !isArray && items.Length() > 1 {
		return nil, fmt.Errorf("field %q is not an array, but found multiple field__item elements (wtf)", name)
	}
	if items.Length() == 0 {
		return nil, fmt.Errorf("field %q does not contain field__item value (wtf)", name)
	}
	if array != isArray {
		if array {
			return nil, fmt.Errorf("field %q is not an array, expected one", name)
		} else {
			return nil, fmt.Errorf("field %q is an array, expected not", name)
		}
	}
	return items, nil
}

// scrapeScheduleGroup scrapes a schedule group collapse section, returning nil
// on failure, and returning a slice of warnings/errors from parsing the
// schedule.
func scrapeScheduleGroup(doc *goquery.Document, facilityName, label string, content *goquery.Selection) (msg *schema.ScheduleGroup, xerrs []string) {
	var group schema.ScheduleGroup_builder
	group.Label = label
	group.XTitle = extractScheduleGroupTitle(label)

	if scheduleChangeH := content.Find("h1,h2,h3,h4,h5,h6").FilterFunction(func(i int, s *goquery.Selection) bool {
		return strings.HasPrefix(strings.TrimSpace(strings.ToLower(s.Text())), "schedule change")
	}); scheduleChangeH.Length() == 1 {
		if sel := scheduleChangeH.Next(); sel.Is("ul") {
			if raw, err := sel.Html(); err == nil {
				group.ScheduleChangesHtml = "<ul>" + raw + "</ul>"
			} else {
				xerrs = append(xerrs, fmt.Sprintf("parse schedule changes for schedule group %q: %v", label, err))
			}
		} else {
			xerrs = append(xerrs, fmt.Sprintf("parse schedule changes for schedule group %q: header is not followed by a list", label))
		}
	} else if scheduleChangeH.Length() != 0 {
		xerrs = append(xerrs, fmt.Sprintf("parse schedule changes for schedule group %q: multiple selector matches found", label))
	}

	for _, btn := range content.Find(".btn").EachIter() {
		tmp := btn.Clone()
		tmp.Find(".fas").Remove()             // font-awesome icons
		tmp.Find(".visually-hidden").Remove() // accessibility text
		label := normalizeText(tmp.Text(), false, false)

		switch {
		case strings.Contains(strings.ToLower(label), "reserve a spot"):
		case strings.Contains(strings.ToLower(btn.AttrOr("href", "")), "reservation.frontdesksuite.ca"):
		default:
			continue
		}

		var burl string
		if href := btn.AttrOr("href", ""); href == "" {
			xerrs = append(xerrs, fmt.Sprintf("parse reservation button for schedule group %q: href is empty", group.Label))
		} else if u, err := resolve(doc, href); err != nil {
			xerrs = append(xerrs, fmt.Sprintf("parse reservation button for schedule group %q: failed to parse href: %v", group.Label, err))
		} else {
			burl = u.String()
		}

		var link schema.ReservationLink_builder
		link.Label = label
		link.Url = burl
		group.ReservationLinks = append(group.ReservationLinks, link.Build())
	}

	for _, el := range content.Find("p,table").EachIter() {
		if el.Is("table") {
			// got the first schedule table, don't continue
			// looking for reservations not required text (if
			// it's there, we can't really trust it since it's
			// between two schedules, so it's ambiguous)
			break
		}
		if req, ok := parseReservationRequirement(el.Text()); ok {
			if req {
				if len(group.ReservationLinks) == 0 {
					slog.Warn("unexpected top-level reservation required text without reservation links")
					xerrs = append(xerrs, "unexpected top-level reservation required text without reservation links")
				}
				continue
			}
			if group.XNoresv {
				slog.Warn("multiple top-level reservation not required text")
			}
			group.XNoresv = true
		}
	}

	for _, table := range content.Find("table").EachIter() {
		schedule, xerrs := scrapeSchedule(table, facilityName)
		if schedule != nil {
			group.Schedules = append(group.Schedules, schedule)
		}
		for _, xerr := range xerrs {
			xerrs = append(xerrs, fmt.Sprintf("group %q: %s", group.Label, xerr))
		}
	}
	return group.Build(), xerrs
}

// scrapeSchedule scrapes a schedule table, returning nil on failure, and
// returning a slice of warnings/errors from parsing the schedule.
func scrapeSchedule(table *goquery.Selection, facilityName string) (msg *schema.Schedule, xerrs []string) {
	var schedule schema.Schedule_builder
	schedule.Caption = normalizeText(table.Find("caption").First().Text(), false, false)

	// date range suffix
	name, date, ok := cutDateRange(schedule.Caption)
	if ok {
		schedule.XDate = date
		if r, ok := parseDateRange(date); ok {
			schedule.XFrom = ptrTo(int32(r.From))
			schedule.XTo = ptrTo(int32(r.To))
		} else {
			xerrs = append(xerrs, fmt.Sprintf("schedule %q: failed to parse date range %q", schedule.Caption, date))
		}
	}
	// " schedule" suffix
	name = strings.TrimSpace(strings.TrimSuffix(strings.ToLower(name), " schedule"))
	// facility name prefix
	if x, ok := strings.CutPrefix(name, strings.ToLower(facilityName)); ok {
		name = x
	} else if x, y, ok := strings.Cut(name, "-"); ok && strings.HasPrefix(strings.ToLower(facilityName), x) {
		name = strings.TrimSpace(y) // e.g., "Jack Purcell Community Centre" with "Jack Purcell - swim and aquafit - January 6 to April 6"
		// note: we shouldn't try to parse the date range
		// (Month DD[, YYYY] to [Month ]DD[, YYYY] OR
		// until|starting Month DD[, YYYY]) since it's
		// manually written and the year isn't automatically
		// added when the year changes, so it's hard to know
		// if we parsed it correctly
	}
	name = strings.TrimLeft(name, " -")
	schedule.XName = strings.TrimLeft(name, " -")

	// TODO: refactor
	for _, row := range table.Find("tr").EachIter() {
		cells := row.Find("th,td")
		if schedule.Days == nil {
			for i, cell := range cells.EachIter() {
				if i != 0 {
					schedule.Days = append(schedule.Days, strings.Join(strings.Fields(cell.Text()), " "))
				}
			}
			schedule.XDaydates = make([]int32, len(schedule.Days))
			for i, x := range schedule.Days {
				if v, ok := parseLooseDate(x); ok {
					schedule.XDaydates[i] = int32(v)
				}
			}
		} else {
			var activity schema.Schedule_Activity_builder
			if cells.Length() != len(schedule.Days)+1 {
				xerrs = append(xerrs, fmt.Sprintf("failed to parse schedule %q: row size mismatch", schedule.Caption))
				return nil, xerrs
			}
			for i, cell := range cells.EachIter() {
				if i == 0 {
					activity.Label = normalizeText(cell.Text(), false, false)
					activity.XName = cleanActivityName(cell.Text())
					if _, resv, ok := cutReservationRequirement(activity.Label); ok {
						activity.XResv = ptrTo(resv)
					}
				} else {
					hdr := schedule.Days[i-1]
					wkday := time.Weekday(-1)
					for wd := range 7 {
						if strings.Contains(strings.ToLower(hdr), strings.ToLower(time.Weekday(wd).String())[:3]) {
							if wkday == -1 {
								wkday = time.Weekday(wd)
							} else {
								slog.Warn("multiple weekday matches for header, ignoring", "schedule", schedule.Caption, "header", hdr)
								wkday = -1 // multiple matches
								break
							}
						}
					}
					if wkday == -1 {
						xerrs = append(xerrs, fmt.Sprintf("warning: failed to parse weekday from header %q", hdr))
					}
					times := []*schema.TimeRange{}
					for t := range strings.FieldsFuncSeq(cell.Text(), func(r rune) bool {
						return r == ','
					}) {
						if strings.Map(func(r rune) rune {
							if unicode.IsSpace(r) {
								return -1
							}
							return r
						}, normalizeText(t, false, true)) == "n/a" {
							continue
						}
						var trange schema.TimeRange_builder
						trange.Label = strings.TrimSpace(normalizeText(t, false, false))
						if wkday != -1 {
							trange.XWkday = ptrTo(schema.Weekday(wkday))
						}
						if r, ok := parseClockRange(t); ok {
							trange.XStart = ptrTo(int32(r.Start))
							trange.XEnd = ptrTo(int32(r.End))
							if r.Start > 24*60 || r.End > 24*60 {
								slog.Warn("note: time range goes into the next day", "raw", t, "parsed", r)
							}
						} else {
							slog.Warn("failed to parse time range", "range", t)
							xerrs = append(xerrs, fmt.Sprintf("warning: failed to parse time range %q", t))
						}
						times = append(times, trange.Build())
					}
					activity.Days = append(activity.Days, schema.Schedule_ActivityDay_builder{
						Times: times,
					}.Build())
				}
			}
			schedule.Activities = append(schedule.Activities, activity.Build())
		}
	}
	if len(schedule.Days) == 0 || len(schedule.Activities) == 0 {
		xerrs = append(xerrs, fmt.Sprintf("failed to parse schedule %q: invalid table layout", schedule.Caption))
		return nil, xerrs
	}
	return schedule.Build(), xerrs
}

// normalizeText performs various transformations on s:
//   - remove invisible characters
//   - collapse some kinds of consecutive whitespace (excluding newlines unless requested, but including nbsp)
//   - replace all kinds of dashes with "-"
//   - perform unicode NFKC normalization
//   - optionally lowercase the string
//   - remove leading and trailing whitespace
func normalizeText(s string, newlines, lower bool) string {
	// normalize the string
	s = norm.NFKC.String(s)

	// transform characters
	s = strings.Map(func(r rune) rune {

		// remove zero-width spaces
		switch r {
		case '\u200b', '\ufeff', '\u200d', '\u200c':
			return -1
		}

		// replace some whitespace for collapsing later
		switch r {
		case '\n':
			if newlines {
				return r
			}
			fallthrough
		case ' ', '\t', '\v', '\f', '\u00a0':
			return ' '
		}
		if unicode.Is(unicode.Zs, r) {
			return ' '
		}

		// replace smart punctuation
		switch r {
		case '“', '”', '‟':
			return '"'
		case '\u2018', '\u2019', '\u201b':
			return '\''
		case '\u2039':
			return '<'
		case '\u203a':
			return '>'
		}

		// normalize all kinds of dashes
		if unicode.Is(unicode.Pd, r) {
			return '-'
		}

		// remove invisible characters
		if !unicode.IsGraphic(r) {
			return -1
		}

		// lowercase (or not)
		if lower {
			return unicode.ToLower(r)
		}
		return r
	}, s)

	// collapse consecutive whitespace
	s = string(slices.CompactFunc([]rune(s), func(a, b rune) bool {
		return a == ' ' && a == b
	}))

	// remove leading/trailing whitespace
	return strings.TrimSpace(s)
}

// extractScheduleGroupTitle extracts the title of the schedule group from a
// section title.
func extractScheduleGroupTitle(s string) (title string) {
	title = normalizeText(s, false, true)
	title = strings.TrimPrefix(title, "drop-in schedule")
	title = strings.TrimPrefix(title, "s ")
	title = strings.Trim(title, "- ")
	title = cases.Title(language.English).String(title)
	return
}

// ageRangeRe matches things like "12+", "(18+)", and "(50 +)", also capturing
// the surrounding dashes/whitespace.
var ageRangeRe = regexp.MustCompile(`(^|[\s-]+)\(?(?:ages\s+)?([0-9]+)(?:\s*\+)\)?([\s(-]+|$)`) // capture: pre-sep age post-sep

// cutAgeMin removes the age minimum from activity, returning it as an int.
func cutAgeMin(activity string) (string, int, bool) {
	if ms := ageRangeRe.FindAllStringSubmatch(activity, -1); len(ms) == 1 {
		var (
			whole   = ms[0][0]
			preSep  = ms[0][1]
			ageStr  = ms[0][2]
			postSep = ms[0][3]
		)
		if age, err := strconv.ParseInt(ageStr, 0, 10); err == nil && age > 0 && age < 150 {
			sep := cmp.Or(preSep, postSep)
			if sep != "" && strings.TrimSpace(sep) == "" {
				if strings.TrimSpace(postSep) == "" {
					sep = " " // collapse if all whitespace
				} else {
					sep = postSep // pre is all whitespace, but post isn't
				}
			}
			return strings.TrimSpace(strings.ReplaceAll(activity, whole, sep)), int(age), true
		}
	}
	return activity, -1, false
}

// cutReservationRequirement removes the reservations (not) required text
// (prefixed by an asterisk) from activity.
func cutReservationRequirement(activity string) (string, bool, bool) {
	if i := strings.LastIndex(activity, "*"); i != -1 {
		if req, ok := parseReservationRequirement(activity[i:]); ok {
			return strings.TrimSpace(activity[:i]), req, true
		}
	}
	return activity, false, false
}

// parseReservationRequirement parses a single reservation requirement string.
func parseReservationRequirement(s string) (bool, bool) {
	switch strings.Trim(normalizeText(s, false, true), "*. ()") {
	case "reservations not required", "reservation not required", "reservation is not required", "reservations are not required":
		return false, true
	case "reservations required", "reservation required", "requires reservations", "requires reservation", "reservation is required", "reservations are required":
		return true, true
	}
	return false, false
}

// reducedCapacityRe matches "reduced" or "reduced capacity" at the beginning or
// end of a string, optionally with spaces/dashes joining it to the rest of the
// string.
var reducedCapacityRe = regexp.MustCompile(`(?i)(?:^reduced(?:\s* capacity)?[\s-]*|[\s-]*reduced(?:\s* capacity)?$)`)

// cutReducedCapacity removes the reduced capacity text from activity. The
// activity name should have already been normalized and lowercased.
func cutReducedCapacity(activity string) (string, bool) {
	x := reducedCapacityRe.ReplaceAllLiteralString(activity, "")
	return x, x != activity
}

// activityReplacer normalizes word tenses and punctuation in activity names.
// The string should have already been normalized and lowercased.
var activityReplacer = strings.NewReplacer(
	"swimming", "swim",
	"aqualite", "aqua lite",
	"skating", "skate",
	"pick up ", "pick-up ",
	"pickup ", "pick-up ",
	"sport ", "sports ",
	" - courts", " court",
	" - court", " court",
	"®", "",
)

// cleanActivityName cleans up activity names.
func cleanActivityName(activity string) string {
	activity = normalizeText(activity, false, true)
	activity, _, _ = cutReservationRequirement(activity)
	activity, age, hasAge := cutAgeMin(activity)
	activity, reduced := cutReducedCapacity(activity)
	activity = activityReplacer.Replace(activity)
	if hasAge {
		activity = strings.TrimRight(activity, "- ") + " " + strconv.Itoa(age) + "+"
	}
	if reduced {
		activity += " - reduced capacity"
	}
	activity = normalizeText(activity, false, false)
	activity = strings.Trim(activity, "*- ")
	return activity
}

// parseClockRange parses a time range for an activity.
func parseClockRange(s string) (r schema.ClockRange, ok bool) {
	strict := false

	s = strings.ReplaceAll(normalizeText(s, false, true), " ", "")

	// TODO: rewrite this all now that I've decided how the edge cases should behave

	parseSeparator := func(s string) (s1, s2 string, ok bool) {
		return stringsCutFirst(s, "-", "to")
	}

	parsePart := func(s string, mdef byte) (t schema.ClockTime, m byte, ok bool) {
		switch s {
		case "midnight":
			return schema.MakeClockTime(0, 0), 'a', true // midnight implies am
		case "noon":
			return schema.MakeClockTime(12, 0), 'p', true // noon implies pm
		}
		sh, sm, ok := strings.Cut(s, "h") // french time
		if !ok {
			if len(s) == 4 && strings.TrimFunc(s, func(r rune) bool { return r >= '0' && r <= '9' }) == "" {
				sh, sm, m = s[:2], s[2:], 0 // military time
			} else {
				if s, ok = strings.CutSuffix(s, "pm"); ok {
					if !strict {
						for {
							x, ok := strings.CutSuffix(strings.TrimRight(s, " "), "pm")
							if !ok {
								break
							}
							s = x // be lenient about duplicate pm suffixes
						}
					}
					m = 'p' // 12h pm
				} else if s, ok = strings.CutSuffix(s, "am"); ok {
					if !strict {
						for {
							x, ok := strings.CutSuffix(strings.TrimRight(s, " "), "am")
							if !ok {
								break
							}
							s = x // be lenient about duplicate am suffixes
						}
					}
					m = 'a' // 12h am
				} else {
					m = mdef // 24h or assumed am/pm
				}
				sh, sm, ok = strings.Cut(s, ":")
				if !ok {
					sm = "00" // no minute
				}
			}
		}
		if len(sh) > 2 || len(sm) > 2 {
			return 0, 0, false // invalid hour/minute length
		}
		hh, err := strconv.ParseInt(sh, 10, 0)
		if err != nil {
			return 0, 0, false // invalid hour
		}
		if m != 0 {
			if hh < 1 || hh > 12 {
				return 0, 0, false // invalid 12h hour
			}
			switch m {
			case 'p':
				if hh < 12 {
					hh += 12
				}
			case 'a':
				if hh == 12 {
					hh = 0
				}
			}
		} else {
			if hh < 0 || hh > 23 {
				return 0, 0, false // invalid 24h hour
			}
		}
		mm, err := strconv.ParseInt(sm, 10, 0)
		if err != nil {
			return 0, 0, false // invalid minute
		}
		if mm < 0 || mm > 59 {
			return 0, 0, false // invalid 24h minute
		}
		return schema.MakeClockTime(int(hh), int(mm)), m, true
	}

	if s == "" {
		return r, false // empty
	}
	s1, s2, ok := parseSeparator(s)
	if !ok {
		return r, false // single time
	}
	if !strict {
		for {
			s2a, s2b, ok := parseSeparator(s2)
			if !ok {
				break // no extraneous separators
			}
			if strings.TrimSpace(s2a) != "" || strings.TrimSpace(s2b) == "" {
				break // junk on the left side, or nothing on the right side
			}
			s2 = s2b // be lenient about extraneous separators with nothing in between (it's a frequent typo)
		}
	}
	if s1 == "" || s2 == "" {
		return r, false // open range
	}
	t1, m1, ok := parsePart(s1, 0)
	if !ok {
		return r, false // invalid lhs
	}
	t2, m2, ok := parsePart(s2, 0)
	if !ok {
		return r, false // invalid rhs
	}
	if m1 != 0 && m2 == 0 {
		return r, false // ambiguous lhs 12h and rhs 24h
	}
	if m1 == 0 && t1 >= 13*60 && m2 != 0 {
		return r, false // ambiguous lhs 24h and rhs 12h
	}
	if m1 == 0 && m2 == 'a' && t2 < 60 && t1 >= 12*60 && t1 < 13*60 {
		t1 -= 12 * 60 // RHS is 12:XX AM and LHS is 12:XX
	}
	if m1 == 0 && m2 != 0 {
		// only if lhs is before rhs AND the difference is greater than 12h
		if t1 < t2 && t2-t1 >= 12*60 {
			t1, m1, ok = parsePart(s1, m2) // reparse lhs with 12h rhs am/pm
			if !ok {
				return r, false // lhs hour is now invalid
			}
			_ = m1
		}
	}
	if t1 == t2 {
		return r, false // zero range
	}
	if t1 > t2 {
		t2 += 24 * 60 // next day
	}
	return schema.ClockRange{Start: t1, End: t2}, true
}

var cutDateRangeRe = sync.OnceValue(func() *regexp.Regexp {
	var b strings.Builder
	b.WriteString(`(?i)`)                 // case-insensitive
	b.WriteString(`^`)                    // anchor
	b.WriteString(`\s*`)                  // trim whitespace
	b.WriteString(`(.+?)`)                // prefix
	b.WriteString(`[ -]*[-][ -]*`)        // separator (spaces/dashes around at least one dash)
	b.WriteString(`((?:(?:[a-z]+|)\s*)?`) // date range modifier
	b.WriteString(`(?:`)                  // start of date range:
	b.WriteString(`(?:`)                  // ... month
	for i := range 12 {
		x := time.Month(1 + i).String()
		if i != 0 {
			b.WriteString(`|`)
		}
		b.WriteString(x[:3]) // first 3
		b.WriteString(`|`)
		b.WriteString(x) // or the whole thing
	}
	b.WriteString(`)(?:$|[ ,])`) // ... ... followed by a space or comma or end
	b.WriteString(`|(?:`)        // ... or weekday
	for i := range 7 {
		x := time.Weekday(i).String()
		if i != 0 {
			b.WriteString(`|`)
		}
		b.WriteString(x[:3]) // first 3
		b.WriteString(`|`)
		b.WriteString(x) // or the whole thing
	}
	b.WriteString(`)(?:$|[ ,])`) // ... ... followed by a space or comma or end
	b.WriteString(`).*)`)        // and the rest
	b.WriteString(`\s*`)         // trim whitespace
	b.WriteString(`$`)           // anchor
	return regexp.MustCompile(b.String())
})

// cutDateRange cuts s around the first match of spacs/dash characters followed
// by a month+space, day+space, or day+comma or day (3 letters) and a
// non-alphanumeric character. For best results, the string should have already
// been normalized.
//
// note: we do it this way so we can be sure we didn't leave part of a date
// behind with parseDateRange.
func cutDateRange(s string) (prefix, dates string, ok bool) {
	if m := cutDateRangeRe().FindStringSubmatch(s); m != nil {
		return m[1], m[2], true
	}
	return s, "", false
}

// parseDateRange parses a schedule date range. If successful, the range will
// always have at least the month and day set on one side.
func parseDateRange(s string) (r schema.DateRange, ok bool) {
	s = normalizeText(s, false, true)

	var starting, until bool
	if s, starting = strings.CutPrefix(s, "starting "); !starting {
		s, until = strings.CutPrefix(s, "until ")
	}

	var and, to bool
	leftStr, rightStr, to := strings.Cut(s, " to ")
	if !to {
		leftStr, rightStr, and = strings.Cut(s, " and ")
	}
	if (and || to) && (starting || until) {
		return r, false // can't both be a range and a one-sided date
	}

	parsePart := func(s string) (schema.Date, bool) {
		d, ok := parseLooseDate(s)
		if !ok {
			return d, false
		}
		if _, hasDay := d.Day(); !hasDay {
			return d, false
		}
		if _, hasMonth := d.Month(); !hasMonth {
			return d, false
		}
		return d, true
	}

	left, ok := parsePart(leftStr)
	if !ok {
		return r, false // failed to parse left side or single
	}

	switch {
	case to, and: // ... and/to ...
		var right schema.Date
		if and {
			if _, hasYear := left.Year(); hasYear {
				return r, false // cannot have year for an "and" range
			}
		}
		if day, err := strconv.ParseInt(rightStr, 10, 0); err == nil && day >= 1 && day <= 32 {
			year, hasYear := left.Year()
			if !hasYear {
				year = 0
			}
			month, hasMonth := left.Month()
			if !hasMonth {
				month = 0
			}
			if and {
				leftDay, hasLeftDay := left.Day()
				if !hasLeftDay {
					return r, false // must have left day for an "and" range
				}
				if leftDay+1 != int(day) {
					return r, false // right day must be 1 more than the left day for an "and" range
				}
			}
			right, ok = schema.MakeDate(year, month, int(day), -1), true
		} else if and {
			return r, false // must only have day number for an "and" range
		} else {
			right, ok = parsePart(rightStr)
		}
		if !ok {
			return r, false // failed to parse right side
		}
		r.From = left
		r.To = right

	case starting: // starting ...
		r.From = left

	case until: // until ...
		r.To = left

	default: // ...
		r.From = left
		r.To = left
	}
	return r, true
}

// parseLooseDate attempts to loosely parse an incomplete date string. The date
// string must contain only any of the month, day, year, and/or weekday. It
// returns false if there is any unparsed text or ambiguity.
func parseLooseDate(s string) (schema.Date, bool) {
	var (
		yyyy int
		mm   time.Month
		dd   int
		w    time.Weekday = -1
	)
	for seg := range strings.FieldsFuncSeq(normalizeText(s, false, true), func(r rune) bool {
		return r == '.' || r == ',' || r == '-' || unicode.IsSpace(r)
	}) {
		var (
			segMonth time.Month
			segWkday time.Weekday = -1
		)
		switch seg {
		case "sun", "sunday":
			segWkday = time.Sunday
		case "mon", "monday":
			segWkday = time.Monday
		case "tue", "tuesday":
			segWkday = time.Tuesday
		case "wed", "wednesday":
			segWkday = time.Wednesday
		case "thu", "thursday":
			segWkday = time.Thursday
		case "fri", "friday":
			segWkday = time.Friday
		case "sat", "saturday":
			segWkday = time.Saturday
		case "jan", "january":
			segMonth = time.January
		case "feb", "february":
			segMonth = time.February
		case "mar", "march":
			segMonth = time.March
		case "apr", "april":
			segMonth = time.April
		case "may":
			segMonth = time.May
		case "jun", "june":
			segMonth = time.June
		case "jul", "july":
			segMonth = time.July
		case "aug", "august":
			segMonth = time.August
		case "sep", "september":
			segMonth = time.September
		case "oct", "october":
			segMonth = time.October
		case "nov", "november":
			segMonth = time.November
		case "dec", "december":
			segMonth = time.December
		}
		if segMonth != 0 {
			if mm != 0 {
				return 0, false // duplicate month
			}
			mm = segMonth
			continue
		}
		if segWkday != -1 {
			if w != -1 {
				return 0, false // duplicate weekday
			}
			w = segWkday
			continue
		}
		if len(seg) == 4 && seg[0] == '2' {
			if n, err := strconv.ParseInt(seg, 10, 0); err == nil {
				if n < 2000 || n >= 3000 {
					return 0, false // year out of range
				}
				if yyyy != 0 {
					return 0, false // duplicate year
				}
				yyyy = int(n)
				continue
			}
		}
		if len(seg) == 2 || len(seg) == 1 {
			if n, err := strconv.ParseInt(seg, 10, 0); err == nil {
				if n < 1 || n > 31 {
					return 0, false // day out of range
				}
				if dd != 0 {
					return 0, false // duplicate day
				}
				dd = int(n)
				continue
			}
		}
		return 0, false // unparsed segment
	}
	d := schema.MakeDate(yyyy, mm, dd, w)
	return d, d.IsValid() // checks that it's nonzero, that the weekday/day is valid for the month/year if specified
}

// stringsCutFirst is like [strings.Cut], but selects the earliest of multiple
// possible separators.
func stringsCutFirst(s string, sep ...string) (before, after string, ok bool) {
	sn, si := 0, -1
	for _, sep := range sep {
		if i := strings.Index(s, sep); i >= 0 {
			if si < 0 || i < si {
				sn, si = len(sep), i
			}
		}
	}
	if si >= 0 {
		return s[:si], s[si+sn:], true
	}
	return s, "", false
}

// overrideGeocode contains manual overrides for geocoding certain addresses.
func overrideGeocode(address string) (lat, lng float64, ok bool) {
	switch {
	case strings.Contains(address, "8720 Russell R"):
		return 45.383679, -75.337301, true
	case strings.Contains(address, "262 Len Purcell D"):
		return 45.499120, -76.093510, true
	case strings.Contains(address, "61 Corkstown R"):
		return 45.346026, -75.827210, true
	case strings.Contains(address, "200 Glen Park D"):
		return 45.430386, -75.563095, true
	case strings.Contains(address, "5660 Osgoode Main S"):
		return 45.146788, -75.601946, true
	case strings.Contains(address, "3832 Carp R"):
		return 45.349095, -76.039116, true
	case strings.Contains(address, "100 Brewer W"):
		return 45.389584, -75.691586, true
	case strings.Contains(address, "2100 Cabot S"):
		return 45.389751, -75.672654, true
	case strings.Contains(address, "930 Somerset S"):
		return 45.407935, -75.715016, true
	case strings.Contains(address, "250 Somerset S"):
		return 45.422913, -75.677798, true
	case strings.Contains(address, "4355 Halmont D"):
		return 45.428983, -75.619521, true
	case strings.Contains(address, "525 Côté S"),
		strings.Contains(address, "525 Cote S"):
		return 45.436256, -75.647392, true
	case strings.Contains(address, "43 Ste-Cécile S"),
		strings.Contains(address, "43 Ste-Cecile S"):
		return 45.442317, -75.669267, true
	case strings.Contains(address, "100 Thornwood R"):
		return 45.450844, -75.657186, true
	case strings.Contains(address, "679 Deancourt C"):
		return 45.481213, -75.487425, true
	case strings.Contains(address, "941 Clyde A"):
		return 45.374731, -75.746711, true
	}
	return 0, 0, false
}

func ptrTo[T any](x T) *T {
	return &x
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

var _ http.RoundTripper = roundTripperFunc(nil)

func headerRoundTripper(next http.RoundTripper, domain, name, value string) http.RoundTripper {
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if matchDomain(domain, r.URL) {
			r2 := *r
			r2.Header = r.Header.Clone()
			if value == "" {
				r2.Header.Del(name)
			} else {
				r2.Header.Set(name, value)
			}
			r = &r2
		}
		return cmp.Or(next, http.DefaultTransport).RoundTrip(r)
	})
}

func rateLimitRoundTripper(next http.RoundTripper, domain string, limiter *rate.Limiter) http.RoundTripper {
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if matchDomain(domain, r.URL) {
			if err := limiter.Wait(r.Context()); err != nil {
				return nil, err
			}
		}
		return cmp.Or(next, http.DefaultTransport).RoundTrip(r)
	})
}

func matchDomain(domain string, u *url.URL) bool {
	if domain == "" {
		return true // match all
	}
	h := strings.Trim(strings.ToLower(u.Hostname()), ".")
	d := strings.ToLower(domain)
	if h == d {
		return true // exact match
	}
	if d[0] == '.' {
		return h == d[1:] || strings.HasSuffix(h, d)
	}
	return false
}
