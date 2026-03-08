# Ottawa Rec Schedules

City of Ottawa drop-in recreation schedule data scraper.

I will be making a website for this soon.

### Usage

The following links contain the most recent data, updated daily. See [data.ottrec.ca](https://data.ottrec.ca/) for more information.

| Format | Data | Schema | Notes |
| --- | --- | --- | --- |
| JSON (simplified) | <a href="https://data.ottrec.ca/export/latest.json" download="ottrec_simplified_latest.json">data.ottrec.ca/export/latest.json</a> | <a href="https://data.ottrec.ca/export/schema.json" download="ottrec_simplified.schema.json">schema.json</a> | Recommended for most uses, easiest to use. |
| CSV (simplified) | <a href="https://data.ottrec.ca/export/latest.csv.zip" download="ottrec_simplified_latest.csv.zip">data.ottrec.ca/export/latest.csv.zip</a> | <a href="https://data.ottrec.ca/export/schema.csv" download="ottrec_simplified.schema.csv">schema.csv</a> | Recommended for tools which require CSV.
| Protobuf (raw) | <a href="https://data.ottrec.ca/v1/latest/pb" download="ottrec_raw_latest.pb">data.ottrec.ca/v1/latest/pb</a> | <a href="https://data.ottrec.ca/v1/latest/proto" download="ottrec_raw_latest.proto">schema.proto</a> | Best for advanced use cases, least lossy.
| TextPB (raw) | <a href="https://data.ottrec.ca/v1/latest/textpb" download="ottrec_raw_latest.textpb">data.ottrec.ca/v1/latest/textpb</a> | <a href="https://data.ottrec.ca/v1/latest/proto" download="ottrec_raw_latest.proto">schema.proto</a> | Best for manual inspection when testing. |
| JSON (raw) | <a href="https://data.ottrec.ca/v1/latest/json" download="ottrec_raw_latest.json">data.ottrec.ca/v1/latest/json</a> | <a href="https://data.ottrec.ca/v1/latest/proto" download="ottrec_raw_latest.proto">schema.proto</a> | Not recommended. |

### About

#### Simplified data

##### Features

- Has facility longitude/latitude.
- Has parsed start/end date/time (if it was able to be parsed unambiguously).
- Has normalized activity names for easy filtering.
- Marks if reservation is likely required for an activity, and has a list of links.
- Error messages are included.
- Has fields with raw text from the website for validation or as a fallback if parsing fails.

##### Limitations

- Special schedules may overlap with a subset of the dates of the regular schedule, so the start/end dates are only good for determining that a scheduled activity does *not* apply to a specific date.
- Exceptions and notifications are included as raw HTML since they're freeform.

#### Raw data

##### Features / Limitations

- Only basic facility information and schedule information are scraped. This helps keep the scraper reliable and ensures the schema can be kept stable long-term.
- Facility addresses are geocoded using geocodio (which has better results than pelias/geocode.earth and nominatim).
- Schedule changes and facility notifications are scraped on a best-effort basis without additional parsing since these fields are inherently free-form. This helps keep the scraper reliable and reduces the likelihood of accidentally missing important information.
- Scraped fields have minimal processing. This helps keep the scraper reliable and reduces the likelihood of accidentally missing important information.
- Optional fields are available which contain best-effort parsing and normalization of scraped fields (to assist with usage), including:
  - Normalized schedule group name.
  - Normalized schedule name (facility and date range stripped).
  - Raw schedule date range (if stripped from the normalized schedule name).
  - Parsed schedule date range.
  - Normalized schedule activity name.
  - Activity time range and weekday as an integer.
  - Explicit reservation requirement in activity names as a boolean (typically, this is used as an exception to the default based on whether the schedule group has reservation links).
- Overlapping schedules (e.g., holiday schedules) are not merged. These schedules are not consistently formatted as they are manually named and created, so although I attempt to parse time ranges, I don't use them to merge schedules. This helps keep the scraper reliable and reduces the likelihood of accidentally missing important information.
- Any potential parsing problems are included in an array of error messages for each facility.
- A protobuf schema is used for maintainability, but it may be changed in backwards-incompatible ways if needed.

##### Changes

- **2026-03-08:** Fixed mixed up longitude/latitude affecting data from bc9be9b7098f8daaba3121daa564fcbeb4b85784 (2025-11-18 - 2026-03-09).
- **2025-10-07:** Initial stable release.
