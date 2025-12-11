# locale() errors when decimal_mark equals grouping_mark

    Code
      locale(decimal_mark = ".", grouping_mark = ".")
    Condition
      Error in `locale()`:
      ! `decimal_mark` and `grouping_mark` must be different.
      i Both were specified as ".".

# locale() errors for unrecognized language code

    Code
      locale(date_names = "fake")
    Condition
      Error in `locale()`:
      ! Unrecognized language code: "fake"

# locale() warns for unrecognized encoding

    Code
      locale(encoding = "FAKE-ENCODING-9999")
    Condition
      Warning:
      `encoding` not found in `iconvlist()`: "FAKE-ENCODING-9999".
    Output
      <locale>
      Numbers:  123,456.78
      Formats:  %AD / %AT
      Timezone: UTC
      Encoding: FAKE-ENCODING-9999
      <date_names>
      Days:   Sunday (Sun), Monday (Mon), Tuesday (Tue), Wednesday (Wed), Thursday
              (Thu), Friday (Fri), Saturday (Sat)
      Months: January (Jan), February (Feb), March (Mar), April (Apr), May (May),
              June (Jun), July (Jul), August (Aug), September (Sep), October
              (Oct), November (Nov), December (Dec)
      AM/PM:  AM/PM

# locale() validates timezone

    Code
      locale(tz = "Invalid/Timezone")
    Condition
      Error in `locale()`:
      ! Unknown timezone: "Invalid/Timezone".

# locale() can consult and validate system time zone

    Code
      locale(tz = "")
    Condition
      Error in `locale()`:
      ! Unknown system timezone: "foo".

