test_that("datetime parsing works", {
  test_vroom(
"date,time,datetime
2018-01-01,10:01:01 AM,2018-01-01 10:01:01
2019-01-01,05:04:03 AM,2019-01-01 05:04:03
",
    delim = ",",
    equals = tibble::tibble(
      date = c(as.Date("2018-01-01"), as.Date("2019-01-01")),
      time = c(hms::hms(1, 1, 10), hms::hms(3, 4, 5)),
      datetime = vctrs::vec_c(as.POSIXct("2018-01-01 10:01:01", tz = "UTC"), as.POSIXct("2019-01-01 05:04:03", tz = "UTC"))
    )
  )
})

# Parsing ----------------------------------------------------------------------

r_parse <- function(x, fmt) as.POSIXct(strptime(x, fmt, tz = "UTC"))

test_that("%d, %m and %y", {
  target <- utctime(2010, 2, 3, 0, 0, 0, 0)

  test_parse_datetime("10-02-03", "%y-%m-%d", expected = target)
  test_parse_datetime("10-03-02", "%y-%d-%m", expected = target)
  test_parse_datetime("03/02/10", "%d/%m/%y", expected = target)
  test_parse_datetime("02/03/10", "%m/%d/%y", expected = target)
})

test_that("Compound formats work", {
  target <- utctime(2010, 2, 3, 0, 0, 0, 0)

  test_parse_datetime("02/03/10", "%D", expected = target)
  test_parse_datetime("2010-02-03", "%F", expected = target)
  test_parse_datetime("10/02/03", "%x", expected = target)
})

test_that("%y matches R behaviour", {
  test_parse_datetime("01-01-69", "%d-%m-%y", expected = r_parse("01-01-69", "%d-%m-%y"))
  test_parse_datetime("01-01-68", "%d-%m-%y", expected = r_parse("01-01-68", "%d-%m-%y"))
})

test_that("%e allows leading space", {
  test_parse_datetime("201010 1", "%Y%m%e", expected = utctime(2010, 10, 1, 0, 0, 0, 0))
})

test_that("%OS captures partial seconds", {
  test_parse_datetime("2001-01-01 00:00:01.125", "%Y-%m-%d %H:%M:%OS", expected = utctime(2001, 1, 1, 0, 0, 1, .125))
  test_parse_datetime("2001-01-01 00:00:01.133", "%Y-%m-%d %H:%M:%OS", expected = utctime(2001, 1, 1, 0, 0, 1, .133))
})

test_that("%Y requires 4 digits", {
  test_parse_date("03-01-01", "%Y-%m-%d", expected = as.Date(NA))
  test_parse_date("003-01-01", "%Y-%m-%d", expected = as.Date(NA))
  test_parse_date("0003-01-01", "%Y-%m-%d", expected = as.Date("0003-01-01"))
  test_parse_date("00003-01-01", "%Y-%m-%d", expected = as.Date(NA))
})

test_that("invalid dates return NA", {
  test_parse_datetime("2010-02-30", "%Y-%m-%d", expected = .POSIXct(NA_real_, tz = "UTC"))
})

test_that("failed parsing returns NA", {
  test_parse_datetime(c("2010-02-ab", "2010-02", "2010/02/01"), "%Y-%m-%d",
    expected = .POSIXct(rep(NA_real_, 3), tz = "UTC"))
})

test_that("invalid specs returns NA", {
  test_parse_datetime("2010-02-20", "%Y-%m-%m", expected = .POSIXct(NA_real_, tz = "UTC"))
})

test_that("ISO8601 partial dates are not parsed", {
  test_parse_datetime("20", "", expected = .POSIXct(NA_real_, tz = "UTC"))
  test_parse_datetime("2001", "", expected = .POSIXct(NA_real_, tz = "UTC"))
  test_parse_datetime("2001-01", "", expected = .POSIXct(NA_real_, tz = "UTC"))
})

test_that("Year only gets parsed", {
  test_parse_datetime("2010", "%Y", expected = ISOdate(2010, 1, 1, 0, tz = "UTC"))
  test_parse_datetime("2010-06", "%Y-%m", expected = ISOdate(2010, 6, 1, 0, tz = "UTC"))
})

test_that("%p detects AM/PM", {
  test_parse_datetime(c("2015-01-01 01:00 AM", "2015-01-01 01:00 am"), "%F %I:%M %p",
    expected = .POSIXct(c(1420074000, 1420074000), "UTC")
  )

  test_parse_datetime(c("2015-01-01 01:00 PM", "2015-01-01 01:00 pm"), "%F %I:%M %p",
    expected = .POSIXct(c(1420117200, 1420117200), "UTC")
  )

  test_parse_datetime("12/31/1991 12:01 AM", "%m/%d/%Y %I:%M %p",
    expected = .POSIXct(694137660, "UTC"))

  test_parse_datetime("12/31/1991 12:01 PM", "%m/%d/%Y %I:%M %p",
    expected = .POSIXct(694180860, "UTC"))

  test_parse_datetime("12/31/1991 01:01 AM", "%m/%d/%Y %I:%M %p",
    expected = .POSIXct(694141260, "UTC"))

  test_parse_datetime(c("12/31/1991 00:01 PM", "12/31/1991 13:01 PM"),
      "%m/%d/%Y %I:%M %p", expected = .POSIXct(rep(NA_real_, 2), tz = "UTC"))
})

test_that("%b and %B are case insensitive", {
  ref <- as.Date("2001-01-01")

  test_parse_date("2001 JAN 01", "%Y %b %d", expected = ref)
  test_parse_date("2001 JANUARY 01", "%Y %B %d", expected = ref)
})

test_that("%. requires a value", {
  ref <- as.Date("2001-01-01")

  test_parse_date("2001?01?01", "%Y%.%m%.%d", expected = ref)
  test_parse_date("20010101", "%Y%.%m%.%d", expected = as.Date(NA))
})

test_that("%Z detects named time zones", {
  ref <- .POSIXct(1285912800, "America/Chicago")
  ct <- locale(tz = "America/Chicago")

  test_parse_datetime("2010-10-01 01:00", "", expected = ref, locale = ct)
  test_parse_datetime("2010-10-01 01:00 America/Chicago", "%Y-%m-%d %H:%M %Z", locale = ct, expected = ref)
})

test_that("%Z detects named time zones", {
  ref <- .POSIXct(1285912800, "UTC")

  test_parse_datetime("1285912800", "%s", expected = ref)
})

test_that("parse_date returns a double like as.Date()", {
  ref <- as.Date("2001-01-01")

  res <- test_parse_date("2001-01-01", "", expected = ref)
  expect_type(res[[1]], "double")
})

test_that("parses NA/empty correctly", {
  expect_equal(
    vroom(I("x\n\n"), delim = ",", col_types = list(x = "T"), skip_empty_rows = FALSE),
    tibble::tibble(x = .POSIXct(NA_real_, tz = "UTC"))
  )

  expect_equal(
    vroom(I("x\n\n"), delim = ",", col_types = list(x = "D"), skip_empty_rows = FALSE),
    tibble::tibble(x = as.Date(NA))
  )

  test_parse_datetime("TeSt", "", na = "TeSt", expected = .POSIXct(NA_real_, tz = "UTC"))
  test_parse_date("TeSt", "", na = "TeSt", expected = as.Date(NA))
})

## Locales -----------------------------------------------------------------

test_that("locale affects months", {
  jan1 <- as.Date("2010-01-01")

  fr <- locale("fr")
  test_parse_date("1 janv. 2010", "%d %b %Y", locale = fr, expected = jan1)
  test_parse_date("1 janvier 2010", "%d %B %Y", locale = fr, expected = jan1)
})

test_that("locale affects day of week", {
  a <- as.POSIXct("2010-01-01", tz = "UTC")
  b <- .POSIXct(unclass(as.Date("2010-01-01")) * 86400, tz = "UTC")
  fr <- locale("fr")

  test_parse_datetime("Ven. 1 janv. 2010", "%a %d %b %Y", locale=fr, expected = a)
  test_parse_datetime("Ven. 1 janv. 2010", "%a %d %b %Y", locale=fr, expected = b)
})

test_that("locale affects am/pm", {
  skip_on_os("windows")
  skip_on_os("solaris")
  skip_if_not(l10n_info()$`UTF-8`)

  expected <- hms::hms(hours = 13, minutes = 30)
  test_parse_time("01:30 PM", "%H:%M %p", expected = expected)
  test_parse_time("\UC624\UD6C4 01\UC2DC 30\UBD84", "%p %H\UC2DC %M\UBD84", expected = expected, locale = locale("ko"))
})

test_that("locale affects both guessing and parsing", {
  out <- vroom(I("01/02/2013\n"), delim = ",", col_names = FALSE, col_types = list(), locale = locale(date_format = "%m/%d/%Y"))
  expect_equal(out[[1]][[1]], as.Date("2013-01-02"))
})

## Time zones ------------------------------------------------------------------

test_that("same times with different offsets parsed as same time", {
  # From http://en.wikipedia.org/wiki/ISO_8601#Time_offsets_from_UTC
  same_time <- paste("2010-02-03", c("18:30Z", "22:30+04", "1130-0700", "15:00-03:30"))
  test_parse_datetime(same_time, format = "", expected = rep(utctime(2010, 2, 3, 18, 30, 0, 0), 4))
})

test_that("offsets can cross date boundaries", {
  expected <- as.POSIXct("2015-02-01 01:00:00Z", tz = "UTC")
  test_parse_datetime("2015-01-31T2000-0500", expected = expected)
  test_parse_datetime("2015-02-01T0100Z", expected = expected)
})

test_that("unambiguous times with and without daylight savings", {
  skip("Not working on CI")
  skip_on_cran() # need to figure out why this fails

  melb <- locale(tz = "Australia/Melbourne")

  # Melbourne had daylight savings in 2015 that ended the morning of 2015-04-05
  expected_melb <- .POSIXct(c(1428109200, 1428285600), "Australia/Melbourne")
  test_parse_datetime(c("2015-04-04 12:00:00", "2015-04-06 12:00:00"), locale = melb, expected = expected_melb)

  # Japan didn't have daylight savings in 2015
  expected_ja <- .POSIXct(c(1428116400, 1428289200), "Japan")
  ja <- locale(tz = "Japan")
  test_parse_datetime(c("2015-04-04 12:00:00", "2015-04-06 12:00:00"), locale = ja, expected = expected_ja)
})

test_that("ambiguous times always choose the earliest time", {
  ny <- locale(tz = "America/New_York")

  format <- "%Y-%m-%d %H:%M:%S%z"
  expected <- as.POSIXct("1970-10-25 01:30:00-0400", tz = "America/New_York", format = format)

  test_parse_datetime("1970-10-25 01:30:00", locale = ny, expected = expected)
})

test_that("nonexistent times return NA", {
  ny <- locale(tz = "America/New_York")
  expected <- .POSIXct(NA_real_, tz = "America/New_York")

  test_parse_datetime("1970-04-26 02:30:00", locale = ny, expected = expected)
})

test_that("can use `tz = ''` for system time zone", {
  withr::local_timezone("Europe/London")

  system <- locale(tz = "")
  expected <- as.POSIXct("1970-01-01 00:00:00", tz = "Europe/London")

  test_parse_datetime("1970-01-01 00:00:00", locale = system, expected = expected)
})

test_that("can catch faulty system time zones", {
  withr::local_timezone("foo")
  expect_error(locale(tz = ""), "Unknown TZ foo")
})

## Guessing ---------------------------------------------------------------------

test_that("DDDD-DD not parsed as date (i.e. doesn't trigger partial date match)", {
  expect_type(vroom(I("1989-90\n1990-91\n"), delim = "\n", col_types = list())[[1]], "character")
})

test_that("leading zeros don't get parsed as date without explicit separator", {
  expect_type(vroom(I("00010203\n"), col_names = FALSE, delim = "\n", col_types = list())[[1]], "double")
  expect_s3_class(vroom(I("0001-02-03\n"), col_names = FALSE, delim = "\n", col_types = list())[[1]], "Date")
})

test_that("must have either two - or none", {
  expect_s3_class(vroom(I("2000-10-10\n"), col_names = FALSE, delim = "\n", col_types = list())[[1]], "Date")
  expect_type(vroom(I("2000-1010\n"), col_names = FALSE, delim = "\n", col_types = list())[[1]], "character")
  expect_type(vroom(I("200010-10\n"), col_names = FALSE, delim = "\n", col_types = list())[[1]], "character")
  expect_type(vroom(I("20001010\n"), col_names = FALSE, delim = "\n", col_types = list())[[1]], "double")
})

test_that("times are guessed even without AM / PM", {
  expect_s3_class(guess_type("01:02:03"), "collector_time")
  expect_s3_class(guess_type("01:02:03 AM"), "collector_time")
  expect_s3_class(guess_type("01:02:03 PM"), "collector_time")
})

test_that("subsetting works with both double and integer indexes", {
  x <- vroom(I("X1\n2020-01-01 01:01:01"), delim = ",", col_type = "T")
  dt <- as.POSIXct("2020-01-01 01:01:01", tz = "UTC")
  na_dt <- .POSIXct(NA_real_, tz = "UTC")
  expect_equal(x$X1[1L], dt)
  expect_equal(x$X1[1], dt)
  expect_equal(x$X1[NA_integer_], na_dt)
  expect_equal(x$X1[NA_real_], na_dt)
})

test_that("malformed date / datetime formats cause R errors", {
  expect_error(
    vroom(I("x\n6/28/2016"), delim = ",", col_types = list(x = col_date("%m/%/%Y")), altrep = FALSE),
    "Unsupported format"
  )

  expect_error(
    vroom(I("x\n6/28/2016"), delim = ",", col_types = list(x = col_datetime("%m/%/%Y")), altrep = FALSE),
    "Unsupported format"
  )
})

test_that("single digit dates and hours are parsed correctly (https://github.com/tidyverse/readr/issues/1276)", {
  test_vroom(I("4/9/2021 2:18:25 PM"), col_types = list(col_datetime("%m/%d/%Y %H:%M:%S %p")), col_names = "X", delim = ",",
    equals = tibble::tibble(X = as.POSIXct("2021-04-09 14:18:25", tz = "UTC"))
  )
})

test_that("durations", {
  parse_time <- function(x, format, ...) {
    vroom(I(x), delim = ",", col_names = FALSE, col_types = list(col_time(format = format)), altrep = FALSE)[[1]]
  }
  expect_warning(parse_time("25:00:00", format = "%H:%M:%S"))
  expect_equal(parse_time("25:00:00", format = "%h:%M:%S"), hms::hms(hours = 25))
  expect_equal(parse_time("1000000000:00:00", format = "%h:%M:%S"), hms::hms(hours = 1e9))
  expect_equal(parse_time("-1:23:45", format = "%h:%M:%S"), hms::as_hms(-hms::hms(45, 23, 1)))
  expect_equal(parse_time("-1:23:45.67", format = "%h:%M:%OS"), hms::as_hms(-hms::hms(45.67, 23, 1)))
})
