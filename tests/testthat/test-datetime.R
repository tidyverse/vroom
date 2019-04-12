context("test-datetime")

test_that("datetime parsing works", {
  test_vroom(
"date,time,datetime
2018-01-01,10:01:01 AM,2018-01-01 10:01:01
2019-01-01,05:04:03 AM,2019-01-01 05:04:03
",
    equals = tibble::tibble(
      date = c(as.Date("2018-01-01"), as.Date("2019-01-01")),
      time = c(hms::hms(1, 1, 10), hms::hms(3, 4, 5)),
      datetime = c(as.POSIXct("2018-01-01 10:01:01", tz = "UTC"), as.POSIXct("2019-01-01 05:04:03", tz = "UTC"))
    )
  )
})

#context("Parsing, datetime")

#test_that("utctime is equivalent to R conversion", {
  #year <- seq(0, 4000)
  #mon <- rep(3, length(year))
  #day <- rep(1, length(year))
  #zero <- rep(0, length(year))

  #expect_equal(
    #ISOdatetime(year, mon, day, zero, zero, zero, tz = "UTC"),
    #utctime(year, mon, day, zero, zero, zero, zero)
  #)
#})

# Parsing ----------------------------------------------------------------------

r_parse <- function(x, fmt) as.POSIXct(strptime(x, fmt, tz = "UTC"))

test_that("%d, %m and %y", {
  target <- readr:::utctime(2010, 2, 3, 0, 0, 0, 0)

  test_parse_datetime("10-02-03", "%y-%m-%d", target)
  test_parse_datetime("10-03-02", "%y-%d-%m", target)
  test_parse_datetime("03/02/10", "%d/%m/%y", target)
  test_parse_datetime("02/03/10", "%m/%d/%y", target)
})

test_that("Compound formats work", {
  target <- readr:::utctime(2010, 2, 3, 0, 0, 0, 0)

  test_parse_datetime("02/03/10", "%D", target)
  test_parse_datetime("2010-02-03", "%F", target)
  test_parse_datetime("10/02/03", "%x", target)
})

test_that("%y matches R behaviour", {
  test_parse_datetime("01-01-69", "%d-%m-%y", r_parse("01-01-69", "%d-%m-%y"))
  test_parse_datetime("01-01-68", "%d-%m-%y", r_parse("01-01-68", "%d-%m-%y"))
})

test_that("%e allows leading space", {
  test_parse_datetime("201010 1", "%Y%m%e", readr:::utctime(2010, 10, 1, 0, 0, 0, 0))
})

test_that("%OS captures partial seconds", {
  test_parse_datetime("2001-01-01 00:00:01.125", "%Y-%m-%d %H:%M:%OS", readr:::utctime(2001, 1, 1, 0, 0, 1, .125))
  test_parse_datetime("2001-01-01 00:00:01.133", "%Y-%m-%d %H:%M:%OS", readr:::utctime(2001, 1, 1, 0, 0, 1, .133))
})

test_that("%Y requires 4 digits", {
  test_parse_date("03-01-01", "%Y-%m-%d", as.Date(NA))
  test_parse_date("003-01-01", "%Y-%m-%d", as.Date(NA))
  test_parse_date("0003-01-01", "%Y-%m-%d", as.Date("0003-01-01"))
  test_parse_date("00003-01-01", "%Y-%m-%d", as.Date(NA))
})

test_that("invalid dates return NA", {
  test_parse_datetime("2010-02-30", "%Y-%m-%d", NA)
})

test_that("failed parsing returns NA", {
  test_parse_datetime(c("2010-02-ab", "2010-02", "2010/02/01"), "%Y-%m-%d",
    rep(NA, 3)
  )
})

test_that("invalid specs returns NA", {
  test_parse_datetime("2010-02-20", "%Y-%m-%m", NA)
})

test_that("ISO8601 partial dates are not parsed", {
  test_parse_datetime("20", "", NA)
  test_parse_datetime("2001", "", NA)
  test_parse_datetime("2001-01", "", NA)
})

test_that("Year only gets parsed", {
  test_parse_datetime("2010", "%Y", ISOdate(2010, 1, 1, 0, tz = "UTC"))
  test_parse_datetime("2010-06", "%Y-%m",ISOdate(2010, 6, 1, 0, tz = "UTC"))
})

test_that("%p detects AM/PM", {
  test_parse_datetime(c("2015-01-01 01:00 AM", "2015-01-01 01:00 am"), "%F %I:%M %p",
    .POSIXct(c(1420074000, 1420074000), "UTC")
  )

  test_parse_datetime(c("2015-01-01 01:00 PM", "2015-01-01 01:00 pm"), "%F %I:%M %p",
    .POSIXct(c(1420117200, 1420117200), "UTC")
  )

  test_parse_datetime("12/31/1991 12:01 AM", "%m/%d/%Y %I:%M %p",
    .POSIXct(694137660, "UTC"))

  test_parse_datetime("12/31/1991 12:01 PM", "%m/%d/%Y %I:%M %p",
    .POSIXct(694180860, "UTC"))

  #TODO: why is this returning NA?
  #test_parse_datetime("12/31/1991 1:01 AM", "%m/%d/%Y %I:%M %p",
    #.POSIXct(694141260, "UTC"))

  test_parse_datetime(c("12/31/1991 00:01 PM", "12/31/1991 13:01 PM"),
      "%m/%d/%Y %I:%M %p", rep(NA, 2))
})

test_that("%b and %B are case insensitve", {
  skip("not working")
  ref <- as.Date("2001-01-01")

  test_parse_date("2001 JAN 01", "%Y %b %d", ref)
  test_parse_date("2001 JANUARY 01", "%Y %B %d", ref)
})

test_that("%. requires a value", {
  ref <- as.Date("2001-01-01")

  test_parse_date("2001?01?01", "%Y%.%m%.%d", ref)
  test_parse_date("20010101", "%Y%.%m%.%d", as.Date(NA))
})

test_that("%Z detects named time zones", {
  ref <- .POSIXct(1285912800, "America/Chicago")
  ct <- locale(tz = "America/Chicago")

  test_parse_datetime("2010-10-01 01:00", "", ref, locale = ct)
  test_parse_datetime("2010-10-01 01:00 America/Chicago", "%Y-%m-%d %H:%M %Z", locale = ct, ref)
})

test_that("parse_date returns a double like as.Date()", {
  ref <- as.Date("2001-01-01")

  res <- test_parse_date("2001-01-01", "", ref)
  expect_type(res[[1]], "double")
})

test_that("parses NA/empty correctly", {
  #test_parse_datetime("", "", .POSIXct(NA_real_))
  #test_parse_date("", "", as.Date(NA))

  test_parse_datetime("NA", "", .POSIXct(NA_real_))
  test_parse_date("NA", "", as.Date(NA))

  test_parse_datetime("TeSt", "", na = "TeSt", .POSIXct(NA_real_))
  test_parse_date("TeSt", "", na = "TeSt", as.Date(NA))
})

## Locales -----------------------------------------------------------------

#test_that("locale affects months", {
  #jan1 <- as.Date("2010-01-01")

  #fr <- locale("fr")
  #expect_equal(parse_date("1 janv. 2010", "%d %b %Y", locale = fr), jan1)
  #expect_equal(parse_date("1 janvier 2010", "%d %B %Y", locale = fr), jan1)
#})

#test_that("locale affects day of week", {
  #a <- parse_datetime("2010-01-01")
  #b <- parse_date("2010-01-01")
  #fr <- locale("fr")
  #expect_equal(parse_datetime("Ven. 1 janv. 2010", "%a %d %b %Y", locale=fr), a)
  #expect_equal(parse_date("Ven. 1 janv. 2010", "%a %d %b %Y", locale=fr), b)
  #expect_warning(parse_datetime("Fri 1 janv. 1020", "%a %d %b %Y", locale=fr))
  #expect_warning(parse_date("Fri 1 janv. 2010", "%a %d %b %Y", locale=fr))
#})

#test_that("locale affects am/pm", {
  #a <- parse_time("1:30 PM", "%H:%M %p")
  #b <- parse_time("오후 1시 30분", "%p %H시 %M분", locale = locale("ko"))
  #expect_equal(a, b)
#})

#test_that("locale affects both guessing and parsing", {
  #out <- parse_guess("01/02/2013", locale = locale(date_format = "%m/%d/%Y"))
  #expect_equal(out, as.Date("2013-01-02"))
#})

#test_that("text re-encoded before strings are parsed", {
  #skip_on_cran() # need to figure out why this fails

  #x <- "1 f\u00e9vrier 2010"
  #y <- iconv(x, to = "ISO-8859-1")
  #feb01 <- as.Date(ISOdate(2010, 02, 01))

  #expect_equal(
    #parse_date(x, "%d %B %Y", locale = locale("fr")),
    #feb01
  #)

  #expect_equal(
    #parse_date(y, "%d %B %Y", locale = locale("fr", encoding = "ISO-8859-1")),
    #feb01
  #)
#})

## Time zones ------------------------------------------------------------------

#test_that("same times with different offsets parsed as same time", {
  ## From http://en.wikipedia.org/wiki/ISO_8601#Time_offsets_from_UTC
  #same_time <- paste("2010-02-03", c("18:30Z", "22:30+04", "1130-0700", "15:00-03:30"))
  #parsed <- parse_datetime(same_time)

  #expect_equal(parsed, rep(utctime(2010, 2, 3, 18, 30, 0, 0), 4))
#})

#test_that("offsets can cross date boundaries", {
  #expect_equal(
    #parse_datetime("2015-01-31T2000-0500"),
    #parse_datetime("2015-02-01T0100Z")
  #)
#})

#test_that("unambiguous times with and without daylight savings", {
  #skip_on_cran() # need to figure out why this fails

  #melb <- locale(tz = "Australia/Melbourne")
  ## Melbourne had daylight savings in 2015 that ended the morning of 2015-04-05
  #expect_equal(
    #parse_datetime(c("2015-04-04 12:00:00", "2015-04-06 12:00:00"), locale = melb),
    #POSIXct(c(1428109200, 1428285600), "Australia/Melbourne")
  #)
  ## Japan didn't have daylight savings in 2015
  #ja <- locale(tz = "Japan")
  #expect_equal(
    #parse_datetime(c("2015-04-04 12:00:00", "2015-04-06 12:00:00"), locale = ja),
    #POSIXct(c(1428116400, 1428289200), "Japan")
  #)
#})


## Guessing ---------------------------------------------------------------------

#test_that("DDDD-DD not parsed as date (i.e. doesn't trigger partial date match)", {
  #expect_equal(guess_parser(c("1989-90", "1990-91")), "character")
#})

#test_that("leading zeros don't get parsed as date without explicit separator", {
  #expect_equal(guess_parser("00010203"), "character")
  #expect_equal(guess_parser("0001-02-03"), "date")
#})

#test_that("must have either two - or none", {
  #expect_equal(guess_parser("2000-10-10"), "date")
  #expect_equal(guess_parser("2000-1010"), "character")
  #expect_equal(guess_parser("200010-10"), "character")
  #expect_equal(guess_parser("20001010"), "double")
#})
