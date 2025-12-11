test_that("locale() creates a valid locale object", {
  loc <- locale()
  expect_s3_class(loc, "locale")
  expect_equal(loc$decimal_mark, ".")
  expect_equal(loc$grouping_mark, ",")
  expect_equal(loc$tz, "UTC")
  expect_equal(loc$encoding, "UTF-8")
})

test_that("locale() errors when decimal_mark equals grouping_mark", {
  expect_snapshot(
    locale(decimal_mark = ".", grouping_mark = "."),
    error = TRUE
  )
})

test_that("locale() sets complementary grouping_mark when only decimal_mark is specified", {
  loc1 <- locale(decimal_mark = ",")
  expect_equal(loc1$decimal_mark, ",")
  expect_equal(loc1$grouping_mark, ".")

  loc2 <- locale(decimal_mark = ".")
  expect_equal(loc2$decimal_mark, ".")
  expect_equal(loc2$grouping_mark, ",")
})

test_that("locale() sets complementary decimal_mark when only grouping_mark is specified", {
  loc1 <- locale(grouping_mark = ".")
  expect_equal(loc1$decimal_mark, ",")
  expect_equal(loc1$grouping_mark, ".")

  loc2 <- locale(grouping_mark = ",")
  expect_equal(loc2$decimal_mark, ".")
  expect_equal(loc2$grouping_mark, ",")
})

test_that("locale() accepts language codes for date_names", {
  fr <- locale("fr")
  expect_equal(fr$date_names$mon[[1]], "janvier")

  es <- locale("es")
  expect_equal(es$date_names$mon[[1]], "enero")
})

test_that("locale() errors for unrecognized language code", {
  expect_snapshot(
    locale(date_names = "fake"),
    error = TRUE
  )
})

test_that("locale() warns for unrecognized encoding", {
  expect_snapshot(
    locale(encoding = "FAKE-ENCODING-9999")
  )
})

test_that("locale() validates timezone", {
  expect_no_error(locale(tz = "UTC"))
  expect_no_error(locale(tz = "America/New_York"))

  expect_snapshot(
    locale(tz = "Invalid/Timezone"),
    error = TRUE
  )
})

test_that("locale() can consult and validate system time zone", {
  expect_no_error(locale(tz = ""))

  withr::local_timezone("foo")
  expect_snapshot(
    locale(tz = ""),
    error = TRUE
  )
})

test_that("default_locale() returns a locale", {
  loc <- default_locale()
  expect_s3_class(loc, "locale")
})
