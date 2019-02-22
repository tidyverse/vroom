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
