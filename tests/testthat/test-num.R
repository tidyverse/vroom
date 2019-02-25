context("test-num.R")

# Flexible number parsing -------------------------------------------------
test_that("col_number only takes first number", {
  test_parse_number("XYZ 123,000 BLAH 456", 123000)
})

test_that("col_number helps with currency", {
  test_parse_number("$1,000,000.00", 1e6)

  es_MX <- locale("es", decimal_mark = ",")
  test_parse_number("$1.000.000,00", locale = es_MX, 1e6)
})

test_that("invalid numbers don't parse", {
  test_parse_number(c("..", "--", "3.3.3", "4-1"), c(NA, NA, 3.3, 4.0))
})
