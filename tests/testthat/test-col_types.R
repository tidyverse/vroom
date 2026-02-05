test_that("You can use !! in cols and cols_only", {
  var <- "xyz"
  expect_equal(
    cols(!!var := col_character()),
    cols(xyz = col_character())
  )

  expect_equal(
    cols_only(!!var := col_character()),
    cols_only(xyz = col_character())
  )
})

test_that("format(col_spec) contains the delimiter if specified", {
  expect_match(fixed = TRUE, format(cols(.delim = "\t")), '.delim = "\\t"')
})

test_that("col_types are truncated if you pass too many (#355)", {
  res <- vroom(I("a,b,c,d\n1,2,3,4"), col_types = "cccccccc")
  expect_equal(res, tibble::tibble(a = "1", b = "2", c = "3", d = "4"))
})

test_that("all col_types can be reported with color", {
  local_reproducible_output(crayon = TRUE)
  dat <- vroom(
    I(glue::glue(
      "
      skip,guess,character,factor,logical,double,integer,big_integer,\\
      number,date,datetime
      skip,a,b,c,TRUE,1.3,5,10,\"1,234.56\",2023-01-20,2018-01-01 10:01:01"
    )),
    col_types = "_?cfldiInDT"
  )
  expect_snapshot(spec(dat))
})

# https://github.com/tidyverse/vroom/issues/548
test_that("col_number() works with empty data", {
  out <- vroom(I(""), col_types = "in")
  expect_equal(out, tibble::tibble(X1 = integer(), X2 = numeric()))
})

# https://github.com/tidyverse/vroom/issues/540
test_that("col_skip works with empty data (#540)", {
  out <- vroom(I("a\tb\tc\n"), delim = "\t", col_types = "c-d", skip = 1L)
  expect_equal(out, tibble::tibble(X1 = character(), X3 = numeric()))

  # All columns skipped
  out <- vroom(I("a\tb\n"), delim = "\t", col_types = "--", skip = 1L)
  expect_equal(out, tibble::tibble())
})

test_that("cols() errors for invalid collector objects", {
  expect_snapshot(
    cols(a = col_character(), b = 123),
    error = TRUE
  )

  expect_snapshot(
    cols(a = col_character(), .default = 123),
    error = TRUE
  )
})

test_that("cols() errors for invalid character specifications", {
  expect_snapshot(cols(X = "z", Y = "i", Z = col_character()), error = TRUE)
  expect_snapshot(cols(X = "i", .default = "wut"), error = TRUE)
})

test_that("as.col_spec() errors for unhandled input type", {
  expect_snapshot(
    vroom(I("whatever"), col_types = data.frame()),
    error = TRUE
  )
})

test_that("as.col_spec() errors for unrecognized single-letter spec", {
  expect_snapshot(vroom(I("whatever"), col_types = "dz"), error = TRUE)
})


