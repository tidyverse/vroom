test_that("Large exponents can parse", {
  res <- vroom(
    I("1e-63,1e-64\n"),
    delim = ",",
    col_types = "dd",
    col_names = FALSE
  )
  expect_equal(res[[1]], 1e-63)
  expect_equal(res[[2]], 1e-64)
})

test_that("only e and E introduce decimal exponents", {
  test_vroom(
    "x\n1e2\n1E-2\n-2e+1\n",
    delim = "\t",
    col_types = "d",
    equals = tibble::tibble(x = c(100, 0.01, -20))
  )

  values <- c(
    "1F",
    paste0("1176413", c("d", "D", "f", "F", "l", "L", "s", "S"), "03")
  )
  input <- paste0(c("x", values, ""), collapse = "\n")
  for (value in c("1F", "1176413S03")) {
    test_vroom(
      paste0("x\n", value, "\n"),
      delim = "\t",
      equals = tibble::tibble(x = value)
    )
  }

  expect_warning(
    parsed <- vroom(I(input), delim = "\t", col_types = "d", altrep = FALSE),
    class = "vroom_parse_issue"
  )
  expect_identical(parsed$x, rep(NA_real_, length(values)))
  expect_identical(problems(parsed)$actual, values)
})

test_that("Doubles parse correctly with comma as decimal separator", {
  res <- vroom(
    I("23,4\n"),
    delim = '\t',
    altrep = FALSE,
    locale = locale(decimal_mark = ','),
    col_types = 'd',
    col_names = FALSE
  )
  expect_equal(res[[1]], 23.4)
  res2 <- vroom(
    I("23,4\n"),
    delim = '\t',
    altrep = TRUE,
    locale = locale(decimal_mark = ','),
    col_types = 'd',
    col_names = FALSE
  )
  expect_equal(res2[[1]], 23.4)
})

test_that("NA can be a double value", {
  test_vroom(
    I("x\n1\n2\n"),
    delim = ",",
    col_types = "d",
    na = "1",
    equals = tibble::tibble(x = c(NA_real_, 2))
  )
})

test_that("NaN values are guessed and parsed as doubles (https://github.com/tidyverse/readr/issues/1277)", {
  test_vroom(
    I("x\nNaN\n"),
    delim = ",",
    col_types = "?",
    equals = tibble::tibble(x = c(NaN))
  )
})

test_that("Test that values with only a NaN prefix are _not_ parsed as doubles", {
  test_vroom(
    I("x\nNaNa\n"),
    delim = ",",
    col_types = "?",
    equals = tibble::tibble(x = c("NaNa"))
  )
})

test_that("Inf and -Inf values are guessed and parsed as doubles (https://github.com/tidyverse/readr/issues/1283)", {
  test_vroom(
    I("x\nInf\n-Inf\n+Inf"),
    delim = ",",
    col_types = "?",
    equals = tibble::tibble(x = c(Inf, -Inf, Inf))
  )
})

test_that("Test that values with only a Inf prefix are _not_ parsed as doubles", {
  test_vroom(
    I("x\nInfa\n-Infb\n+Infc"),
    delim = ",",
    col_types = "?",
    equals = tibble::tibble(x = c("Infa", "-Infb", "+Infc"))
  )
})
