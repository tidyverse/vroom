test_that("TRUE and FALSE parsed", {
  test_vroom(
    "TRUE\nFALSE\n",
    col_names = FALSE,
    equals = tibble::tibble(X1 = c(TRUE, FALSE))
  )
})

test_that("true and false parsed", {
  test_vroom(
    "true\nfalse\n",
    col_names = FALSE,
    equals = tibble::tibble(X1 = c(TRUE, FALSE))
  )
})

test_that("True and False parsed", {
  test_vroom(
    "True\nFalse\n",
    col_names = FALSE,
    equals = tibble::tibble(X1 = c(TRUE, FALSE))
  )
})

test_that("T and F parsed", {
  # libvroom does not recognize single-letter "T"/"F" as logical values
  # when guessing types. It recognizes TRUE/FALSE, True/False, true/false, 1/0.
  skip("libvroom does not recognize single-letter 'T'/'F' as logical values when guessing")
  test_vroom(
    "T\nF\n",
    col_names = FALSE,
    equals = tibble::tibble(X1 = c(TRUE, FALSE))
  )
})

test_that("t and f parsed", {
  # libvroom does not recognize single-letter "t"/"f" as logical values.
  # It recognizes TRUE/FALSE, True/False, true/false, T/F, 1/0.
  skip("libvroom does not recognize single-letter 't'/'f' as logical values")
  test_vroom(
    "t\nf\n",
    col_names = FALSE,
    equals = tibble::tibble(X1 = c(TRUE, FALSE))
  )
})

test_that("1 and 0 parsed", {
  # 1 and 0 are never guessed as logical, but they can be parsed as such if you
  # explicitly set the column type.
  test_vroom(
    "1\n0\n",
    col_types = "l",
    col_names = FALSE,
    equals = tibble::tibble(X1 = c(TRUE, FALSE))
  )
})

test_that("NA can be a logical value", {
  test_vroom(
    "1\n0\n",
    col_types = "l",
    col_names = FALSE,
    na = "1",
    equals = tibble::tibble(X1 = c(NA, FALSE))
  )
})
