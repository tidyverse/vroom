context("logical")

test_that("TRUE and FALSE parsed", {
  test_vroom("TRUE\nFALSE", col_names = FALSE, equals = tibble::tibble(X1 = c(TRUE, FALSE)))
})

test_that("true and false parsed", {
  test_vroom("true\nfalse", col_names = FALSE, equals = tibble::tibble(X1 = c(TRUE, FALSE)))
})

test_that("True and False parsed", {
  test_vroom("True\nFalse", col_names = FALSE, equals = tibble::tibble(X1 = c(TRUE, FALSE)))
})

test_that("T and F parsed", {
  test_vroom("T\nF", col_names = FALSE, equals = tibble::tibble(X1 = c(TRUE, FALSE)))
})

test_that("t and f parsed", {
  test_vroom("t\nf", col_names = FALSE, equals = tibble::tibble(X1 = c(TRUE, FALSE)))
})

test_that("1 and 0 parsed", {
  # 1 and 0 are never guessed as logical, but they can be parsed as such if you
  # explicitly set the column type.
  test_vroom("1\n0", col_types = "l", col_names = FALSE, equals = tibble::tibble(X1 = c(TRUE, FALSE)))
})
