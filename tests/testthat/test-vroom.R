context("test-vroom.R")

test_that("vroom can read a tsv", {
  expect_equal(
    vroom("a\tb\tc\n1\t2\t3\n"),
    tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom can read a csv", {
  expect_equal(
    vroom("a,b,c\n1,2,3\n", delim = ","),
    tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom guesses columns with NAs", {
  expect_equal(
    vroom("a,b,c\nNA,2,3\n4,5,6", delim = ","),
    tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  expect_equal(
    vroom("a,b,c\nfoo,2,3\n4,5,6", delim = ",", na = "foo"),
    tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  expect_equal(
    vroom("a,b,c\nfoo,2,3\n4.0,5,6", delim = ",", na = "foo"),
    tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  expect_equal(
    vroom("a,b,c\nfoo,2,3\nbar,5,6", delim = ",", na = "foo"),
    tibble::tibble(a = c(NA, "bar"), b = c(2, 5), c = c(3, 6))
  )
})
