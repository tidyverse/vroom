context("test-vroom.R")

test_that("vroom can read a tsv", {
  expect_equivalent(
    vroom("a\tb\tc\n1\t2\t3\n"),
    tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom can read a csv", {
  expect_equivalent(
    vroom("a,b,c\n1,2,3\n", delim = ","),
    tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom guesses columns with NAs", {
  expect_equivalent(
    vroom("a,b,c\nNA,2,3\n4,5,6", delim = ","),
    tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  expect_equivalent(
    vroom("a,b,c\nfoo,2,3\n4,5,6", delim = ",", na = "foo"),
    tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  expect_equivalent(
    vroom("a,b,c\nfoo,2,3\n4.0,5,6", delim = ",", na = "foo"),
    tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  expect_equivalent(
    vroom("a,b,c\nfoo,2,3\nbar,5,6", delim = ",", na = "foo"),
    tibble::tibble(a = c(NA, "bar"), b = c(2, 5), c = c(3, 6))
  )
})

test_that("vroom can read all the column types", {
  expect_equivalent(
    vroom("a,b,c,d\nfoo,2,3.0,T", delim = ","),
    tibble::tibble(a = "foo", b = 2L, c = 3.0, d = TRUE)
  )
})

#test_that("vroom can read a tsv from a connection", {
  #tf <- tempfile()
  #on.exit(unlink(tf))
  #readr::write_lines(c("a\tb\tc", "1\t2\t3"), tf)

  #con <- file(tf, "rb")
  #on.exit(close(con), add = TRUE)

  #res <- vroom(con)

  ## Has a temp_file environment, with a filename
  #tf <- attr(res, "filename")
  #expect_true(is.character(tf))
  #expect_true(file.exists(tf))
  #expect_equivalent(
    #res,
    #tibble::tibble(a = 1, b = 2, c = 3)
  #)

  #rm(res)
  #gc()

  ## Which is removed after the object is deleted and the finalizer has run
  #expect_false(file.exists(tf))
#})
