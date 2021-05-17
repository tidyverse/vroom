test_that("vroom_lines works with normal files", {
  infile <- vroom_example("mtcars.csv")

  expected <- readLines(infile)

  actual <- vroom_lines(infile)

  expect_equal(length(actual), length(expected))

  expect_equal(head(actual), head(expected))

  expect_equal(tail(actual), tail(expected))

  expect_equal(actual, expected)
})

test_that("vroom_lines works with connections files", {
  infile <- vroom_example("mtcars.csv")

  con <- file(infile)
  expected <- readLines(con)
  close(con)

  actual <- vroom_lines(file(infile))

  expect_equal(length(actual), length(expected))

  expect_equal(head(actual), head(expected))

  expect_equal(tail(actual), tail(expected))

  expect_equal(actual, expected)
})


test_that("vroom_lines works with files with no trailing newline", {
  f <- tempfile()
  on.exit(unlink(f))

  writeBin(charToRaw("foo"), f)
  expect_equal(vroom_lines(f), "foo")

  f2 <- tempfile()
  on.exit(unlink(f2), add = TRUE)

  writeBin(charToRaw("foo\nbar"), f2)
  expect_equal(vroom_lines(f2), c("foo", "bar"))
})

test_that("vroom_lines respects n_max", {
  infile <- vroom_example("mtcars.csv")
  expect_equal(vroom_lines(infile, n_max = 2), readLines(infile, n = 2))
})

test_that("vroom_lines works with empty files", {
  f <- tempfile()
  file.create(f)
  on.exit(unlink(f))

  expect_equal(vroom_lines(f), character())
})

test_that("vroom_lines uses na argument", {
  expect_equal(vroom_lines(I("abc\n123"), progress = FALSE), c("abc", "123"))
  expect_equal(vroom_lines(I("abc\n123"), na = "abc", progress = FALSE), c(NA_character_, "123"))
  expect_equal(vroom_lines(I("abc\n123"), na = "123", progress = FALSE), c("abc", NA_character_))
  expect_equal(vroom_lines(I("abc\n123"), na = c("abc", "123"), progress = FALSE), c(NA_character_, NA_character_))
})

test_that("vroom_lines works with files with mixed line endings", {
  expect_equal(vroom_lines(I("foo\r\n\nbar\n\r\nbaz\r\n")), c("foo", "", "bar", "", "baz"))
})
