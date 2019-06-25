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
