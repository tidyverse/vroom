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
