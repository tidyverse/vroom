context("test-path.R")

mt <- vroom(vroom_example("mtcars.csv"))

test_that("read_file works with compressed files", {
  expect_equal(vroom(vroom_example("mtcars.csv.gz")), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.bz2")), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.xz")), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.zip")), mt)
})

test_that("read_file works via https", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/jimhester/vroom/master/inst/extdata/mtcars.csv"
  expect_equal(vroom(url), mt)
})

test_that("read_file works via https on gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/jimhester/vroom/master/inst/extdata/mtcars.csv.gz"
  expect_equal(vroom(url), mt)
})
