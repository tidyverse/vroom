context("test-path.R")

mt <- vroom(vroom_example("mtcars.csv"))

test_that("vroom errors if the file does not exist", {

  tf <- tempfile()

  expect_error(vroom(tf), "does not exist", class = "Rcpp::eval_error")
})

test_that("vroom works with compressed files", {
  expect_equal(vroom(vroom_example("mtcars.csv.gz")), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.bz2")), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.xz")), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.zip")), mt)
})

test_that("read_file works via https", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/master/inst/extdata/mtcars.csv"
  expect_equal(vroom(url), mt)
})

test_that("vroom works via https on gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/master/inst/extdata/mtcars.csv.gz"
  expect_equal(vroom(url), mt)
})

test_that("vroom errors via https on non-gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/master/inst/extdata/mtcars.csv.bz2"
  expect_error(vroom(url), "Reading from remote `bz2` compressed files is not supported", class = "Rcpp::eval_error")
})
