mt <- vroom(vroom_example("mtcars.csv"), col_types = list())

test_that("vroom errors if the file does not exist", {

  tf <- tempfile()

  expect_error(vroom(tf, col_types = list()), "does not exist")
})

test_that("vroom works with compressed files", {
  expect_equal(vroom(vroom_example("mtcars.csv.gz"), col_types = list()), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.bz2"), col_types = list()), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.xz"), col_types = list()), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.zip"), col_types = list()), mt)
})

test_that("read_file works via https", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/master/inst/extdata/mtcars.csv"
  expect_equal(vroom(url, col_types = list()), mt)
})

test_that("vroom works via https on gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/master/inst/extdata/mtcars.csv.gz"
  expect_equal(vroom(url, col_types = list()), mt)
})

test_that("vroom errors via https on non-gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/master/inst/extdata/mtcars.csv.bz2"
  expect_error(vroom(url, col_types = list()), "Reading from remote `bz2` compressed files is not supported")
})

test_that("informative error message when writing to zip file", {
  expect_error(vroom_write(mtcars, "mtcars.zip"), ".zip")
})
