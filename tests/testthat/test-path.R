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

  url <- "https://raw.githubusercontent.com/r-lib/vroom/main/inst/extdata/mtcars.csv"
  expect_equal(vroom(url, col_types = list()), mt)
})

test_that("vroom works via https on gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/main/inst/extdata/mtcars.csv.gz"
  expect_equal(vroom(url, col_types = list()), mt)
})

test_that("vroom errors via https on non-gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/r-lib/vroom/main/inst/extdata/mtcars.csv.bz2"
  expect_error(vroom(url, col_types = list()), "Reading from remote `bz2` compressed files is not supported")
})

test_that("split_path_ext works", {
  expect_equal(
    split_path_ext(character()),
    list(path = character(), extension = "")
  )

  expect_equal(
    split_path_ext(""),
    list(path = "", extension = "")
  )

  expect_equal(
    split_path_ext("foo"),
    list(path = "foo", extension = "")
  )

  expect_equal(
    split_path_ext("foo/bar.baz"),
    list(path = "foo/bar", extension = "baz")
  )

  expect_equal(
    split_path_ext("foo/bar.tar.gz"),
    list(path = "foo/bar", extension = "tar.gz")
  )
})

test_that("can write to a zip file if the archive package is available", {
  skip_on_cran()
  skip_if(!rlang::is_installed("archive"))

  tempfile <- file.path(tempdir(), "mtcars.zip")
  on.exit(unlink(tempfile))
  vroom_write(mtcars, tempfile)

  # PK is the zip magic number
  expect_equal(
    readBin(tempfile, raw(), n = 2),
    as.raw(c(0x50, 0x4b))
  )
})

test_that("can write to a tar.gz file if the archive package is available", {
  skip_on_cran()
  skip_if(!rlang::is_installed("archive"))

  tempfile <- file.path(tempdir(), "mtcars.tar.gz")
  on.exit(unlink(tempfile))
  vroom_write(mtcars, tempfile)

  # 1F 8B is the gz magic number
  expect_equal(
    readBin(tempfile, raw(), n = 2),
    as.raw(c(0x1f, 0x8b))
  )

  res <- archive::archive(tempfile)
  expect_equal(res$path, "mtcars")
  expect_equal(res$size, 1281)
})

test_that("can read filepaths with non-ASCII characters", {
  skip_on_cran()
  ## chosen to be non-ASCII but
  ## [1] representable in Windows-1252 and
  ## [2] not any of the few differences between Windows-1252 and ISO-8859-1
  ## a-grave + e-diaeresis  + Eth + '.csv'
  tricky_filename <- withr::local_tempfile(
    fileext = "-\u00C0\u00CB\u00D0\u002E\u0063\u0073\u0076")
  writeLines("a, b, c\n 1, 2, 3\n", tricky_filename)
  expect_true(file.exists(tricky_filename))
  expect_equal(vroom(tricky_filename, show_col_types = FALSE),
               tibble::tibble(a = 1, b = 2, c = 3))

  ## a-grave + e-diaeresis  + '.csv'
  tricky_filename2 <- withr::local_tempfile(
    fileext = "-\u00E0\u00EB\u002E\u0063\u0073\u0076")
  writeLines("a, b, c\n 4, 5, 6\n", tricky_filename2)
  expect_true(file.exists(tricky_filename2))
  expect_equal(vroom(c(tricky_filename, tricky_filename2), show_col_types = FALSE),
               tibble::tibble(a = c(1, 4), b = c(2,5), c = c(3,6)))
})
