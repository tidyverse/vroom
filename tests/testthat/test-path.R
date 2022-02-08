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

# https://github.com/r-lib/vroom/issues/394
# https://github.com/r-lib/vroom/pull/402
test_that("we can read multi-byte file names", {

  expected <- tibble::tibble(
    id = 1:3,
    name = c("ed", "leigh", "nathan"),
    age = c(36, NA, 14)
  )

  # use the filename 'creme-brulee', but with various accents
  source <- test_path("multi-byte-unicode.txt")
  target <- paste(tempdir(), "cr\u00e8me-br\u00fbl\u00e9e.txt", sep = "/")
  file.copy(source, target)

  # test with the filename as UTF-8
  test_vroom(enc2utf8(target), delim = "\U2764", equals = expected)

  # only run the test if we can represent the filename in the native encoding
  if (target == enc2native(target)) {
    test_vroom(enc2native(target), delim = "\U2764", equals = expected)
  }

})
