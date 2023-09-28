test_that("vroom errors if the file does not exist", {
  tf <- tempfile()
  expect_error(vroom(tf, col_types = list()), "does not exist")
})

test_that("vroom works with compressed files", {
  mt <- vroom(vroom_example("mtcars.csv"), col_types = list())
  expect_equal(vroom(vroom_example("mtcars.csv.gz"), col_types = list()), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.bz2"), col_types = list()), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.xz"), col_types = list()), mt)
  expect_equal(vroom(vroom_example("mtcars.csv.zip"), col_types = list()), mt)
})

test_that("read_file works via https", {
  skip_on_cran()

  mt <- vroom(vroom_example("mtcars.csv"), col_types = list())
  url <- "https://raw.githubusercontent.com/tidyverse/vroom/main/inst/extdata/mtcars.csv"
  expect_equal(vroom(url, col_types = list()), mt)
})

test_that("vroom works via https on gz file", {
  skip_on_cran()

  mt <- vroom(vroom_example("mtcars.csv"), col_types = list())
  url <- "https://raw.githubusercontent.com/tidyverse/vroom/main/inst/extdata/mtcars.csv.gz"
  expect_equal(vroom(url, col_types = list()), mt)
})

test_that("vroom errors via https on non-gz file", {
  skip_on_cran()

  url <- "https://raw.githubusercontent.com/tidyverse/vroom/main/inst/extdata/mtcars.csv.bz2"
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

test_that("vroom() informs user to use I() for literal data (or not)", {
  expect_snapshot(
    x <- vroom("a,b,c,d\n1,2,3,4", show_col_types = FALSE)
  )
})

test_that("can write to a zip file if the archive package is available", {
  skip_on_cran()
  skip_if_not_installed("archive")

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
  skip_if_not_installed("archive")

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

# https://github.com/tidyverse/vroom/issues/394
test_that("can read file w/o final newline, w/ multi-byte characters in path", {
  pattern <- "no-trailing-n\u00e8wline-m\u00fblti-byt\u00e9-path-"
  tfile <- withr::local_tempfile(pattern = pattern, fileext = ".csv")
  writeChar("a,b\nA,B", con = tfile, eos = NULL)

  expect_equal(
    vroom(tfile, show_col_types = FALSE),
    tibble::tibble(a = "A", b = "B")
  )
})

# for completeness, w.r.t. test above
test_that("can read file w/ final newline, w/ multi-byte characters in path", {
  pattern <- "yes-trailing-n\u00e8wline-m\u00fblti-byt\u00e9-path-"
  tfile <- withr::local_tempfile(pattern = pattern, fileext = ".csv")
  writeLines(c("a,b", "A,B"), tfile)

  expect_equal(
    vroom(tfile, show_col_types = FALSE),
    tibble::tibble(a = "A", b = "B")
  )
})

test_that("can write to path with non-ascii characters", {
  pattern <- "cr\u00E8me-br\u00FBl\u00E9e-"
  tfile <- withr::local_tempfile(pattern = pattern, fileext = ".csv")
  dat <- tibble::tibble(a = "A", b = "B")
  vroom_write(dat, tfile, delim = ",")
  expect_equal(readLines(tfile), c("a,b", "A,B"))
})

test_that("can read/write a compressed file with non-ascii characters in path", {
  skip_on_cran()
  skip_if_not_installed("archive")
  # https://github.com/r-lib/archive/issues/75
  skip_if(l10n_info()$`Latin-1`)

  make_temp_path <- function(ext) file.path(tempdir(), paste0("d\u00E4t", ext))

  gzfile   <- withr::local_file(make_temp_path(".tar.gz"))
  bz2file  <- withr::local_file(make_temp_path(".tar.bz2"))
  xzfile   <- withr::local_file(make_temp_path(".tar.xz"))
  zipfile  <- withr::local_file(make_temp_path(".zip"))

  dat <- tibble::tibble(a = "A", b = "B")

  vroom_write(dat, gzfile)
  vroom_write(dat, bz2file)
  vroom_write(dat, xzfile)
  vroom_write(dat, zipfile)

  expect_equal(detect_compression(gzfile), "gz")
  expect_equal(detect_compression(bz2file), "bz2")
  expect_equal(detect_compression(xzfile), "xz")
  expect_equal(detect_compression(zipfile), "zip")

  expect_equal(vroom(gzfile,  show_col_types = FALSE), dat)
  expect_equal(vroom(bz2file, show_col_types = FALSE), dat)
  expect_equal(vroom(xzfile,  show_col_types = FALSE), dat)
  expect_equal(vroom(zipfile, show_col_types = FALSE), dat)
})

test_that("can read fwf file w/ non-ascii characters in path", {
  tfile <- withr::local_tempfile(pattern = "fwf-y\u00F6-", fileext = ".txt")
  writeLines(c("A B", "C D"), tfile)

  expect_equal(
    spec <- fwf_empty(tfile, col_names = c("a", "b")),
    list(begin = c(0L, 2L), end = c(1L, NA), col_names = c("a", "b"))
  )

  expect_equal(
    vroom_fwf(tfile, spec, show_col_types = FALSE),
    tibble::tibble(a = c("A", "C"), b = c("B", "D"))
  )
})

test_that("standardise_path() errors for a mix of connection and not connection", {
  file <- test_path("multi-file", "foo")
  conn <- file(test_path("multi-file", "bar"))

  # wrap it, so we can check the caller is displayed correctly
  f <- function(some_arg_name) {
    standardise_path(some_arg_name)
  }

  expect_snapshot(
    error = TRUE,
    f(list(file, conn))
  )
})

test_that("standardise_path() errors for invalid input", {
  files <- test_path("multi-file", c("foo", "baz"))
  # wrap it, so we can check the caller is displayed correctly
  f <- function(some_arg_name) {
    standardise_path(some_arg_name)
  }

  expect_snapshot(
    error = TRUE,
    f(as.list(files))
  )
})