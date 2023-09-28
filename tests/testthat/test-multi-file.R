test_that("vroom adds the id column from the filename for one file", {
  res <- vroom(vroom_example("mtcars.csv"), id = "filename", col_types = list())
  expect_true(all(res$filename == vroom_example("mtcars.csv")))
})

test_that("vroom adds the id column from the filename for multiple files", {
  dir <- tempfile()
  dir.create(dir)
  
  splits <- split(mtcars, mtcars$cyl)
  for (i in seq_along(splits)) {
    vroom_write(splits[[i]], file.path(dir, paste0("mtcars_", names(splits)[[i]], ".tsv")), delim = "\t")
  }
  
  files <- list.files(dir, full.names = TRUE)
  
  res <- vroom(files, id = "filename", col_types = list())
  
  # construct what the filename column should look like
  filenames <- paste0("mtcars_", rep(names(splits), vapply(splits, nrow, integer(1))), ".tsv")
  
  expect_equal(basename(res$filename), filenames)
})

test_that("vroom adds the id column from the filename for multiple connections", {
  dir <- tempfile()
  dir.create(dir)
  
  splits <- split(mtcars, mtcars$cyl)
  for (i in seq_along(splits)) {
    # write_tsv will automatically gzip them
    vroom_write(splits[[i]], file.path(dir, paste0("mtcars_", names(splits)[[i]], ".tsv.gz")), delim = "\t")
  }
  
  files <- list.files(dir, full.names = TRUE)
  
  res <- vroom(files, id = "filename", col_types = list())
  
  # construct what the filename column should look like
  filenames <- paste0("mtcars_", rep(names(splits), vapply(splits, nrow, integer(1))), ".tsv.gz")
  
  expect_equal(basename(res$filename), filenames)
})

test_that("vroom works with many files", {
  skip_on_os("solaris")
  
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  
  for (i in seq_len(200)) {
    vroom_write(
      tibble::tibble(
        x = rnorm(10),
        y = rnorm(10),
      ),
      file.path(dir, paste0(i, ".csv")),
      delim = ","
    )
  }
  
  files <- list.files(dir, pattern = ".*[.]csv", full.names = TRUE)
  
  res <- vroom::vroom(files, col_types = list())
  
  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 2000)
})

test_that("vroom works with many connections", {
  skip_on_os("solaris")
  
  dir <- withr::local_tempdir()
  
  # the number of files is intentionally larger than 128, which has
  # historically been the maximum number of connections allowed by R
  # in R >= 4.4, 128 is likely to be the new default, with higher values
  # allowed
  # https://github.com/tidyverse/vroom/issues/64
  # https://github.com/tidyverse/vroom/commit/a41465d70db37ab2bc628ff1e606b71c410fb0e3
  for (i in seq_len(200)) {
    vroom_write(
      tibble::tibble(
        x = rnorm(10),
        y = rnorm(10),
      ),
      file.path(dir, paste0(i, ".csv.gz")),
      delim = ","
    )
  }
  
  files <- list.files(dir, pattern = ".*[.]csv[.]gz", full.names = TRUE)

  # vroom manages the connections internally
  res <- vroom::vroom(files, col_types = list())
  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 2000)

  # use explicit connections, so we don't ask for anything close to R's max
  connections <- lapply(files[1:20], gzfile)
  
  res <- vroom::vroom(connections, col_types = list())
  
  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 200)
})

test_that("vroom errors if numbers of columns are inconsistent", {
  files <- test_path("multi-file", c("foo", "baz"))
  expect_error(vroom::vroom(files, col_types = list()), "must all have")
})

test_that("vroom errors if column names are inconsistent", {
  files <- test_path("multi-file", c("foo", "bar"))
  expect_error(vroom::vroom(files, col_types = list()), "consistent column names")
})

test_that("vroom works if a file contains no data", {
  files <- test_path("multi-file", c("foo", "qux"))
  res <- vroom(files, col_types = list())
  expect_equal(res, tibble::tibble(A = 1, B = 2))
})

test_that("vroom works if some files contain no data, regardless of order (#430)", {
  destdir <- withr::local_tempdir("testing-multiple-files")
  
  vroom_write_lines(c("A,B"), file.path(destdir, "header_only.csv"))
  vroom_write_lines(c("A,B"), file.path(destdir, "another_header_only.csv"))
  vroom_write_lines(c("A,B", "1,2"), file.path(destdir, "header_and_one_row.csv"))
  
  files <- file.path(destdir, c("header_only.csv", "header_and_one_row.csv"))
  res <- vroom(files, show_col_types = FALSE)
  expect_equal(res, tibble::tibble(A = 1, B = 2))
  
  files <- file.path(destdir, c(
    "header_only.csv",
    "another_header_only.csv",
    "header_and_one_row.csv"
  ))
  res <- vroom(files, show_col_types = FALSE)
  expect_equal(res, tibble::tibble(A = 1, B = 2))
  
  files <- file.path(destdir, c(
    "header_only.csv",
    "header_and_one_row.csv",
    "another_header_only.csv"
  ))
  res <- vroom(files, show_col_types = FALSE)
  expect_equal(res, tibble::tibble(A = 1, B = 2))
  
  files <- file.path(destdir, c(
    "header_and_one_row.csv",
    "header_only.csv",
    "another_header_only.csv"
  ))
  res <- vroom(files, show_col_types = FALSE)
  expect_equal(res, tibble::tibble(A = 1, B = 2))
  
  files <- file.path(destdir, c(
    "header_only.csv",
    "another_header_only.csv"
  ))
  res <- vroom(files, show_col_types = FALSE)
  x <- tibble::tibble(A = "", B = "", .rows = 0)
  expect_equal(res, x)
})

test_that("vroom works for indxes that span file boundries (#383)", {
  x <- vroom(c(vroom_example("mtcars.csv"), vroom_example("mtcars.csv")), col_types = list())
  y <- rbind(mtcars, mtcars)
  idx <- c(c(34, 33), sample(NROW(x), size = 25, replace = T))
  expect_equal(x[idx, 5, drop = TRUE], y[idx, 4])
})
