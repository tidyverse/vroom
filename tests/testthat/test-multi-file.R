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
    delim = ",")
  }

  files <- list.files(dir, pattern = ".*[.]csv", full.names = TRUE)

  res <- vroom::vroom(files, col_types = list())

  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 2000)
})

test_that("vroom works with many connections", {
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
      file.path(dir, paste0(i, ".csv.gz")),
    delim = ",")
  }

  files <- list.files(dir, pattern = ".*[.]csv[.]gz", full.names = TRUE)

  res <- vroom::vroom(files, col_types = list())

  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 2000)
})

test_that("vroom errors if numbers of columns are inconsistent", {

  files <- test_path("multi-file", c("foo", "baz"))
  expect_error(vroom::vroom(files, col_types = list()), "must all have")
})

test_that("vroom errors if column names are inconsistent", {

  files <- test_path("multi-file", c("foo", "bar"))
  expect_error(vroom::vroom(files, col_types = list()), "consistent column names")
})
