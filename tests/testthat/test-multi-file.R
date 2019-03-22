context("test-multi-file.R")


test_that("vroom adds the id column from the filename for one file", {
  res <- vroom(vroom_example("mtcars.csv"), id = "filename")
  expect_true(all(res$filename == vroom_example("mtcars.csv")))
})

test_that("vroom adds the id column from the filename for multiple files", {
  dir <- tempfile()
  dir.create(dir)

  splits <- split(mtcars, mtcars$cyl)
  for (i in seq_along(splits)) {
    readr::write_tsv(splits[[i]], file.path(dir, paste0("mtcars_", names(splits)[[i]], ".tsv")))
  }

  files <- list.files(dir, full.names = TRUE)

  res <- vroom(files, id = "filename")

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
    readr::write_tsv(splits[[i]], file.path(dir, paste0("mtcars_", names(splits)[[i]], ".tsv.gz")))
  }

  files <- list.files(dir, full.names = TRUE)

  res <- vroom(files, id = "filename")

  # construct what the filename column should look like
  filenames <- paste0("mtcars_", rep(names(splits), vapply(splits, nrow, integer(1))), ".tsv.gz")

  expect_equal(basename(res$filename), filenames)
})

test_that("vroom works with many files", {

  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  for (i in seq_len(200)) {
    readr::write_csv(
      tibble::tibble(
        x = rnorm(10),
        y = rnorm(10),
      ),
      file.path(dir, paste0(i, ".csv"))
    )
  }

  files <- list.files(dir, pattern = ".*[.]csv", full.names = TRUE)

  res <- vroom::vroom(files)

  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 2000)
})

test_that("vroom works with many connections", {

  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  for (i in seq_len(200)) {
    readr::write_csv(
      tibble::tibble(
        x = rnorm(10),
        y = rnorm(10),
      ),
      file.path(dir, paste0(i, ".csv.gz"))
    )
  }

  files <- list.files(dir, pattern = ".*[.]csv[.]gz", full.names = TRUE)

  res <- vroom::vroom(files)

  expect_equal(colnames(res), c("x", "y"))
  expect_equal(NROW(res), 2000)
})
