test_that("reading from connection is consistent with reading directly from a file", {
  expected <- vroom(vroom_example("mtcars.csv"), col_types = list())

  # This needs to be small enough to have a few blocks in the file, but big
  # enough to fit on the first line (until #47 is fixed)
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 100), {
    actual <- vroom(file(vroom_example("mtcars.csv"), ""), delim = ",", col_types = list())
  })
  expect_equal(actual, expected)
})

test_that("reading from connection is consistent with reading directly from a file with quoted fields", {
  ir <- iris
  ir$Species <- as.character(ir$Species)

  # add some commas
  locs <- vapply(nchar(ir$Species), sample.int, integer(1), 1)
  substr(ir$Species, locs, locs) <- ","

  out <- tempfile()
  vroom_write(ir, out, delim = ",")

  expected <- vroom(out, col_types = list())

  # This needs to be small enough to have a few blocks in the file, but big
  # enough to fit on the first line (until #47 is fixed)
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 100), {
    actual <- vroom(file(out), col_types = list())
  })
  expect_equal(actual, expected)
})

test_that("vroom errors when the connection buffer is too small", {
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 32), {
    expect_error(vroom(file(vroom_example("mtcars.csv")), col_types = list()), "not large enough")
  })
})

test_that("vroom can read files with only a single line and no newlines", {
  f <- tempfile()
  on.exit(unlink(f))

  writeChar("a,b,c", eos = NULL, f)

  # with a header
  expect_named(vroom(f, delim = ",", col_types = list()), c("a", "b", "c"))
  expect_named(vroom(f, col_types = list()), c("a", "b", "c"))

  # without a header
  expect_equal(vroom(f, col_names = FALSE, delim = ",", col_types = list()), tibble::tibble(X1 = "a", X2 = "b", X3 = "c"))
  expect_equal(vroom(f, col_names = FALSE, col_types = list()), tibble::tibble(X1 = "a", X2 = "b", X3 = "c"))
})
