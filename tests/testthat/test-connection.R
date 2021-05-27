test_that("reading from connection is consistent with reading directly from a file", {
  skip_if(is_windows() && on_github_actions())

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

test_that("vroom works with file connections and quoted fields", {
  f <- tempfile()
  on.exit(unlink(f))
  writeLines('a,b,c\n"1","2","3"\n"4","5","6"', f)
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 8), {
    x <- vroom(file(f), delim = ",", col_types = list())
  })
  expect_equal(x, tibble::tibble(a = c(1, 4), b = c(2, 5), c = c(3, 6)))
})

test_that("vroom works with windows newlines and a connection size that lies directly on the newline", {
  tf <- tempfile()
  on.exit(unlink(tf))

  writeChar("1,2\r\na,bbb\r\ne,f\r\n", tf, eos = NULL)

  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 12), {
    x <- vroom(file(tf), col_types = "cc")
  })
  expect_equal(x[[1]], c("a", "e"))
})
