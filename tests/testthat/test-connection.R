test_that("reading from connection is consistent with reading directly from a file", {
  skip_if(is_windows() && on_github_actions())

  expected <- vroom(vroom_example("mtcars.csv"), col_types = list())

  # This needs to be small enough to have a few blocks in the file, but big
  # enough to fit on the first line (until #47 is fixed)
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 100), {
    actual <- vroom(
      file(vroom_example("mtcars.csv"), ""),
      delim = ",",
      col_types = list()
    )
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

test_that("reading from a file() connection uses libvroom when eligible", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  actual <- vroom(
    file(vroom_example("mtcars.csv")),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("reading from a gzfile() connection works via libvroom", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  actual <- vroom(
    gzfile(vroom_example("mtcars.csv.gz")),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("reading from a rawConnection works via libvroom", {
  raw_data <- charToRaw("a,b,c\n1,2,3\n4,5,6\n")
  actual <- vroom(rawConnection(raw_data), delim = ",", show_col_types = FALSE)
  expect_equal(nrow(actual), 2)
  expect_equal(ncol(actual), 3)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("connection reads work with small VROOM_CONNECTION_SIZE via libvroom", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = "100"), {
    actual <- vroom(
      file(vroom_example("mtcars.csv")),
      delim = ",",
      show_col_types = FALSE
    )
  })
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("vroom handles very small connection buffer sizes", {
  # libvroom's connection reading handles small buffer sizes gracefully;
  # it reads and concatenates all chunks before parsing.
  withr::local_envvar(c("VROOM_CONNECTION_SIZE" = 32))
  expected <- vroom(vroom_example("mtcars.csv"), col_types = list())
  actual <- vroom(file(vroom_example("mtcars.csv")), col_types = list())
  expect_equal(actual, expected)
})

test_that("vroom can read files with only a single line and no newlines", {
  f <- tempfile()
  on.exit(unlink(f))

  writeChar("a,b,c", eos = NULL, f)

  # with a header
  expect_named(vroom(f, delim = ",", col_types = list()), c("a", "b", "c"))
  expect_named(vroom(f, col_types = list()), c("a", "b", "c"))

  # without a header
  expect_equal(
    vroom(f, col_names = FALSE, delim = ",", col_types = list()),
    tibble::tibble(X1 = "a", X2 = "b", X3 = "c")
  )
  expect_equal(
    vroom(f, col_names = FALSE, col_types = list()),
    tibble::tibble(X1 = "a", X2 = "b", X3 = "c")
  )
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

  writeChar("x,y\r\na,bbb\r\ne,f\r\n", tf, eos = NULL)

  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 12), {
    x <- vroom(file(tf), col_types = "cc")
  })
  expect_equal(x[[1]], c("a", "e"))
})

# https://github.com/tidyverse/vroom/issues/488
test_that("vroom() doesn't leak a connection when opening fails (bad URL)", {
  skip_if_offline()
  connections_before <- showConnections(all = TRUE)

  # Try to read from a bad URL (should fail with 404)
  bad_url <- "https://cloud.r-project.org/CRAN_mirrorsZ.csv"
  # Not using snapshots, because not our error or warning
  expect_error(
    expect_warning(
      vroom(bad_url, show_col_types = FALSE)
    ),
    "cannot open"
  )

  connections_after <- showConnections(all = TRUE)
  expect_equal(nrow(connections_before), nrow(connections_after))
})

test_that("vroom_fwf() doesn't leak a connection when opening fails (permission denied)", {
  skip_on_os("windows")

  tfile <- withr::local_tempfile(
    lines = c("col1  col2  col3", "val1  val2  val3"),
    pattern = "no-permissions-",
    fileext = ".txt"
  )
  Sys.chmod(tfile, mode = "000") # Remove all permissions
  connections_before <- showConnections(all = TRUE)

  # Not using snapshots, because not our error or warning
  expect_error(
    expect_warning(
      vroom_fwf(file(tfile), fwf_widths(c(6, 6, 6)), show_col_types = FALSE)
    ),
    "cannot open"
  )

  connections_after <- showConnections(all = TRUE)
  expect_equal(nrow(connections_before), nrow(connections_after))
})

test_that("reading no data, from a connection", {
  skip_if_offline()
  # inspired by:
  # https://github.com/tidyverse/vroom/issues/539
  # remote compressed file with n_max = 0 and explicit col_names
  expect_equal(
    vroom(
      "https://vroom.tidyverse.org/mtcars.csv.gz",
      col_names = c("a", "b", "c"),
      n_max = 0,
      show_col_types = FALSE
    ),
    tibble::tibble(a = character(), b = character(), c = character())
  )

  # remote file without extension - tests default switch case
  expect_equal(
    vroom(
      "https://vroom.tidyverse.org/mtcars",
      col_names = c("a", "b", "c"),
      n_max = 0,
      show_col_types = FALSE
    ),
    tibble::tibble(a = character(), b = character(), c = character())
  )
})
