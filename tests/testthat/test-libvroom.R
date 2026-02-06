# Tests for libvroom integration layer optimizations
# These tests verify the optimized code paths work correctly

# Helper function to test with libvroom backend
test_libvroom <- function(
  content,
  delim = ",",
  col_types = NULL,
  ...,
  equals
) {
  # Create a temp file since libvroom only supports files, not connections or I()
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw(content), out_con)
  close(out_con)

  suppressWarnings({
    result <- vroom(
      tf,
      delim = delim,
      col_types = col_types,
      show_col_types = FALSE,
      ...
    )
    expect_equal(result, equals)
  })

  invisible(result)
}

test_that("libvroom can read basic CSV files", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n7,8,9\n",
    delim = ",",
    equals = tibble::tibble(
      a = c(1L, 4L, 7L),
      b = c(2L, 5L, 8L),
      c = c(3L, 6L, 9L)
    )
  )

  test_libvroom(
    "x,y,z\nhello,world,test\nfoo,bar,baz\n",
    delim = ",",
    equals = tibble::tibble(
      x = c("hello", "foo"),
      y = c("world", "bar"),
      z = c("test", "baz")
    )
  )
})

test_that("libvroom can read TSV files", {
  test_libvroom(
    "a\tb\tc\n1\t2\t3\n4\t5\t6\n7\t8\t9\n",
    delim = "\t",
    equals = tibble::tibble(
      a = c(1L, 4L, 7L),
      b = c(2L, 5L, 8L),
      c = c(3L, 6L, 9L)
    )
  )
})

test_that("libvroom handles quoted fields correctly", {
  test_libvroom(
    '"a","b","c"\n"foo","bar","baz"\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # Fields with delimiters inside quotes
  test_libvroom(
    '"a","b","c"\n",foo","bar","baz"\n',
    delim = ",",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )
})

# Zero-copy optimization tests (issue #3 optimization 1)
# These test that escape processing is handled correctly
test_that("libvroom handles escaped quotes (double-quote escaping)", {
  # Test doubled quotes - this tests the escape_double path
  test_libvroom(
    'a,b\n"foo ""bar""","baz"\n',
    delim = ",",
    equals = tibble::tibble(a = 'foo "bar"', b = "baz")
  )

  # Multiple escaped quotes in one field
  test_libvroom(
    'a\n"he said ""hello"" and ""goodbye"""\n',
    delim = ",",
    equals = tibble::tibble(a = 'he said "hello" and "goodbye"')
  )
})

test_that("libvroom handles fields without escapes (zero-copy fast path)", {
  # These should take the fast zero-copy path since no escape processing needed
  test_libvroom(
    'a,b,c\nsimple,text,values\n',
    delim = ",",
    equals = tibble::tibble(a = "simple", b = "text", c = "values")
  )

  # Quoted but no escaped quotes - still should be fast path after quote stripping
  test_libvroom(
    '"a","b","c"\n"foo","bar","baz"\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )
})

test_that("libvroom trims whitespace by default (matching old parser)", {
  # libvroom trims leading/trailing whitespace (spaces and tabs) from fields
  test_libvroom(
    "a,b,c\n foo ,  bar  ,baz\n",
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_libvroom(
    "a,b,c\n\tfoo\t,\t\tbar\t\t,baz\n",
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )
})

# Single-file optimization tests (issue #3 optimization 2)
# These test the index_collection bypass for single files
test_that("libvroom single-file optimization works correctly", {
  # This tests that get_column bypasses the full_iterator for single files
  # Should produce identical results to multi-file processing

  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n4,5,6\n7,8,9\n"), out_con)
  close(out_con)

  suppressWarnings({
    result <- vroom(tf, delim = ",")
    expect_equal(
      result,
      tibble::tibble(a = c(1, 4, 7), b = c(2, 5, 8), c = c(3, 6, 9))
    )
  })
})

test_that("libvroom handles NA values correctly", {
  test_libvroom(
    "a,b,c\nNA,2,3\n4,5,6\n",
    delim = ",",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_libvroom(
    "a,b,c\nfoo,2,3\n4,5,6\n",
    delim = ",",
    na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )
})

test_that("libvroom handles different column types", {
  # Integer columns
  test_libvroom(
    "a,b\n1,2\n3,4\n",
    delim = ",",
    col_types = "ii",
    equals = tibble::tibble(a = c(1L, 3L), b = c(2L, 4L))
  )

  # Double columns
  test_libvroom(
    "a,b\n1.5,2.5\n3.5,4.5\n",
    delim = ",",
    col_types = "dd",
    equals = tibble::tibble(a = c(1.5, 3.5), b = c(2.5, 4.5))
  )

  # Character columns
  test_libvroom(
    "a,b\nfoo,bar\nbaz,qux\n",
    delim = ",",
    col_types = "cc",
    equals = tibble::tibble(a = c("foo", "baz"), b = c("bar", "qux"))
  )
})

test_that("libvroom handles explicit col_types with compact string notation", {
  # Integer + double + character
  test_libvroom(
    "a,b,c\n1,2.5,hello\n3,4.5,world\n",
    delim = ",",
    col_types = "idc",
    equals = tibble::tibble(
      a = c(1L, 3L),
      b = c(2.5, 4.5),
      c = c("hello", "world")
    )
  )
})

test_that("libvroom handles logical col_type", {
  test_libvroom(
    "a,b\nTRUE,1\nFALSE,2\n",
    delim = ",",
    col_types = "li",
    equals = tibble::tibble(a = c(TRUE, FALSE), b = c(1L, 2L))
  )
})

test_that("libvroom handles date and datetime col_types", {
  test_libvroom(
    "a,b\n2023-01-20,2018-01-01T10:01:01\n2024-06-15,2019-06-15T12:30:00\n",
    delim = ",",
    col_types = "DT",
    equals = tibble::tibble(
      a = as.Date(c("2023-01-20", "2024-06-15")),
      b = as.POSIXct(
        c("2018-01-01 10:01:01", "2019-06-15 12:30:00"),
        tz = "UTC"
      )
    )
  )
})

test_that("libvroom handles Windows line endings (CRLF)", {
  test_libvroom(
    "a,b\r\n1,2\r\n3,4\r\n",
    delim = ",",
    equals = tibble::tibble(a = c(1, 3), b = c(2, 4))
  )
})

test_that("libvroom reads mixed-type CSV correctly", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  content <- "a,b,c\n1,foo,3.5\n4,bar,6.5\nNA,baz,9.5\n"
  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw(content), out_con)
  close(out_con)

  result <- vroom(
    tf,
    delim = ",",
    col_types = NULL,
    show_col_types = FALSE
  )

  expect_equal(result$a, c(1, 4, NA))
  expect_equal(result$b, c("foo", "bar", "baz"))
  expect_equal(result$c, c(3.5, 6.5, 9.5))
})

test_that("libvroom handles col_skip in compact notation", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    col_types = "i_d",
    equals = tibble::tibble(a = c(1L, 4L), c = c(3, 6))
  )
})

test_that("libvroom handles cols() with named columns", {
  test_libvroom(
    "a,b,c\n1,2.5,hello\n3,4.5,world\n",
    delim = ",",
    col_types = cols(a = col_integer(), b = col_double(), c = col_character()),
    equals = tibble::tibble(
      a = c(1L, 3L),
      b = c(2.5, 4.5),
      c = c("hello", "world")
    )
  )
})

test_that("libvroom handles cols() with partial specification", {
  test_libvroom(
    "a,b,c\n1,2.5,TRUE\n3,4.5,FALSE\n",
    delim = ",",
    col_types = cols(a = col_integer()),
    equals = tibble::tibble(
      a = c(1L, 3L),
      b = c(2.5, 4.5),
      c = c(TRUE, FALSE)
    )
  )
})

test_that("libvroom handles cols_only()", {
  test_libvroom(
    "a,b,c\n1,2.5,hello\n3,4.5,world\n",
    delim = ",",
    col_types = cols_only(a = col_integer(), c = col_character()),
    equals = tibble::tibble(a = c(1L, 3L), c = c("hello", "world"))
  )
})

test_that("libvroom handles cols(.default = col_character())", {
  test_libvroom(
    "a,b,c\n1,2.5,TRUE\n3,4.5,FALSE\n",
    delim = ",",
    col_types = cols(.default = col_character()),
    equals = tibble::tibble(
      a = c("1", "3"),
      b = c("2.5", "4.5"),
      c = c("TRUE", "FALSE")
    )
  )
})

test_that("libvroom handles cols(.default = col_double())", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    col_types = cols(.default = col_double()),
    equals = tibble::tibble(a = c(1, 4), b = c(2, 5), c = c(3, 6))
  )
})

test_that("libvroom handles cols(.default = col_integer())", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    col_types = cols(.default = col_integer()),
    equals = tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("libvroom handles list(.default = 'i') shorthand", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    col_types = list(.default = "i"),
    equals = tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("libvroom handles .default with named column overrides", {
  test_libvroom(
    "a,b,c\n1,2.5,hello\n3,4.5,world\n",
    delim = ",",
    col_types = cols(.default = col_character(), a = col_integer()),
    equals = tibble::tibble(
      a = c(1L, 3L),
      b = c("2.5", "4.5"),
      c = c("hello", "world")
    )
  )
})

test_that("libvroom handles .default = col_logical()", {
  test_libvroom(
    "a,b\nTRUE,FALSE\nFALSE,TRUE\n",
    delim = ",",
    col_types = cols(.default = col_logical()),
    equals = tibble::tibble(a = c(TRUE, FALSE), b = c(FALSE, TRUE))
  )
})

test_that("libvroom handles .default = col_date()", {
  test_libvroom(
    "a,b\n2023-01-20,2024-06-15\n2023-02-10,2024-07-20\n",
    delim = ",",
    col_types = cols(.default = col_date()),
    equals = tibble::tibble(
      a = as.Date(c("2023-01-20", "2023-02-10")),
      b = as.Date(c("2024-06-15", "2024-07-20"))
    )
  )
})

test_that("libvroom handles .default = col_datetime()", {
  test_libvroom(
    "a,b\n2023-01-20T10:01:01,2024-06-15T12:30:00\n2023-02-10T08:00:00,2024-07-20T16:45:00\n",
    delim = ",",
    col_types = cols(.default = col_datetime()),
    equals = tibble::tibble(
      a = as.POSIXct(
        c("2023-01-20 10:01:01", "2023-02-10 08:00:00"),
        tz = "UTC"
      ),
      b = as.POSIXct(
        c("2024-06-15 12:30:00", "2024-07-20 16:45:00"),
        tz = "UTC"
      )
    )
  )
})

test_that("libvroom falls back for unsupported .default types", {
  # col_number needs R-side post-processing and can't be a native default
  test_libvroom(
    "a\n\"1,234.56\"\n\"7,890.12\"\n",
    delim = "\t",
    col_types = cols(.default = col_number()),
    equals = tibble::tibble(a = c(1234.56, 7890.12))
  )
})

test_that("libvroom spec reflects full file schema with col_select", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,hello,3.5\n2,world,4.5\n"), out_con)
  close(out_con)

  result <- vroom(
    tf,
    delim = ",",
    col_select = c(a, c),
    show_col_types = FALSE
  )

  # Output should only have selected columns
  expect_equal(names(result), c("a", "c"))

  # But spec should reflect ALL columns in the file
  s <- spec(result)
  expect_equal(length(s$cols), 3)
  expect_true(all(c("a", "b", "c") %in% names(s$cols)))
})

test_that("libvroom handles comment character", {
  test_libvroom(
    "# this is a comment\na,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    comment = "#",
    equals = tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("libvroom auto-detects delimiter", {
  # CSV auto-detection
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n4,5,6\n"), out_con)
  close(out_con)

  result <- vroom(
    tf,
    delim = NULL,
    show_col_types = FALSE
  )
  expect_equal(
    result,
    tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )

  # TSV auto-detection
  tf2 <- tempfile(fileext = ".tsv")
  on.exit(unlink(tf2), add = TRUE)

  out_con2 <- file(tf2, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a\tb\tc\n1\t2\t3\n4\t5\t6\n"), out_con2)
  close(out_con2)

  result2 <- vroom(
    tf2,
    delim = NULL,
    show_col_types = FALSE
  )
  expect_equal(
    result2,
    tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("libvroom handles skip parameter", {
  test_libvroom(
    "metadata line 1\nmetadata line 2\na,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    skip = 2,
    equals = tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("libvroom handles skip with CRLF line endings", {
  test_libvroom(
    "skip me\r\na,b\r\n1,2\r\n3,4\r\n",
    delim = ",",
    skip = 1,
    equals = tibble::tibble(a = c(1L, 3L), b = c(2L, 4L))
  )
})

test_that("libvroom handles skip combined with comment", {
  test_libvroom(
    "metadata\n# comment\na,b\n1,2\n3,4\n",
    delim = ",",
    skip = 1,
    comment = "#",
    equals = tibble::tibble(a = c(1L, 3L), b = c(2L, 4L))
  )
})

test_that("libvroom handles n_max parameter", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n7,8,9\n",
    delim = ",",
    n_max = 2,
    equals = tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("libvroom can read multiple files", {
  dir <- withr::local_tempdir()

  writeLines("a,b\n1,2\n3,4", file.path(dir, "f1.csv"))
  writeLines("a,b\n5,6\n7,8", file.path(dir, "f2.csv"))

  files <- file.path(dir, c("f1.csv", "f2.csv"))

  result <- vroom(
    files,
    delim = ",",
    show_col_types = FALSE
  )

  expect_equal(
    result,
    tibble::tibble(a = c(1L, 3L, 5L, 7L), b = c(2L, 4L, 6L, 8L))
  )
})

test_that("libvroom adds id column from filename for one file", {
  dir <- withr::local_tempdir()
  writeLines("a,b\n1,2\n3,4", file.path(dir, "f1.csv"))

  result <- vroom(
    file.path(dir, "f1.csv"),
    delim = ",",
    id = "source",
    show_col_types = FALSE
  )

  expect_true("source" %in% names(result))
  expect_true(all(grepl("f1\\.csv$", result$source)))
  expect_equal(result$a, c(1L, 3L))
  expect_equal(result$b, c(2L, 4L))
})

test_that("libvroom adds id column from filename for multiple files", {
  dir <- withr::local_tempdir()

  writeLines("a,b\n1,2\n3,4", file.path(dir, "f1.csv"))
  writeLines("a,b\n5,6\n7,8", file.path(dir, "f2.csv"))

  files <- file.path(dir, c("f1.csv", "f2.csv"))

  result <- vroom(
    files,
    delim = ",",
    id = "source",
    show_col_types = FALSE
  )

  expect_true("source" %in% names(result))
  expect_equal(result$a, c(1L, 3L, 5L, 7L))
  expect_equal(result$b, c(2L, 4L, 6L, 8L))
  expect_true(all(grepl("f1\\.csv$", result$source[1:2])))
  expect_true(all(grepl("f2\\.csv$", result$source[3:4])))
})

test_that("libvroom handles empty files in multi-file reading", {
  dir <- withr::local_tempdir()

  writeLines("a,b", file.path(dir, "empty.csv"))
  writeLines("a,b\n1,2\n3,4", file.path(dir, "data.csv"))

  files <- file.path(dir, c("empty.csv", "data.csv"))

  result <- vroom(
    files,
    delim = ",",
    show_col_types = FALSE
  )

  expect_equal(
    result,
    tibble::tibble(a = c(1L, 3L), b = c(2L, 4L))
  )
})

test_that("libvroom multi-file reads correctly", {
  dir <- withr::local_tempdir()

  writeLines("a,b,c\n1,foo,3.5\n4,bar,6.5", file.path(dir, "f1.csv"))
  writeLines("a,b,c\nNA,baz,9.5\n7,qux,1.5", file.path(dir, "f2.csv"))

  files <- file.path(dir, c("f1.csv", "f2.csv"))

  result <- vroom(
    files,
    delim = ",",
    show_col_types = FALSE
  )

  expect_equal(result$a, c(1, 4, NA, 7))
  expect_equal(result$b, c("foo", "bar", "baz", "qux"))
  expect_equal(result$c, c(3.5, 6.5, 9.5, 1.5))
})

test_that("libvroom handles col_names = FALSE", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("1,2,3\n4,5,6\n"), out_con)
  close(out_con)

  # Should NOT fall back to legacy parser (no warning)
  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = FALSE,
      show_col_types = FALSE
    )
  )
  expect_equal(
    result,
    tibble::tibble(X1 = c(1L, 4L), X2 = c(2L, 5L), X3 = c(3L, 6L))
  )
})

test_that("libvroom handles col_names = FALSE with header-like first row", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n"), out_con)
  close(out_con)

  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = FALSE,
      show_col_types = FALSE
    )
  )
  # First row is data, not header
  expect_equal(
    result,
    tibble::tibble(X1 = c("a", "1"), X2 = c("b", "2"), X3 = c("c", "3"))
  )
})

test_that("libvroom handles custom col_names as character vector", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("1,2,3\n4,5,6\n"), out_con)
  close(out_con)

  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = c("foo", "bar", "baz"),
      show_col_types = FALSE
    )
  )
  expect_equal(
    result,
    tibble::tibble(foo = c(1L, 4L), bar = c(2L, 5L), baz = c(3L, 6L))
  )
})

test_that("libvroom handles custom col_names with header row treated as data", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n"), out_con)
  close(out_con)

  # When col_names is a character vector, first row is data
  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = c("x", "y", "z"),
      show_col_types = FALSE
    )
  )
  expect_equal(
    result,
    tibble::tibble(x = c("a", "1"), y = c("b", "2"), z = c("c", "3"))
  )
})

test_that("libvroom col_names works correctly", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,foo,3.5\n4,bar,6.5\n"), out_con)
  close(out_con)

  # col_names = FALSE
  expect_no_warning(
    res <- vroom(
      tf,
      delim = ",",
      col_names = FALSE,
      show_col_types = FALSE
    )
  )
  expect_equal(
    res,
    tibble::tibble(
      X1 = c("a", "1", "4"),
      X2 = c("b", "foo", "bar"),
      X3 = c("c", "3.5", "6.5")
    )
  )

  # col_names = character vector
  expect_no_warning(
    res2 <- vroom(
      tf,
      delim = ",",
      col_names = c("x", "y", "z"),
      show_col_types = FALSE
    )
  )
  expect_equal(
    res2,
    tibble::tibble(
      x = c("a", "1", "4"),
      y = c("b", "foo", "bar"),
      z = c("c", "3.5", "6.5")
    )
  )
})

test_that("libvroom handles named col_types with custom col_names", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  # Use col_types that differ from what inference would produce:
  # "1","3" would infer as INT32, but we force character
  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("1,2.5,hello\n3,4.5,world\n"), out_con)
  close(out_con)

  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = c("x", "y", "z"),
      col_types = cols(x = col_character()),
      show_col_types = FALSE
    )
  )
  # x forced to character (not integer from inference)
  expect_equal(
    result,
    tibble::tibble(x = c("1", "3"), y = c(2.5, 4.5), z = c("hello", "world"))
  )
})

test_that("libvroom handles positional col_types with col_names = FALSE", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("1,2.5,hello\n3,4.5,world\n"), out_con)
  close(out_con)

  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = FALSE,
      col_types = "idc",
      show_col_types = FALSE
    )
  )
  expect_equal(
    result,
    tibble::tibble(
      X1 = c(1L, 3L),
      X2 = c(2.5, 4.5),
      X3 = c("hello", "world")
    )
  )
})

test_that("libvroom handles cols_only() with custom col_names", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("1,2.5,hello\n3,4.5,world\n"), out_con)
  close(out_con)

  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      col_names = c("x", "y", "z"),
      col_types = cols_only(x = col_integer(), z = col_character()),
      show_col_types = FALSE
    )
  )
  expect_equal(
    result,
    tibble::tibble(x = c(1L, 3L), z = c("hello", "world"))
  )
})

test_that("libvroom reads gzip-compressed CSV files", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  actual <- vroom(
    vroom_example("mtcars.csv.gz"),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("libvroom reads bz2-compressed CSV files", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  actual <- vroom(
    vroom_example("mtcars.csv.bz2"),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("libvroom reads xz-compressed CSV files", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  actual <- vroom(
    vroom_example("mtcars.csv.xz"),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("libvroom reads zip-compressed CSV files", {
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  actual <- vroom(
    vroom_example("mtcars.csv.zip"),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("libvroom reads I() literal data", {
  actual <- vroom(
    I("a,b,c\n1,2,3\n4,5,6\n"),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(
    actual,
    tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("libvroom handles Latin-1 encoding via transcoding", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  # Write Latin-1 encoded file with e-acute (0xE9)
  content_utf8 <- "a,b\ncaf\u00e9,1\n"
  content_latin1 <- iconv(
    content_utf8,
    from = "UTF-8",
    to = "latin1",
    toRaw = TRUE
  )[[1]]
  writeBin(content_latin1, tf)

  # Should NOT fall back (no warning)
  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      locale = locale(encoding = "latin1"),
      show_col_types = FALSE
    )
  )

  expect_equal(result$a, "caf\u00e9")
  expect_equal(result$b, 1L)
})

test_that("libvroom Latin-1 encoding reads correctly", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  content_utf8 <- "a,b\ncaf\u00e9,1\nna\u00efve,2\n"
  content_latin1 <- iconv(
    content_utf8,
    from = "UTF-8",
    to = "latin1",
    toRaw = TRUE
  )[[1]]
  writeBin(content_latin1, tf)

  result <- vroom(
    tf,
    delim = ",",
    locale = locale(encoding = "latin1"),
    show_col_types = FALSE
  )

  expect_equal(result$a, c("caf\u00e9", "na\u00efve"))
  expect_equal(result$b, c(1L, 2L))
})

test_that("libvroom handles UTF-16 encoding via transcoding", {
  bom <- as.raw(c(255, 254))
  text <- "x,y\ncaf\u00e9,2\n"
  text_utf16 <- iconv(text, from = "UTF-8", to = "UTF-16LE", toRaw = TRUE)[[1]]

  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  fd <- file(tf, "wb")
  writeBin(bom, fd)
  writeBin(text_utf16, fd)
  close(fd)

  expect_no_warning(
    result <- vroom(
      tf,
      delim = ",",
      locale = locale(encoding = "UTF-16"),
      col_types = "ci",
      show_col_types = FALSE
    )
  )

  expect_equal(result$x, "caf\u00e9")
  expect_equal(result$y, 2L)
})

test_that("libvroom reads remote CSV files", {
  skip_if_offline()

  actual <- vroom(
    "https://raw.githubusercontent.com/tidyverse/vroom/main/inst/extdata/mtcars.csv",
    delim = ",",
    show_col_types = FALSE
  )
  expected <- vroom(
    vroom_example("mtcars.csv"),
    delim = ",",
    show_col_types = FALSE
  )
  expect_equal(actual, expected)
  expect_s3_class(actual, "spec_tbl_df")
})

test_that("libvroom reports problems for inconsistent field count", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  # Row 2 has 2 fields instead of 3
  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n4,5\n7,8,9\n"), out_con)
  close(out_con)

  result <- vroom(
    tf,
    delim = ",",
    col_types = "iii",
    show_col_types = FALSE
  )

  probs <- problems(result)
  expect_s3_class(probs, "tbl_df")
  expect_true(nrow(probs) > 0)
  expect_named(probs, c("row", "col", "expected", "actual", "file"))
})

test_that("libvroom problems() returns empty tibble for clean data", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n4,5,6\n"), out_con)
  close(out_con)

  result <- vroom(
    tf,
    delim = ",",
    col_types = "iii",
    show_col_types = FALSE
  )

  probs <- problems(result)
  expect_s3_class(probs, "tbl_df")
  expect_equal(nrow(probs), 0)
})

test_that("libvroom problems include file path", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw("a,b,c\n1,2,3\n4,5\n"), out_con)
  close(out_con)

  result <- vroom(
    tf,
    delim = ",",
    col_types = "iii",
    show_col_types = FALSE
  )

  probs <- problems(result)
  expect_true(nrow(probs) > 0)
  expect_true(all(nzchar(probs$file)))
})

test_that("libvroom problems work across multiple files", {
  dir <- withr::local_tempdir()

  writeLines("a,b\n1,2\n3", file.path(dir, "f1.csv"))
  writeLines("a,b\n5,6\n7", file.path(dir, "f2.csv"))

  files <- file.path(dir, c("f1.csv", "f2.csv"))

  result <- vroom(
    files,
    delim = ",",
    col_types = "ii",
    show_col_types = FALSE
  )

  probs <- problems(result)
  expect_true(nrow(probs) >= 2)
  # Both files should appear in problems
  expect_true(any(grepl("f1\\.csv", probs$file)))
  expect_true(any(grepl("f2\\.csv", probs$file)))
})

# ========================================================================
# Backslash escape support
# ========================================================================

test_that("libvroom handles escaped delimiter with backslash", {
  test_libvroom(
    "a,b\nfoo\\,bar,baz\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      a = "foo,bar",
      b = "baz"
    )
  )
})

test_that("libvroom handles escaped quote with backslash", {
  test_libvroom(
    "a,b\n\\\"hello\\\",world\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      a = "\"hello\"",
      b = "world"
    )
  )
})

test_that("libvroom handles escaped backslash (double backslash)", {
  test_libvroom(
    "a,b\nfoo\\\\bar,baz\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      a = "foo\\bar",
      b = "baz"
    )
  )
})

test_that("libvroom handles escaped newline with backslash", {
  test_libvroom(
    "a,b\nfoo\\\nbar,baz\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      a = "foo\nbar",
      b = "baz"
    )
  )
})

test_that("libvroom handles escaped backslash before real quote", {
  # \\\" = escaped backslash + real quote
  test_libvroom(
    "a,b\n\"foo\\\\\",bar\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      a = "foo\\",
      b = "bar"
    )
  )
})

test_that("libvroom handles mixed backslash escapes in one row", {
  test_libvroom(
    "a,b,c\nfoo\\,bar,hello\\\\world,te\\\"st\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "ccc",
    equals = tibble::tibble(
      a = "foo,bar",
      b = "hello\\world",
      c = "te\"st"
    )
  )
})

test_that("libvroom handles backslash with quoted fields", {
  test_libvroom(
    "a,b\n\"foo\\,bar\",baz\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      a = "foo,bar",
      b = "baz"
    )
  )
})

test_that("libvroom handles backslash in header column names", {
  test_libvroom(
    "col\\,1,col2\nfoo,bar\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "cc",
    equals = tibble::tibble(
      `col,1` = "foo",
      col2 = "bar"
    )
  )
})

test_that("libvroom backslash escape reads correctly", {
  content <- "a,b,c\nfoo\\,bar,hello\\\\world,te\\\"st\n"

  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw(content), out_con)
  close(out_con)

  result <- suppressWarnings(vroom(
    tf,
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    col_types = "ccc",
    show_col_types = FALSE
  ))

  expect_equal(result$a, "foo,bar")
  expect_equal(result$b, "hello\\world")
  expect_equal(result$c, "te\"st")
})

test_that("libvroom handles numeric types with backslash escaping", {
  test_libvroom(
    "a,b,c\n1,2.5,TRUE\n3,4.5,FALSE\n",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    equals = tibble::tibble(
      a = c(1, 3),
      b = c(2.5, 4.5),
      c = c(TRUE, FALSE)
    )
  )
})
