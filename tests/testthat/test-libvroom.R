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
    # Test with libvroom
    result <- vroom(
      tf,
      delim = delim,
      col_types = col_types,
      use_libvroom = TRUE,
      ...
    )
    expect_equal(result, equals)

    # Force materialization and check again (important for ALTREP)
    for (i in seq_along(result)) {
      force_materialization(result[[i]])
    }
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
    result <- vroom(tf, delim = ",", use_libvroom = TRUE)
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

test_that("libvroom result equals standard vroom result", {
  # This is a key test: verify libvroom produces identical results to standard vroom
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))

  content <- "a,b,c\n1,foo,3.5\n4,bar,6.5\nNA,baz,9.5\n"
  out_con <- file(tf, "wb", encoding = "UTF-8")
  writeBin(charToRaw(content), out_con)
  close(out_con)

  suppressWarnings({
    standard_result <- vroom(
      tf,
      delim = ",",
      col_types = NULL,
      use_libvroom = FALSE
    )
    libvroom_result <- vroom(
      tf,
      delim = ",",
      col_types = NULL,
      use_libvroom = TRUE
    )

    expect_equal(libvroom_result, standard_result)

    # Force materialization and compare again
    for (i in seq_along(standard_result)) {
      force_materialization(standard_result[[i]])
      force_materialization(libvroom_result[[i]])
    }
    expect_equal(libvroom_result, standard_result)
  })
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

test_that("vroom with use_libvroom=TRUE gracefully falls back for .default", {
  test_libvroom(
    "a,b,c\n1,2,3\n4,5,6\n",
    delim = ",",
    col_types = list(.default = "i"),
    equals = tibble::tibble(a = c(1L, 4L), b = c(2L, 5L), c = c(3L, 6L))
  )
})

test_that("vroom with use_libvroom=TRUE gracefully falls back for col_number()", {
  test_libvroom(
    "a\n\"1,234.56\"\n\"7,890.12\"\n",
    delim = "\t",
    col_types = cols(a = col_number()),
    equals = tibble::tibble(a = c(1234.56, 7890.12))
  )
})

test_that("vroom with use_libvroom=TRUE gracefully falls back for col_factor()", {
  test_libvroom(
    "a\napple\nbanana\napple\n",
    delim = ",",
    col_types = cols(a = col_factor(levels = c("apple", "banana"))),
    equals = tibble::tibble(
      a = factor(c("apple", "banana", "apple"), levels = c("apple", "banana"))
    )
  )
})

test_that("vroom with use_libvroom=TRUE gracefully falls back for col_time()", {
  test_libvroom(
    "a\n10:01:01\n12:30:00\n",
    delim = ",",
    col_types = cols(a = col_time()),
    equals = tibble::tibble(a = hms::hms(c(36061, 45000)))
  )
})

test_that("vroom with use_libvroom=TRUE gracefully falls back for col_date() with custom format", {
  test_libvroom(
    "a\n01/20/2023\n06/15/2024\n",
    delim = ",",
    col_types = cols(a = col_date(format = "%m/%d/%Y")),
    equals = tibble::tibble(a = as.Date(c("2023-01-20", "2024-06-15")))
  )
})

test_that("vroom with use_libvroom=TRUE gracefully falls back for col_big_integer()", {
  test_libvroom(
    "a\n1234567890123\n9876543210987\n",
    delim = ",",
    col_types = cols(a = col_big_integer()),
    equals = tibble::tibble(
      a = bit64::as.integer64(c("1234567890123", "9876543210987"))
    )
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
    use_libvroom = TRUE,
    show_col_types = FALSE
  )

  # Output should only have selected columns
  expect_equal(names(result), c("a", "c"))

  # But spec should reflect ALL columns in the file
  s <- spec(result)
  expect_equal(length(s$cols), 3)
  expect_true(all(c("a", "b", "c") %in% names(s$cols)))
})
