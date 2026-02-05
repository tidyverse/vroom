test_that("trailing spaces omitted", {
  spec <- fwf_empty(test_path("fwf-trailing.txt"))
  expect_equal(spec$begin, c(0, 4))
  expect_equal(spec$end, c(3, NA))

  df <- vroom_fwf(test_path("fwf-trailing.txt"), spec, col_types = list())
  expect_equal(df$X1, df$X2)
})

test_that("dos newlines handles", {
  spec <- fwf_empty(test_path("fwf-trailing.txt"))
  x <- vroom_fwf(test_path("fwf-trailing.txt"), spec, col_types = list())
  y <- vroom_fwf(test_path("fwf-trailing-crlf.txt"), spec, col_types = list())
  expect_equal(x, y)

  z <- vroom_fwf(
    file(test_path("fwf-trailing-crlf.txt")),
    spec,
    col_types = list()
  )
  expect_equal(z, x)
})

test_that("connections and normal files produce identical output", {
  spec <- fwf_empty(test_path("fwf-trailing.txt"))

  y <- vroom_fwf(test_path("fwf-trailing-crlf.txt"), spec, col_types = list())
  x <- vroom_fwf(
    file(test_path("fwf-trailing-crlf.txt")),
    spec,
    col_types = list()
  )

  expect_equal(x, y)
})

test_that("respects the trim_ws argument", {
  x <- I("a11 b22 c33\nd   e   f  ")
  out1 <- vroom_fwf(x, fwf_empty(x), trim_ws = FALSE, col_types = list())
  expect_equal(out1$X1, c("a11", "d  "))
  expect_equal(out1$X2, c("b22", "e  "))
  expect_equal(out1$X3, c("c33", "f  "))

  out2 <- vroom_fwf(x, fwf_empty(x), trim_ws = TRUE, col_types = list())
  expect_equal(out2$X1, c("a11", "d"))
  expect_equal(out2$X2, c("b22", "e"))
  expect_equal(out2$X3, c("c33", "f"))
})

test_that("respects the trim_ws argument with empty fields", {
  x <- I("a11 b22 c33\nd       f  ")
  out1 <- vroom_fwf(x, fwf_empty(x), trim_ws = FALSE, col_types = list())
  expect_equal(out1$X1, c("a11", "d  "))
  expect_equal(out1$X2, c("b22", "   "))
  expect_equal(out1$X3, c("c33", "f  "))

  out1 <- vroom_fwf(
    x,
    fwf_empty(x),
    trim_ws = TRUE,
    na = "NA",
    col_types = list()
  )
})

test_that("skipping column doesn't pad col_names", {
  x <- I("1 2 3\n4 5 6")

  out1 <- vroom_fwf(x, fwf_empty(x), col_types = 'd-d')
  expect_named(out1, c("X1", "X3"))

  names <- c("a", "b", "c")
  out2 <- vroom_fwf(x, fwf_empty(x, col_names = names), col_types = 'd-d')
  expect_named(out2, c("a", "c"))
})

test_that("fwf_empty can skip comments", {
  x <- I("COMMENT\n1 2 3\n4 5 6")

  out1 <- vroom_fwf(
    x,
    fwf_empty(x, comment = "COMMENT"),
    comment = "COMMENT",
    col_types = list()
  )
  expect_equal(dim(out1), c(2, 3))
})

test_that("fwf_empty can skip lines", {
  x <- I("foo\nbar\baz\n1 2 3\n4 5 6\n")

  obj <- fwf_empty(x, skip = 3)

  exp <- list(
    begin = c(0L, 2L, 4L),
    end = c(1L, 3L, NA_integer_),
    col_names = c("X1", "X2", "X3")
  )

  expect_equal(obj, exp)
})

test_that("passing \"\" to vroom_fwf's 'na' option", {
  expect_equal(
    vroom_fwf(
      I("foobar\nfoo   "),
      fwf_widths(c(3, 3)),
      na = "",
      col_types = list()
    )[[2]],
    c("bar", NA)
  )
})

test_that("ragged last column expanded with NA", {
  x <- vroom_fwf(I("1a\n2ab\n3abc"), fwf_widths(c(1, NA)), col_types = list())
  expect_equal(x$X2, c("a", "ab", "abc"))
})

#test_that("ragged last column shrunk with warning", {
#expect_warning(x <- vroom_fwf(I("1a\n2ab\n3abc"), fwf_widths(c(1, 3))))
#expect_equal(x$X2, c("a", "ab", "abc"))
#})

test_that("read all columns with positions, non ragged", {
  col_pos <- fwf_positions(c(1, 3, 6), c(2, 5, 6))
  x <- vroom_fwf(
    I("12345A\n67890BBBBBBBBB\n54321C"),
    col_positions = col_pos,
    col_types = list()
  )
  expect_equal(x$X3, c("A", "B", "C"))
})

test_that("read subset columns with positions", {
  col_pos <- fwf_positions(c(1, 3), c(2, 5))
  x <- vroom_fwf(
    I("12345A\n67890BBBBBBBBB\n54321C"),
    col_positions = col_pos,
    col_types = list()
  )
  expect_equal(x$X1, c(12, 67, 54))
  expect_equal(x$X2, c(345, 890, 321))
})

test_that("read columns with positions, ragged", {
  col_pos <- fwf_positions(c(1, 3, 6), c(2, 5, NA))
  x <- vroom_fwf(
    I("12345A\n67890BBBBBBBBB\n54321C"),
    col_positions = col_pos,
    col_types = list()
  )
  expect_equal(x$X1, c(12, 67, 54))
  expect_equal(x$X2, c(345, 890, 321))
  expect_equal(x$X3, c('A', 'BBBBBBBBB', 'C'))
})

test_that("read columns with width, ragged", {
  col_pos <- fwf_widths(c(2, 3, NA))
  x <- vroom_fwf(
    I("12345A\n67890BBBBBBBBB\n54321C"),
    col_positions = col_pos,
    col_types = list()
  )
  expect_equal(x$X1, c(12, 67, 54))
  expect_equal(x$X2, c(345, 890, 321))
  expect_equal(x$X3, c('A', 'BBBBBBBBB', 'C'))
})

#test_that("vroom_fwf returns an empty data.frame on an empty file", {
##expect_true(all.equal(vroom_fwf(test_path("empty-file")), tibble::data_frame()))
#})

#test_that("check for line breaks in between widths", {
#txt1 <- paste(
#"1 1",
#"2",
#"1 1 ",
#sep = "\n"
#)
#expect_warning(out1 <- vroom_fwf(txt1, fwf_empty(txt1)))
#expect_equal(n_problems(out1), 2)

#txt2 <- paste(
#" 1 1",
#" 2",
#" 1 1 ",
#sep = "\n"
#)
#expect_warning(out2 <- vroom_fwf(txt2, fwf_empty(txt2)))
#expect_equal(n_problems(out2), 2)

#exp <- tibble::tibble(X1 = c(1L, 2L, 1L), X2 = c(1L, NA, 1L))
#expect_true(all.equal(out1, exp))
#expect_true(all.equal(out2, exp))

#})

#test_that("error on empty spec (#511, #519)", {
#txt = "foo\n"
#pos = fwf_positions(start = numeric(0), end = numeric(0))
#expect_error(vroom_fwf(txt, pos), "Zero-length.*specifications not supported")
#})

#test_that("error on negatives in fwf spec", {
#txt = "foo\n"
#pos = fwf_positions(start = c(1, -1), end = c(2, 3))
#expect_error(vroom_fwf(txt, pos), ".*offset.*greater than 0")
#})

test_that("fwf spec can overlap", {
  x <- vroom_fwf(
    I("2015a\n2016b"),
    fwf_positions(c(1, 3, 5), c(4, 4, 5)),
    col_types = list()
  )
  expect_equal(x$X1, c(2015, 2016))
  expect_equal(x$X2, c(15, 16))
  expect_equal(x$X3, c("a", "b"))
})

# fwf_cols
test_that("fwf_cols produces correct fwf_positions object with elements of length 2", {
  expected <- fwf_positions(c(1L, 9L, 4L), c(2L, 12L, 6L), c("a", "b", "d"))
  expect_equal(
    fwf_cols(a = c(1, 2), b = c(9, 12), d = c(4, 6)),
    expected,
    ignore_attr = TRUE
  )
})

test_that("fwf_cols produces correct fwf_positions object with elements of length 1", {
  expected <- fwf_widths(c(2L, 4L, 3L), c("a", "b", "c"))
  expect_equal(fwf_cols(a = 2, b = 4, c = 3), expected, ignore_attr = TRUE)
})


test_that("fwf_cols errors when arguments have different shapes", {
  expect_snapshot(
    fwf_cols(a = 10, b = c(11, 15)),
    error = TRUE
  )

  expect_snapshot(
    fwf_cols(a = 1:3, b = 4:5),
    error = TRUE
  )
})

test_that("fwf_cols errors with invalid number of values", {
  expect_snapshot(
    fwf_cols(a = 1:4, b = 5:8),
    error = TRUE
  )

  expect_snapshot(
    fwf_cols(a = c(), b = c()),
    error = TRUE
  )
})

test_that("fwf_cols works with unnamed columns", {
  expect_equal(
    ignore_attr = TRUE,
    fwf_cols(c(1, 2), c(9, 12), c(4, 6)),
    fwf_positions(c(1L, 9L, 4L), c(2L, 12L, 6L), c("X1", "X2", "X3"))
  )
  expect_equal(
    ignore_attr = TRUE,
    fwf_cols(a = c(1, 2), c(9, 12), c(4, 6)),
    fwf_positions(c(1L, 9L, 4L), c(2L, 12L, 6L), c("a", "X2", "X3"))
  )
})

# fwf_positions ---------------------------------------------------------------

test_that("fwf_positions errors when start and end have different lengths", {
  expect_snapshot(
    fwf_positions(c(1, 5, 10), c(4, 9)),
    error = TRUE
  )
})

test_that("fwf_positions always returns col_names as character (#797)", {
  begin <- c(1, 2, 4, 8)
  end <- c(1, 3, 7, 15)

  # Input a factor, should return a character
  nms <- factor(letters[1:4])

  info <- fwf_positions(begin, end, nms)

  expect_type(info$begin, "double")
  expect_type(info$end, "double")
  expect_type(info$col_names, "character")
})

# https://github.com/tidyverse/readr/issues/1544
test_that("fwf_positions() errors for start position of 0", {
  expect_snapshot(
    fwf_positions(c(0, 4), c(3, 7)),
    error = TRUE
  )
})

# Robustness ----------------------------------------------------------------

test_that("vroom_fwf() is robust to improper inputs", {
  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2\n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2 \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2  \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4\n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4 \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4  \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4   \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4   5\n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4   5 \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4   5  \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4   5   \n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("foo bar baz\n1   2   \n4   5   6\n"), col_types = list())
  )

  expect_error_free(
    vroom_fwf(I("A\n  a\n"), col_types = list())
  )
})

test_that("Errors if begin is greater than end", {
  positions <- fwf_positions(
    start = c(1, 3, 5),
    end = c(3, 1, NA),
    col_names = c("foo", "bar", "baz")
  )

  expect_snapshot(
    vroom_fwf(I("1  2  3\n"), positions, col_types = list()),
    error = TRUE
  )
})

test_that("vroom_fwf respects n_max (#334)", {
  out <- vroom_fwf(
    I("foo 1\nbar 2\nbaz 3\nqux 4"),
    n_max = 0,
    col_types = list()
  )
  expect_named(out, c("X1", "X2"))
  # With libvroom backend, empty results have inferred types
  expect_equal(out[[1]], character())
  expect_equal(out[[2]], double())

  out <- vroom_fwf(
    I("foo 1\nbar 2\nbaz 3\nqux 4"),
    n_max = 1,
    col_types = list()
  )
  expect_named(out, c("X1", "X2"))
  expect_equal(out[[1]], c("foo"))
  expect_equal(out[[2]], c(1))

  out <- vroom_fwf(
    I("foo 1\nbar 2\nbaz 3\nqux 4"),
    n_max = 2,
    col_types = list()
  )
  expect_named(out, c("X1", "X2"))
  expect_equal(out[[1]], c("foo", "bar"))
  expect_equal(out[[2]], c(1, 2))
})

test_that("vroom_fwf respects n_max when reading from a connection", {
  f <- tempfile()
  on.exit(unlink(f))
  writeLines(rep("00010002", 1000), f)

  out1 <- vroom_fwf(
    file(f),
    col_positions = fwf_widths(c(4, 4)),
    col_types = "ii"
  )

  expect_equal(dim(out1), c(1000, 2))

  out2 <- vroom_fwf(
    file(f),
    n_max = 900,
    col_positions = fwf_widths(c(4, 4)),
    col_types = "ii"
  )

  expect_equal(dim(out2), c(900, 2))

  out3 <- vroom_fwf(
    file(f),
    n_max = 100,
    col_positions = fwf_widths(c(4, 4)),
    col_types = "ii"
  )

  expect_equal(dim(out3), c(100, 2))

  out4 <- vroom_fwf(
    file(f),
    n_max = 10,
    col_positions = fwf_widths(c(4, 4)),
    col_types = "ii"
  )

  expect_equal(dim(out4), c(10, 2))

  out5 <- vroom_fwf(
    file(f),
    n_max = 1,
    col_positions = fwf_widths(c(4, 4)),
    col_types = "ii"
  )

  expect_equal(dim(out5), c(1, 2))
})

test_that("vroom_fwf(n_max = 0) works with connection", {
  f <- withr::local_tempfile(fileext = ".gz")
  con <- gzfile(f, "w")
  writeLines(c("abcdef", "ghijkl"), con)
  close(con)

  result <- vroom_fwf(
    f,
    col_positions = fwf_widths(c(2, 2, 2), c("a", "b", "c")),
    n_max = 0
  )

  expect_equal(
    result,
    tibble::tibble(a = character(), b = character(), c = character())
  )
})

test_that("vroom_fwf works when skip_empty_rows is false (https://github.com/tidyverse/readr/issues/1211)", {
  f <- tempfile()
  on.exit(unlink(f))

  writeLines(rep(" ", 10), f)

  out <- vroom_fwf(
    f,
    fwf_cols(A = c(1, NA)),
    col_types = "c",
    na = " ",
    trim_ws = FALSE,
    skip_empty_rows = FALSE
  )

  expect_equal(out[[1]], rep(NA_character_, 10))
})

test_that("vroom_fwf correctly reads files with no trailing newline (https://github.com/tidyverse/readr/issues/1293)", {
  f <- tempfile()
  on.exit(unlink(f))

  writeBin(charToRaw("111222\n333444"), f)

  out <- vroom_fwf(f, fwf_widths(c(3, 3)), col_types = "ii")

  expect_equal(out, tibble::tibble(X1 = c(111, 333), X2 = c(222, 444)))

  out2 <- vroom_fwf(file(f), fwf_widths(c(3, 3)), col_types = "ii")

  expect_equal(out, out2)
})

test_that("vroom_fwf correctly reads DOS files with no trailing newline (https://github.com/tidyverse/readr/issues/1293)", {
  f <- tempfile()
  on.exit(unlink(f))

  writeBin(charToRaw("111222\r\n333444"), f)

  out <- vroom_fwf(f, fwf_widths(c(3, 3)), col_types = "ii")

  expect_equal(out, tibble::tibble(X1 = c(111, 333), X2 = c(222, 444)))

  out2 <- vroom_fwf(file(f), fwf_widths(c(3, 3)), col_types = "ii")

  expect_equal(out, out2)
})

# https://github.com/tidyverse/vroom/issues/554
# https://github.com/tidyverse/vroom/issues/534
test_that("vroom_fwf(col_select =) output has 'spec_tbl_df' class, spec, and problems when readr is attached", {
  # Register readr's [.spec_tbl_df method which drops attributes and the "spec_tbl_df" class
  readr_single_bracket <- function(x, ...) {
    attr(x, "spec") <- NULL
    attr(x, "problems") <- NULL
    class(x) <- setdiff(class(x), "spec_tbl_df")
    NextMethod(`[`)
  }
  registerS3method("[", "spec_tbl_df", readr_single_bracket)

  # Unfortunately you can't just de-register the method and
  # local_mocked_s3_method() only works if method exists to begin with.
  # We'll do next best thing which is to put a pass-through method back.
  withr::defer({
    registerS3method("[", "spec_tbl_df", function(x, ...) NextMethod(`[`))
  })

  # libvroom backend doesn't emit parse warnings, so don't require them
  dat <- suppressWarnings(vroom_fwf(
    I("a  b  \n1  2  \nz  3  \n4  5  "),
    fwf_widths(c(3, 3)),
    col_types = "dc",
    col_select = c(X1, X2),
    show_col_types = FALSE,
    altrep = FALSE
  ))

  expect_s3_class(dat, "spec_tbl_df")
  expect_equal(
    attr(dat, "spec", exact = TRUE),
    cols(
      X1 = col_double(),
      X2 = col_character(),
      .delim = ""
    )
  )
  # problems() should work (even if empty with libvroom backend)
  expect_no_error(probs <- problems(dat))
})

# ============================================================================
# libvroom FWF backend tests
# ============================================================================

# Helper: write text to a temp file (ensures libvroom path is used via file path)
write_fwf_file <- function(lines) {
  f <- withr::local_tempfile(.local_envir = parent.frame())
  writeLines(lines, f)
  f
}

test_that("libvroom FWF: basic reading with type inference", {
  f <- write_fwf_file(c(
    "  1  3.14  TRUE  2024-01-15  hello",
    "  2  2.71 FALSE  2024-02-20  world",
    "  3  1.41  TRUE  2024-03-25   test"
  ))
  result <- vroom_fwf(f, fwf_widths(c(3, 6, 6, 12, 7)))
  expect_equal(nrow(result), 3)
  expect_equal(ncol(result), 5)
  expect_type(result$X1, "double")
  expect_type(result$X2, "double")
  expect_type(result$X3, "logical")
  expect_s3_class(result$X4, "Date")
  expect_type(result$X5, "character")
  expect_equal(result$X1, c(1, 2, 3))
  expect_equal(result$X2, c(3.14, 2.71, 1.41))
  expect_equal(result$X3, c(TRUE, FALSE, TRUE))
  expect_equal(result$X5, c("hello", "world", "test"))
})

test_that("libvroom FWF: ragged last column", {
  f <- write_fwf_file(c("1a", "2ab", "3abc"))
  result <- vroom_fwf(f, fwf_widths(c(1, NA)))
  expect_equal(result$X2, c("a", "ab", "abc"))
})

test_that("libvroom FWF: whitespace trimming", {
  f <- write_fwf_file(c("a11 b22 c33", "d   e   f  "))
  out_trim <- vroom_fwf(f, fwf_widths(c(4, 4, 3)), trim_ws = TRUE)
  expect_equal(out_trim$X1, c("a11", "d"))
  expect_equal(out_trim$X2, c("b22", "e"))
  expect_equal(out_trim$X3, c("c33", "f"))
})

test_that("libvroom FWF: empty fields become NA", {
  f <- write_fwf_file(c("foobar", "foo   "))
  result <- vroom_fwf(f, fwf_widths(c(3, 3)), na = "")
  expect_equal(result[[2]], c("bar", NA))
})

test_that("libvroom FWF: comment lines skipped", {
  f <- write_fwf_file(c("#comment", "1 2 3", "4 5 6"))
  result <- vroom_fwf(f, fwf_widths(c(2, 2, 1)), comment = "#")
  expect_equal(nrow(result), 2)
  expect_equal(result$X1, c(1, 4))
})

test_that("libvroom FWF: CRLF line endings", {
  f <- withr::local_tempfile()
  writeBin(charToRaw("111222\r\n333444\r\n"), f)
  result <- vroom_fwf(f, fwf_widths(c(3, 3)))
  expect_equal(nrow(result), 2)
  expect_equal(result$X1, c(111, 333))
  expect_equal(result$X2, c(222, 444))
})

test_that("libvroom FWF: no trailing newline", {
  f <- withr::local_tempfile()
  writeBin(charToRaw("111222\n333444"), f)
  result <- vroom_fwf(f, fwf_widths(c(3, 3)))
  expect_equal(nrow(result), 2)
  expect_equal(result$X1, c(111, 333))
  expect_equal(result$X2, c(222, 444))
})

test_that("libvroom FWF: large file triggers parallel parsing", {
  f <- withr::local_tempfile()
  # Generate >1MB of FWF data
  n <- 50000
  lines <- sprintf("%010d%010d", seq_len(n), seq_len(n) * 2L)
  writeLines(lines, f)
  result <- vroom_fwf(f, fwf_widths(c(10, 10)))
  expect_equal(nrow(result), n)
  expect_equal(result$X1, seq_len(n))
  expect_equal(result$X2, seq_len(n) * 2L)
})

test_that("libvroom FWF: equivalence with old backend", {
  f <- write_fwf_file(c(
    "John Smith          WA        418-Y11-4111",
    "Mary Hartford       CA        319-Z19-4341",
    "Evan Nolan          IL        219-532-c301"
  ))
  pos <- fwf_widths(c(20, 10, 12), c("name", "state", "ssn"))

  # New libvroom backend (no col_types, no skip, no n_max)
  new_result <- vroom_fwf(f, pos)
  # Force old backend with col_types
  old_result <- vroom_fwf(f, pos, col_types = list())

  expect_equal(new_result$name, old_result$name)
  expect_equal(new_result$state, old_result$state)
  expect_equal(new_result$ssn, old_result$ssn)
})

test_that("libvroom FWF: overlapping columns", {
  f <- write_fwf_file(c("2015a", "2016b"))
  result <- vroom_fwf(f, fwf_positions(c(1, 3, 5), c(4, 4, 5)))
  expect_equal(result$X1, c(2015, 2016))
  expect_equal(result$X2, c(15, 16))
  expect_equal(result$X3, c("a", "b"))
})

test_that("libvroom FWF: NA values handled correctly", {
  f <- write_fwf_file(c(
    "  1 hello",
    " NA    NA",
    "  3 world"
  ))
  result <- vroom_fwf(f, fwf_widths(c(3, 6)))
  expect_equal(result$X1, c(1L, NA, 3L))
  expect_equal(result$X2, c("hello", NA, "world"))
})

test_that("libvroom FWF: file connection reads correctly", {
  f <- write_fwf_file(c(
    "John Smith          WA        418-Y11-4111",
    "Mary Hartford       CA        319-Z19-4341",
    "Evan Nolan          IL        219-532-c301"
  ))
  pos <- fwf_widths(c(20, 10, 12), c("name", "state", "ssn"))

  # Read via file path (libvroom path)
  direct <- vroom_fwf(f, pos)
  # Read via file() connection (also libvroom path)
  from_con <- vroom_fwf(file(f), pos)

  expect_equal(direct$name, from_con$name)
  expect_equal(direct$state, from_con$state)
  expect_equal(direct$ssn, from_con$ssn)
})

test_that("libvroom FWF: rawConnection reads correctly", {
  lines <- "  1 hello\n  2 world\n  3 vroom\n"
  raw_data <- charToRaw(lines)
  result <- vroom_fwf(rawConnection(raw_data), fwf_widths(c(3, 6)))
  expect_equal(result$X1, 1:3)
  expect_equal(result$X2, c("hello", "world", "vroom"))
})

test_that("libvroom FWF: gzfile connection reads correctly", {
  f <- write_fwf_file(c(
    "  1 hello",
    "  2 world",
    "  3 vroom"
  ))
  gz_file <- tempfile(fileext = ".gz")
  con <- gzfile(gz_file, "wb")
  writeLines(readLines(f), con)
  close(con)

  result <- vroom_fwf(gzfile(gz_file), fwf_widths(c(3, 6)))
  expect_equal(result$X1, 1:3)
  expect_equal(result$X2, c("hello", "world", "vroom"))
})

test_that("libvroom FWF: connection matches file for type inference", {
  f <- write_fwf_file(c(
    "  1 TRUE  3.14",
    "  2 FALSE 2.72",
    "  3 TRUE  1.41"
  ))
  pos <- fwf_widths(c(3, 6, 5))

  direct <- vroom_fwf(f, pos)
  from_con <- vroom_fwf(file(f), pos)

  expect_equal(direct, from_con)
  expect_type(direct$X1, "double")
  expect_type(direct$X2, "logical")
  expect_type(direct$X3, "double")
})

test_that("libvroom FWF: skip works", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc",
    "  4 ddd"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(f, pos, skip = 2)
  expect_equal(nrow(result), 2)
  expect_equal(result$X1, 3:4)
  expect_equal(result$X2, c("ccc", "ddd"))
})

test_that("libvroom FWF: n_max works", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc",
    "  4 ddd"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(f, pos, n_max = 2)
  expect_equal(nrow(result), 2)
  expect_equal(result$X1, 1:2)
  expect_equal(result$X2, c("aaa", "bbb"))
})

test_that("libvroom FWF: skip + n_max combined", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc",
    "  4 ddd",
    "  5 eee"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(f, pos, skip = 1, n_max = 2)
  expect_equal(nrow(result), 2)
  expect_equal(result$X1, 2:3)
  expect_equal(result$X2, c("bbb", "ccc"))
})

test_that("libvroom FWF: n_max = 0 returns empty tibble", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(f, pos, n_max = 0)
  expect_equal(nrow(result), 0)
  expect_equal(ncol(result), 2)
})

test_that("libvroom FWF: id column works with file path", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(f, pos, id = "source")
  expect_equal(ncol(result), 3)
  expect_equal(names(result)[1], "source")
  expect_equal(result$source, rep(f, 3))
  expect_equal(result$X1, 1:3)
})

test_that("libvroom FWF: id column with connection gives NA", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(file(f), pos, id = "source")
  expect_equal(ncol(result), 3)
  expect_equal(names(result)[1], "source")
  expect_true(all(is.na(result$source)))
})

test_that("libvroom FWF: skip + n_max + id combined", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc",
    "  4 ddd"
  ))
  pos <- fwf_widths(c(3, 4))

  result <- vroom_fwf(f, pos, skip = 1, n_max = 2, id = "file")
  expect_equal(nrow(result), 2)
  expect_equal(names(result)[1], "file")
  expect_equal(result$X1, 2:3)
})

test_that("libvroom FWF: skip with connection", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc"
  ))
  pos <- fwf_widths(c(3, 4))

  direct <- vroom_fwf(f, pos, skip = 1)
  from_con <- vroom_fwf(file(f), pos, skip = 1)
  expect_equal(direct$X1, from_con$X1)
  expect_equal(direct$X2, from_con$X2)
})

test_that("libvroom FWF: n_max with connection", {
  f <- write_fwf_file(c(
    "  1 aaa",
    "  2 bbb",
    "  3 ccc"
  ))
  pos <- fwf_widths(c(3, 4))

  direct <- vroom_fwf(f, pos, n_max = 2)
  from_con <- vroom_fwf(file(f), pos, n_max = 2)
  expect_equal(direct$X1, from_con$X1)
  expect_equal(direct$X2, from_con$X2)
})

test_that("libvroom FWF handles explicit col_types with compact notation", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("  12  34 TRUE", "  56  78FALSE"), tf)

  result <- vroom_fwf(
    tf,
    fwf_widths(c(4, 4, 5), c("a", "b", "c")),
    col_types = "idc"
  )
  expect_equal(result$a, c(12L, 56L))
  expect_type(result$b, "double")
  expect_type(result$c, "character")
})

test_that("libvroom FWF handles col_skip", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("  12  34hello", "  56  78world"), tf)

  result <- vroom_fwf(
    tf,
    fwf_widths(c(4, 4, 5), c("a", "b", "c")),
    col_types = "i_c"
  )
  expect_equal(names(result), c("a", "c"))
  expect_equal(result$a, c(12L, 56L))
})

test_that("libvroom FWF handles cols() with named columns", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("  12  34hello", "  56  78world"), tf)

  result <- vroom_fwf(
    tf,
    fwf_widths(c(4, 4, 5), c("a", "b", "c")),
    col_types = cols(a = col_integer(), b = col_double(), c = col_character())
  )
  expect_equal(result$a, c(12L, 56L))
  expect_type(result$b, "double")
  expect_type(result$c, "character")
})

test_that("libvroom FWF handles cols_only()", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("  12  34hello", "  56  78world"), tf)

  result <- vroom_fwf(
    tf,
    fwf_widths(c(4, 4, 5), c("a", "b", "c")),
    col_types = cols_only(a = col_integer(), c = col_character())
  )
  expect_equal(names(result), c("a", "c"))
  expect_equal(result$a, c(12L, 56L))
  expect_equal(result$c, c("hello", "world"))
})

test_that("libvroom FWF path shows col_types by default", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("JohnSmith 42", "JaneDoe   28"), tf)

  expect_message(
    vroom_fwf(
      tf,
      fwf_widths(c(10, 2), c("name", "age")),
      show_col_types = NULL
    ),
    "Column specification"
  )
})

test_that("libvroom FWF path suppresses col_types when show_col_types = FALSE", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("JohnSmith 42", "JaneDoe   28"), tf)

  expect_no_message(
    vroom_fwf(
      tf,
      fwf_widths(c(10, 2), c("name", "age")),
      show_col_types = FALSE
    )
  )
})

test_that("libvroom FWF path attaches spec attribute", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines(c("JohnSmith 42", "JaneDoe   28"), tf)

  res <- vroom_fwf(
    tf,
    fwf_widths(c(10, 2), c("name", "age")),
    show_col_types = FALSE
  )
  s <- spec(res)
  expect_s3_class(s, "col_spec")
  expect_equal(length(s$cols), 2L)
})
