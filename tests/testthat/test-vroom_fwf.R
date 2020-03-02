context("vroom_fwf")

test_that("trailing spaces omitted", {
  spec <- fwf_empty(test_path("fwf-trailing.txt"))
  expect_equal(spec$begin, c(0, 4))
  expect_equal(spec$end, c(3, NA))

  df <- vroom_fwf(test_path("fwf-trailing.txt"), spec)
  expect_equal(df$X1, df$X2)
})

test_that("dos newlines handles", {
  spec <- fwf_empty(test_path("fwf-trailing.txt"))
  x <- vroom_fwf(test_path("fwf-trailing.txt"), spec)
  y <- vroom_fwf(test_path("fwf-trailing-crlf.txt"), spec)
  expect_equal(x, y)

  z <- vroom_fwf(file(test_path("fwf-trailing-crlf.txt")), spec)
  expect_equal(x, z)
})

test_that("connections and normal files produce identical output", {
  spec <- fwf_empty(test_path("fwf-trailing.txt"))

  y <- vroom_fwf(test_path("fwf-trailing-crlf.txt"), spec)
  x <- vroom_fwf(file(test_path("fwf-trailing-crlf.txt")), spec)

  expect_equal(x, y)
})

test_that("respects the trim_ws argument", {
  x <- "a11 b22 c33\nd   e   f  "
  out1 <- vroom_fwf(x, fwf_empty(x), trim_ws = FALSE)
  expect_equal(out1$X1, c("a11", "d  "))
  expect_equal(out1$X2, c("b22", "e  "))
  expect_equal(out1$X3, c("c33", "f  "))

  out2 <- vroom_fwf(x, fwf_empty(x), trim_ws = TRUE)
  expect_equal(out2$X1, c("a11", "d"))
  expect_equal(out2$X2, c("b22", "e"))
  expect_equal(out2$X3, c("c33", "f"))
})

test_that("respects the trim_ws argument with empty fields", {
  x <- "a11 b22 c33\nd       f  "
  out1 <- vroom_fwf(x, fwf_empty(x), trim_ws = FALSE)
  expect_equal(out1$X1, c("a11", "d  "))
  expect_equal(out1$X2, c("b22", "   "))
  expect_equal(out1$X3, c("c33", "f  "))

  out1 <- vroom_fwf(x, fwf_empty(x), trim_ws = TRUE, na = "NA")
})

test_that("skipping column doesn't pad col_names", {
  x <- "1 2 3\n4 5 6"

  out1 <- vroom_fwf(x, fwf_empty(x), col_types = 'd-d')
  expect_named(out1, c("X1", "X3"))

  names <- c("a", "b", "c")
  out2 <- vroom_fwf(x, fwf_empty(x, col_names = names), col_types = 'd-d')
  expect_named(out2, c("a", "c"))
})

test_that("fwf_empty can skip comments", {
  x <- "COMMENT\n1 2 3\n4 5 6"

  out1 <- vroom_fwf(x, fwf_empty(x, comment = "COMMENT"), comment = "COMMENT")
  expect_equal(dim(out1), c(2, 3))
})

test_that("fwf_empty can skip lines", {
  x <- "foo\nbar\baz\n1 2 3\n4 5 6\n"

  obj <- fwf_empty(x, skip = 3)

  exp <- list(begin = c(0L, 2L, 4L), end = c(1L, 3L, NA_integer_), col_names = c("X1", "X2", "X3"))

  expect_equal(obj, exp)
})

test_that("passing \"\" to vroom_fwf's 'na' option", {
  expect_equal(vroom_fwf('foobar\nfoo   ', fwf_widths(c(3, 3)), na = "")[[2]],
               c("bar", NA))
})

test_that("ragged last column expanded with NA", {
  x <- vroom_fwf("1a\n2ab\n3abc", fwf_widths(c(1, NA)))
  expect_equal(x$X2, c("a", "ab", "abc"))
})

#test_that("ragged last column shrunk with warning", {
  #expect_warning(x <- vroom_fwf("1a\n2ab\n3abc", fwf_widths(c(1, 3))))
  #expect_equal(x$X2, c("a", "ab", "abc"))
#})

test_that("read all columns with positions, non ragged", {
  col_pos <- fwf_positions(c(1,3,6),c(2,5,6))
  x <- vroom_fwf('12345A\n67890BBBBBBBBB\n54321C',col_positions = col_pos)
  expect_equal(x$X3, c("A", "B", "C"))
})

test_that("read subset columns with positions", {
  col_pos <- fwf_positions(c(1,3),c(2,5))
  x <- vroom_fwf('12345A\n67890BBBBBBBBB\n54321C',col_positions = col_pos)
  expect_equal(x$X1, c(12, 67, 54))
  expect_equal(x$X2, c(345, 890, 321))
})

test_that("read columns with positions, ragged", {
  col_pos <- fwf_positions(c(1,3,6),c(2,5,NA))
  x <- vroom_fwf('12345A\n67890BBBBBBBBB\n54321C',col_positions = col_pos)
  expect_equal(x$X1, c(12, 67, 54))
  expect_equal(x$X2, c(345, 890, 321))
  expect_equal(x$X3, c('A', 'BBBBBBBBB', 'C'))
})

test_that("read columns with width, ragged", {
  col_pos <- fwf_widths(c(2,3,NA))
  x <- vroom_fwf('12345A\n67890BBBBBBBBB\n54321C',col_positions = col_pos)
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
    x <- vroom_fwf("2015a\n2016b", fwf_positions(c(1, 3, 5), c(4, 4, 5)))
    expect_equal(x$X1, c(2015, 2016))
    expect_equal(x$X2, c(15, 16))
    expect_equal(x$X3, c("a", "b"))
})

# fwf_cols
test_that("fwf_cols produces correct fwf_positions object with elements of length 2", {
  expected <- fwf_positions(c(1L, 9L, 4L), c(2L, 12L, 6L), c("a", "b", "d"))
  expect_equivalent(fwf_cols(a = c(1, 2), b = c(9, 12), d = c(4, 6)), expected)
})

test_that("fwf_cols produces correct fwf_positions object with elements of length 1", {
  expected <- fwf_widths(c(2L, 4L, 3L), c("a", "b", "c"))
  expect_equivalent(fwf_cols(a = 2, b = 4, c = 3), expected)
})


test_that("fwf_cols throws error when arguments are not length 1 or 2", {
  expect_error(fwf_cols(a = 1:3, b = 4:5))
  expect_error(fwf_cols(a = c(), b = 4:5))
})

test_that("fwf_cols works with unnamed columns", {
  expect_equivalent(
    fwf_cols(c(1, 2), c(9, 12), c(4, 6)),
    fwf_positions(c(1L, 9L, 4L), c(2L, 12L, 6L), c("X1", "X2", "X3"))
  )
  expect_equivalent(
    fwf_cols(a = c(1, 2), c(9, 12), c(4, 6)),
    fwf_positions(c(1L, 9L, 4L), c(2L, 12L, 6L), c("a", "X2", "X3"))
  )
})

# fwf_positions ---------------------------------------------------------------

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

# Robustness

test_that("vroom_fwf() is robust to improper inputs", {
  expect_error_free(
    vroom_fwf("foo bar baz\n1   2\n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2 \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2  \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4\n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4 \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4  \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4   \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4   5\n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4   5 \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4   5  \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4   5   \n")
  )

  expect_error_free(
    vroom_fwf("foo bar baz\n1   2   \n4   5   6\n")
  )

  expect_error_free(
    vroom_fwf("A\n  a\n")
  )
})

test_that("Errors if begin is greater than end", {
  positions <- fwf_positions(
    start = c(1, 3, 5),
    end = c(3, 1, NA),
    col_names = c("foo", "bar", "baz")
  )

  expect_error(
    vroom_fwf("1  2  3\n", positions),
    "`col_positions` must have begin less than end"
  )
})
