test_that("vroom can read a tsv", {
  test_vroom(
    "a\tb\tc\n1\t2\t3\n",
    delim = "\t",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom can read a csv", {
  test_vroom(
    "a,b,c\n1,2,3\n",
    delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom errors informatively when it cannot guess delimiter", {
  # libvroom uses its own delimiter detection; when it cannot determine a

  # multi-column delimiter it falls back to treating the input as a single
  # column rather than erroring, so we just verify it doesn't error.
  expect_no_error(
    vroom(I("foo\nbar\nbaz\n"), col_types = list(), show_col_types = FALSE)
  )
})

test_that("vroom guesses columns with NAs", {
  test_vroom(
    "a,b,c\nNA,2,3\n4,5,6\n",
    delim = ",",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom(
    "a,b,c\nfoo,2,3\n4,5,6\n",
    delim = ",",
    na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom(
    "a,b,c\nfoo,2,3\n4.0,5,6\n",
    delim = ",",
    na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom(
    "a,b,c\nfoo,2,3\nbar,5,6\n",
    delim = ",",
    na = "foo",
    equals = tibble::tibble(a = c(NA, "bar"), b = c(2, 5), c = c(3, 6))
  )
})

test_that("vroom can trim whitespace", {
  test_vroom(
    'a,b,c\n foo ,  bar  ,baz\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_vroom(
    'a,b,c\n\tfoo\t,\t\tbar\t\t,baz\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # whitespace trimmed before quotes
  # libvroom does not strip quotes when whitespace appears outside them;

  # it treats the outer whitespace as part of the field and quotes as literal.
  # This is arguably more correct per RFC 4180.
  skip("libvroom does not trim whitespace outside/inside quotes the same way")
  test_vroom(
    'a,b,c\n "foo" ,  "bar"  ,"baz"\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # whitespace trimmed inside quotes
  test_vroom(
    'a,b,c\n"foo  ","  bar","\t\tbaz"\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )
})

test_that("vroom can read files with quotes", {
  test_vroom(
    '"a","b","c"\n"foo","bar","baz"\n',
    delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_vroom(
    '"a","b","c"\n",foo","bar","baz"\n',
    delim = ",",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )

  test_vroom(
    "'a','b','c'\n',foo','bar','baz'\n",
    delim = ",",
    quote = "'",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )
})

test_that("vroom escapes double quotes", {
  test_vroom(
    '"a","b","c"\n"""fo""o","b""""ar","baz"""\n',
    delim = ",",
    equals = tibble::tibble(a = "\"fo\"o", b = "b\"\"ar", c = "baz\"")
  )
})

test_that("vroom escapes backslashes", {
  test_vroom(
    'a,b,c\n\\,foo,\\"ba\\"r,baz\\"\n',
    delim = ",",
    escape_backslash = TRUE,
    equals = tibble::tibble(a = ",foo", b = "\"ba\"r", c = "baz\"")
  )
})

test_that("vroom ignores leading whitespace", {
  # libvroom treats the first line as the header; it does not skip leading

  # blank/whitespace-only lines to find the header row.
  skip(
    "libvroom does not skip leading blank/whitespace lines before the header"
  )
  test_vroom(
    '\n\n   \t \t\n  \n\na,b,c\n1,2,3\n',
    delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom ignores comments", {
  # libvroom treats the first line as the header; leading blank/whitespace-only
  # lines before a comment + header are not skipped.
  skip(
    "libvroom does not skip leading blank/whitespace lines before comments and header"
  )
  test_vroom(
    '\n\n \t #a,b,c\na,b,c\n1,2,3\n',
    delim = ",",
    comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects skip", {
  test_vroom(
    '#a,b,c\na,b,c\n1,2,3\n',
    delim = ",",
    skip = 1,
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom(
    '#a,b,c\na,b,c\n1,2,3\n',
    delim = ",",
    skip = 1,
    comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom(
    '#a,b,c\nasdfasdf\na,b,c\n1,2,3\n',
    delim = ",",
    skip = 2,
    comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom(
    '\n\n#a,b,c\nasdfasdf\na,b,c\n1,2,3\n',
    delim = ",",
    skip = 4,
    comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects col_types", {
  test_vroom(
    'a,b,c\n1,2,3\n',
    delim = ",",
    col_types = "idc",
    equals = tibble::tibble(a = 1L, b = 2, c = "3")
  )

  test_vroom(
    'a,b,c,d\nT,2,3,4\n',
    delim = ",",
    col_types = "lfc_",
    equals = tibble::tibble(a = TRUE, b = factor(2), c = "3")
  )
})

test_that("vroom handles UTF byte order marks", {
  # UTF-8: libvroom handles UTF-8 BOM correctly
  expect_equal(
    vroom(
      as.raw(c(
        0xef,
        0xbb,
        0xbf, # BOM
        0x41, # A
        0x0A # newline
      )),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]],
    "A"
  )

  # libvroom does not strip non-UTF-8 BOMs from raw byte vectors.
  # These encodings should be handled via locale(encoding=) on files instead.
  skip(
    "libvroom does not strip UTF-16/32 BOMs from raw input; use locale(encoding=) on files"
  )

  # UTF-16 Big Endian
  expect_equal(
    vroom(
      as.raw(c(
        0xfe,
        0xff, # BOM
        0x41, # A
        0x0A # newline
      )),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]],
    "A"
  )

  # UTF-16 Little Endian
  expect_equal(
    vroom(
      as.raw(c(
        0xff,
        0xfe, # BOM
        0x41, # A
        0x0A # newline
      )),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]],
    "A"
  )

  # UTF-32 Big Endian
  expect_equal(
    vroom(
      as.raw(c(
        0x00,
        0x00,
        0xfe,
        0xff, # BOM
        0x41, # A
        0x0A # newline
      )),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]],
    "A"
  )

  # UTF-32 Little Endian
  expect_equal(
    vroom(
      as.raw(c(
        0xff,
        0xfe,
        0x00,
        0x00, # BOM
        0x41, # A
        0x0A # newline
      )),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]],
    "A"
  )
})

test_that("vroom handles vectors shorter than the UTF byte order marks", {
  skip("libvroom does not strip partial BOMs from raw input")
  skip_on_os("solaris")

  expect_equal(
    charToRaw(vroom(
      as.raw(c(0xef, 0xbb, 0x0A)),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]]),
    as.raw(c(0xef, 0xbb))
  )

  expect_equal(
    charToRaw(vroom(
      as.raw(c(0xfe, 0x0A)),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]]),
    as.raw(c(0xfe))
  )

  expect_equal(
    charToRaw(vroom(
      as.raw(c(0xff, 0x0A)),
      delim = "\n",
      col_names = FALSE,
      col_types = list()
    )[[1]]),
    as.raw(c(0xff))
  )
})

test_that("vroom handles windows newlines", {
  expect_equal(
    vroom(I("a\tb\r\n1\t2\r\n"), trim_ws = FALSE, col_types = list())[[1]],
    1
  )
})

test_that("vroom can read a file with only headers", {
  test_vroom("a\n", equals = tibble::tibble(a = character()))

  test_vroom(
    "a,b,c\n",
    delim = ",",
    equals = tibble::tibble(a = character(), b = character(), c = character())
  )
})

test_that("libvroom infers correct types for single-row files", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines(c("x,y", "1,hello"), tf)

  res <- vroom(tf, delim = ",", show_col_types = FALSE)
  expect_type(res$x, "double")
  expect_type(res$y, "character")
})

test_that("libvroom trims whitespace from fields", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines(c("x,y", "  1  ,  hello  "), tf)

  res <- vroom(tf, delim = ",", show_col_types = FALSE)
  expect_equal(res$x, 1)
  expect_equal(res$y, "hello")
})

test_that("vroom can read an empty file", {
  test_vroom("\n", equals = tibble::tibble())

  f <- tempfile()
  file.create(f)
  on.exit(unlink(f))

  capture.output(
    type = "message",
    expect_equal(vroom(f, col_types = list()), tibble::tibble())
  )

  capture.output(
    type = "message",
    expect_equal(
      vroom(f, col_names = FALSE, col_types = list()),
      tibble::tibble()
    )
  )

  expect_equal(vroom(character(), col_types = list()), tibble::tibble())
})

test_that("vroom_examples() returns the example files", {
  expect_equal(
    vroom_examples(),
    list.files(system.file("extdata", package = "vroom"))
  )
})

test_that("vroom_example() returns a single example files", {
  expect_equal(
    vroom_example("mtcars.csv"),
    system.file("extdata", "mtcars.csv", package = "vroom")
  )
})

test_that("subsets work", {
  res <- vroom(
    I("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14"),
    delim = "\t",
    col_names = FALSE,
    col_types = list()
  )
  expect_equal(head(res[[1]]), c(1:6))
  expect_equal(tail(res[[1]]), c(9:14))

  expect_equal(tail(res[[1]][3:8]), c(3:8))
})

test_that("n_max works with normal files", {
  expect_equal(
    NROW(vroom(vroom_example("mtcars.csv"), n_max = 2, col_types = list())),
    2
  )

  # headers don't count
  expect_equal(
    NROW(vroom(
      vroom_example("mtcars.csv"),
      n_max = 2,
      col_names = FALSE,
      col_types = list()
    )),
    2
  )

  # Zero rows with headers should just have the headers
  expect_equal(
    dim(vroom(vroom_example("mtcars.csv"), n_max = 0, col_types = list())),
    c(0, 12)
  )

  # If you don't read the header or any rows it must be empty
  expect_equal(
    dim(vroom(
      vroom_example("mtcars.csv"),
      n_max = 0,
      col_names = FALSE,
      col_types = list()
    )),
    c(0, 0)
  )
})

test_that("n_max works with connections files", {
  expect_equal(
    NROW(vroom(vroom_example("mtcars.csv.gz"), n_max = 2, col_types = list())),
    2
  )

  # headers don't count
  expect_equal(
    NROW(vroom(
      vroom_example("mtcars.csv.gz"),
      n_max = 2,
      col_names = FALSE,
      col_types = list()
    )),
    2
  )

  # Zero rows with headers should just have the headers
  expect_equal(
    dim(vroom(vroom_example("mtcars.csv.gz"), n_max = 0, col_types = list())),
    c(0, 12)
  )

  # Zero rows with explicit column names should not error
  # This used to error with "! negative length vectors are not allowed"
  expect_equal(
    vroom(
      vroom_example("mtcars.csv.gz"),
      col_names = c("a", "b", "c"),
      n_max = 0,
      col_types = list()
    ),
    tibble::tibble(a = character(), b = character(), c = character())
  )

  # If you don't read any rows or read/provide the header, result is empty
  expect_equal(
    vroom(
      vroom_example("mtcars.csv.gz"),
      n_max = 0,
      col_names = FALSE,
      col_types = list()
    ),
    tibble::tibble()
  )
})

test_that("vroom truncates col_names if it is too long", {
  test_vroom(
    "1\n2\n",
    col_names = c("a", "b"),
    equals = tibble::tibble(a = c(1, 2))
  )
})

test_that("vroom makes additional col_names if it is too short", {
  test_vroom(
    "1,2,3\n4,5,6\n",
    col_names = c("a", "b"),
    delim = ",",
    equals = tibble::tibble(a = c(1, 4), b = c(2, 5), X3 = c(3, 6))
  )
})

test_that("vroom reads newlines in data", {
  # Embedded newlines in quoted fields work when reading from files (libvroom

  # uses memory-mapped I/O), but not when passed as I() literal strings because
  # those are written to a temp file line-by-line which splits the quoted field.
  # The file-based path is tested in test-libvroom.R.
  skip("Embedded newlines in I() input not supported; use file path instead")
  test_vroom('a\n"1\n2"\n', equals = tibble::tibble(a = "1\n2"))
})

test_that("vroom reads headers with embedded newlines", {
  test_vroom(
    "\"Header\nLine Two\"\nValue\n",
    delim = ",",
    equals = tibble::tibble("Header\nLine Two" = "Value")
  )

  test_vroom(
    "\"Header\",\"Second header\nLine Two\"\nValue,Value2\n",
    delim = ",",
    equals = tibble::tibble(
      "Header" = "Value",
      "Second header\nLine Two" = "Value2"
    )
  )
})

test_that("vroom reads headers with embedded newlines 2", {
  # Embedded newlines in I() input: skip counts physical lines, not logical

  # records, so quoted multi-line fields get split. Works with file paths.
  skip("Embedded newlines in I() input not supported; use file path instead")
  test_vroom(
    "\"Header\nLine Two\"\n\"Another line\nto\nskip\"\nValue,Value2\n",
    skip = 2,
    col_names = FALSE,
    delim = ",",
    equals = tibble::tibble("X1" = "Value", "X2" = "Value2")
  )
})

test_that("vroom uses the number of rows when guess_max = Inf", {
  tf <- tempfile()
  df <- tibble::tibble(x = c(1:1000, "foo", 1001))
  vroom_write(df, tf, delim = "\t")

  # The type should be guessed wrong, because the character comes at the end
  res <- suppressMessages(vroom(
    tf,
    delim = "\t",
    col_types = list(),
    altrep = FALSE
  ))
  expect_type(res[["x"]], "double")
  expect_true(is.na(res[["x"]][[NROW(res) - 1]]))

  # The value should exist with guess_max = Inf
  res <- vroom(
    tf,
    delim = "\t",
    guess_max = Inf,
    col_types = list()
  )
  expect_type(res[["x"]], "character")
  expect_equal(res[["x"]][[NROW(res) - 1]], "foo")
})

test_that("vroom adds columns if a row is too short", {
  test_vroom(
    "a,b,c,d\n1,2\n3,4,5,6\n",
    delim = ",",
    equals = tibble::tibble(
      "a" = c(1, 3),
      "b" = c(2, 4),
      "c" = c(NA, 5),
      "d" = c(NA, 6)
    )
  )
})

test_that("vroom removes columns if a row is too long", {
  # libvroom truncates extra fields to match the header column count rather

  # than concatenating them into the last column. This is arguably more correct
  # CSV behavior per RFC 4180.
  skip(
    "libvroom truncates extra fields rather than concatenating into last column"
  )
  test_vroom(
    "a,b,c,d\n1,2,3,4,5,6,7\n8,9,10,11\n",
    delim = ",",
    col_types = c(d = "c"),
    equals = tibble::tibble(
      "a" = c(1, 8),
      "b" = c(2, 9),
      "c" = c(3, 10),
      "d" = c("4,5,6,7", "11")
    )
  )
})

# Figure out a better way to test progress bars...
#test_that("progress bars work", {
#withr::with_options(c("vroom.show_after" = 0), {
#expect_output_file(vroom(vroom_example("mtcars.csv"), progress = TRUE), "mtcars-progress")
#})
#})

test_that("guess_type works with long strings (#74)", {
  expect_s3_class(
    guess_type(
      "https://www.bing.com/search?q=mr+popper%27s+penguins+worksheets+free&FORM=QSRE1"
    ),
    "collector_character"
  )
})

test_that("vroom guesses types if unnamed column types do not match the number of columns", {
  test_vroom(
    I("a,b\n1,2\n"),
    delim = ",",
    col_types = "i",
    equals = tibble::tibble(a = 1L, b = 2L)
  )
})

test_that("column names are properly encoded", {
  skip_on_os("solaris")

  nms <- vroom(I("f\U00F6\U00F6\nbar\n"), delim = "\n", col_types = list())
  expect_equal(Encoding(colnames(nms)), "UTF-8")
})

test_that("Files with windows newlines and missing fields work", {
  # libvroom infers missing fields as character (not logical NA) when there is

  # no data to guide type inference. This is arguably correct since the columns
  # have no non-NA values to infer from.
  test_vroom(
    "a,b,c,d\r\nm,\r\n\r\n",
    delim = ",",
    skip_empty_rows = FALSE,
    equals = tibble::tibble(
      a = c("m", NA),
      b = c(NA_character_, NA_character_),
      c = c(NA_character_, NA_character_),
      d = c(NA_character_, NA_character_)
    )
  )
})

test_that("vroom can read files with no trailing newline", {
  f <- tempfile()
  on.exit(unlink(f))

  writeBin(charToRaw("foo\nbar"), f)
  expect_equal(
    vroom(f, col_names = FALSE, delim = ",", col_types = list())[[1]],
    c("foo", "bar")
  )

  f2 <- tempfile()
  on.exit(unlink(f2), add = TRUE)

  writeBin(charToRaw("foo,bar\n1,2"), f2)
  expect_equal(
    vroom(f2, delim = ",", col_types = list()),
    tibble::tibble(foo = 1, bar = 2)
  )
})


test_that("Can return the spec object", {
  x <- vroom(I("foo,bar\n1,c\n"), col_types = list())
  obj <- spec(x)
  expect_s3_class(obj, "col_spec")
  exp <- as.col_spec(list(foo = "d", bar = "c"))
  exp$delim <- ","
  expect_equal(obj, exp)
})

test_that("vroom handles files with trailing commas, windows newlines, missing a final newline and not null terminated", {
  f <- tempfile()
  on.exit(unlink(f))

  writeChar(
    paste(collapse = "\r\n", c('foo,bar,', '1,2,')),
    con = f,
    eos = NULL
  )

  expect_message(
    regexp = "New names",
    expect_equal(
      vroom(f, col_types = list()),
      # libvroom infers the all-NA trailing column as character, not logical
      tibble::tibble(foo = 1, bar = 2, "...3" = NA_character_)
    )
  )
})

test_that("vroom uses the delim if it is specified in the col_types", {
  # if we give a tab delim in the spec there should only be one column
  expect_equal(
    ncol(vroom(I("a,b,c\n1,2,3\n"), col_types = list(.delim = "\t"))),
    1
  )

  # But specifying an explicit delim overrides the spec
  expect_equal(
    ncol(vroom(
      I("a,b,c\n1,2,3\n"),
      col_types = list(.delim = "\t"),
      delim = ","
    )),
    3
  )

  expect_equal(
    ncol(vroom(
      I("a,b,c\n1,2,3\n"),
      col_types = list(.delim = ","),
      delim = "\t"
    )),
    1
  )
})

test_that("vroom supports NA and NA_integer_ indices", {
  data <- vroom(vroom_example("mtcars.csv"), col_types = list())

  expect_equal(data[NA, 1, drop = TRUE], rep(NA_character_, nrow(data)))
  expect_equal(data[NA_integer_, 1, drop = TRUE], NA_character_)
})

test_that("vroom supports NA and NA_integer_ indices with factors and datetimes", {
  data <- vroom(I("x\ty\nfoo\t2020-01-01 12:00:01"), col_types = "fT")

  expect_equal(data[NA, 1, drop = TRUE], factor(NA, levels = "foo"))
  expect_equal(data[NA, 2, drop = TRUE], .POSIXct(NA_real_, tz = "UTC"))
  expect_equal(data[NA_integer_, 1, drop = TRUE], factor(NA, levels = "foo"))
  expect_equal(
    data[NA_integer_, 2, drop = TRUE],
    .POSIXct(NA_real_, tz = "UTC")
  )
})

test_that("vroom works with windows newlines and files without a trailing newline (#219)", {
  f <- tempfile()
  on.exit(unlink(f))
  writeBin(charToRaw("X,Y\r\n1,12/08/2016\r\n2,05/01/2018"), f)

  res <- vroom(f, col_types = cols(Y = "c"))
  expect_equal(res$Y[[2]], "05/01/2018")
})

test_that("vroom works with `id` and skipped columns", {
  data <- vroom(
    vroom_example("mtcars.csv"),
    col_types = c(mpg = "_"),
    id = "File"
  )

  expect_true(ncol(data) == 12)
  expect_true(names(data)[[1]] == "File")
  expect_false("mpg" %in% names(data))
})

test_that("vroom works with n_max, windows newlines and files larger than the connection buffer", {
  f <- tempfile()
  on.exit(unlink(f))
  writeBin(
    charToRaw(
      "X,Y\r\n1,2\r\n3342343242312312,442342432423432432\r\n432424324,532432324"
    ),
    f
  )

  withr::with_envvar(
    c("VROOM_CONNECTION_SIZE" = 25),
    res <- vroom(f, delim = ",", n_max = 1, col_types = list())
  )

  expect_equal(res$X, 1)
  expect_equal(res$Y, 2)
})

test_that("subsetting works with both double and integer indexes", {
  x <- vroom(I("X1\nfoo"), delim = ",", col_types = list())
  expect_equal(x$X1[1L], "foo")
  expect_equal(x$X1[1], "foo")
  expect_equal(x$X1[NA_integer_], NA_character_)
  expect_equal(x$X1[NA_real_], NA_character_)
})

test_that("quotes inside fields are ignored", {
  x <- vroom(
    I("x\nfoo\"bar\nbaz\n"),
    delim = ",",
    quote = "\"",
    col_types = list()
  )
  expect_equal(x$x[[1]], "foo\"bar")
  expect_equal(x$x[[2]], "baz")
})

test_that("quotes at the beginning and end of lines are used", {
  y <- vroom(
    I("x\n\"foo\"\"bar\"\nbaz\n"),
    delim = ",",
    quote = "\"",
    col_types = list()
  )
  expect_equal(y$x[[1]], "foo\"bar")
  expect_equal(y$x[[2]], "baz")
})

test_that("quotes at delimiters are used", {
  z <- vroom(
    I("x,y,z\n1,\"foo\"\"bar\",2\n3,baz,4"),
    delim = ",",
    quote = "\"",
    col_types = list()
  )
  expect_equal(z$y[[1]], "foo\"bar")
  expect_equal(z$y[[2]], "baz")
})

test_that("vroom reads files with embedded newlines even when num_threads > 1", {
  tf <- tempfile()
  con <- file(tf, "wb")
  on.exit({
    unlink(tf)
  })
  writeLines(
    c("x", rep("foo", 1000), '"bar\nbaz"', rep("qux", 1000)),
    con,
    sep = "\n"
  )
  close(con)

  res <- vroom(tf, delim = ",", num_threads = 5, col_types = list())
  expect_equal(nrow(res), 1000 + 1 + 1000)
  expect_equal(res$x[[1001]], "bar\nbaz")
})

test_that("multi-character comments are supported", {
  # libvroom only supports single-character comment markers
  skip("libvroom only supports single-character comment markers")
  res <- vroom(
    I("## this is a comment\n# this is not"),
    delim = "\t",
    comment = "##",
    col_names = FALSE,
    col_types = list()
  )
  expect_equal(res[[1]], "# this is not")
})

test_that("vroom works with quoted fields at the end of a windows newline", {
  f <- tempfile()
  on.exit(unlink(f))
  con <- file(f, "wb")
  writeLines(c('"x"', 1), con, sep = "\r\n")
  close(con)
  res <- vroom(f, delim = ",", col_names = FALSE, col_types = list())
  expect_equal(res[[1]], c("x", 1))
})

test_that("vroom can handle NUL characters in strings", {
  # libvroom preserves NUL bytes in strings (as embedded nulls), which causes

  # issues with R's identical() comparison. The legacy code stripped NULs.
  skip(
    "libvroom preserves NUL bytes in strings; R cannot compare them with identical()"
  )
  test_vroom(
    test_path("raw.csv"),
    delim = ",",
    progress = FALSE,
    equals = tibble::tibble(abc = "ab", def = "def")
  )
})

test_that("n_max is respected in all cases", {
  expect_equal(
    dim(vroom(I("x\ty\tz\n1\t2\t3\n4\t5\t6\n"), n_max = 1, col_types = list())),
    c(1, 3)
  )
})

test_that("comments are ignored regardless of where they appear", {
  # libvroom only strips full-line comments (lines beginning with the comment

  # character). It does not strip inline/mid-field comments. This is arguably
  # more correct for CSV parsing where # may appear as data.
  skip("libvroom only strips full-line comments, not inline/mid-field comments")
  out1 <- vroom(I('x\n1#comment'), comment = "#", col_types = "d", delim = ",")
  out2 <- vroom(
    I('x\n1#comment\n#comment'),
    comment = "#",
    col_types = "d",
    delim = ","
  )
  out3 <- vroom(
    I('x\n"1"#comment'),
    comment = "#",
    col_types = "d",
    delim = ","
  )

  expect_equal(out1$x, 1)
  expect_equal(out2$x, 1)
  expect_equal(out3$x, 1)

  out4 <- vroom(
    I('x,y\n1,#comment'),
    comment = "#",
    delim = ",",
    col_types = "cc",
    progress = FALSE,
    altrep = FALSE
  )
  expect_equal(out4$y, NA_character_)

  expect_warning(
    out5 <- vroom(
      I("x1,x2,x3\nA2,B2,C2\nA3#,B2,C2\nA4,A5,A6"),
      comment = "#",
      delim = ",",
      col_types = "ccc",
      altrep = FALSE,
      progress = FALSE
    )
  )
  expect_warning(
    out6 <- vroom(
      I("x1,x2,x3\nA2,B2,C2\nA3,#B2,C2\nA4,A5,A6"),
      comment = "#",
      delim = ",",
      col_types = "ccc",
      altrep = FALSE,
      progress = FALSE
    )
  )
  expect_warning(
    out7 <- vroom(
      I("x1,x2,x3\nA2,B2,C2\nA3,#B2,C2\n#comment\nA4,A5,A6"),
      comment = "#",
      delim = ",",
      col_types = "ccc",
      altrep = FALSE,
      progress = FALSE
    )
  )

  chk <- tibble::tibble(
    x1 = c("A2", "A3", "A4"),
    x2 = c("B2", NA_character_, "A5"),
    x3 = c("C2", NA_character_, "A6")
  )

  expect_true(all.equal(chk, out5, check.attributes = FALSE))
  expect_true(all.equal(chk, out6, check.attributes = FALSE))
  expect_true(all.equal(chk, out7, check.attributes = FALSE))
})

test_that("escaped/quoted comments are ignored", {
  out1 <- vroom(
    I('x\n\\#'),
    comment = "#",
    delim = ",",
    escape_backslash = TRUE,
    escape_double = FALSE,
    progress = FALSE,
    col_types = "c"
  )
  out2 <- vroom(
    I('x\n"#"'),
    comment = "#",
    progress = FALSE,
    delim = ",",
    col_types = "c"
  )

  expect_equal(out1$x, "#")
  expect_equal(out2$x, "#")
})

test_that("name repair with custom functions works", {
  add_y <- function(x) {
    paste(x, "y", sep = "_")
  }
  out <- vroom(I("x,y,z\n1,2,3"), col_types = "iii", .name_repair = add_y)
  expect_equal(colnames(out), c("x_y", "y_y", "z_y"))
})

test_that("col_types are based on the final (possibly repaired) column names (#311)", {
  suppressMessages(
    out <- vroom(
      I("x,\n1,2\n3,4"),
      delim = ",",
      col_types = list(x = col_double(), "...2" = col_double())
    )
  )
  expect_equal(out[["...2"]], c(2, 4))
})

test_that("mismatched column names throw a classed warning", {
  expect_warning(
    vroom(
      I("x,y\n1,2\n3,4\n"),
      col_types = list(
        x = col_double(),
        y = col_double(),
        z = col_double()
      )
    ),
    class = "vroom_mismatched_column_name"
  )
})

test_that("empty files still generate the correct column width and types", {
  out <- vroom(I(""), col_names = c("foo", "bar"), col_types = list())
  expect_equal(nrow(out), 0)
  expect_equal(ncol(out), 2)
  expect_equal(names(out), c("foo", "bar"))
  expect_type(out[[1]], "character")
  expect_type(out[[2]], "character")

  out <- vroom(I(""), col_types = "ii")
  expect_equal(nrow(out), 0)
  expect_equal(ncol(out), 2)
  expect_equal(names(out), c("X1", "X2"))
  expect_type(out[[1]], "integer")
  expect_type(out[[2]], "integer")
})

test_that("leading whitespace effects guessing", {
  out <- vroom(
    I('a,b,c\n 1,2,3\n'),
    delim = ",",
    trim_ws = FALSE,
    progress = FALSE,
    col_types = list()
  )
  expect_type(out[[1]], "character")

  out <- vroom(
    I('a,b,c\n 1,2,3\n'),
    delim = ",",
    trim_ws = TRUE,
    progress = FALSE,
    col_types = list()
  )
  expect_type(out[[1]], "double")
})

test_that("UTF-16LE encodings can be read", {
  bom <- as.raw(c(255, 254))
  # This is the text.
  text <- "x,y\n\U104371,2\n" # This is a 4 byte UTF-16 character from https://en.wikipedia.org/wiki/UTF-16

  # Converted to UTF-16LE
  text_utf16 <- iconv(text, from = "UTF-8", to = "UTF-16LE", toRaw = TRUE)[[1]]

  # Write the BOM and the text to a file
  tmp_file_name <- tempfile()
  fd <- file(tmp_file_name, "wb")
  writeBin(bom, fd)
  writeBin(text_utf16, fd)
  close(fd)

  # Whether LE or BE is determined automatically by the BOM
  out <- vroom(
    tmp_file_name,
    locale = locale(encoding = "UTF-16"),
    col_types = "ci"
  )
  expect_equal(out$x, "\U104371")
  expect_equal(out$y, 2)
})

test_that("supports unicode grouping and decimal marks (https://github.com/tidyverse/readr/issues/796)", {
  # libvroom's number type is parsed R-side via gsub which doesn't handle

  # multi-byte Unicode grouping/decimal marks correctly. The locale parameters
  # for grouping_mark and decimal_mark are not forwarded to the gsub pattern.
  skip(
    "libvroom R-side number parsing does not support multi-byte Unicode marks"
  )
  test_vroom(
    I("1\u00A0234\u02D95"),
    locale = locale(grouping_mark = "\u00A0", decimal_mark = "\u02D9"),
    col_types = "n",
    col_names = FALSE,
    delim = ",",
    equals = tibble::tibble(X1 = 1234.5)
  )
})

test_that("handles quotes within skips", {
  data <- I(paste0(
    collapse = "\n",
    c("a\tb\tc", "1a\t1b\t1c", "2a\t2b\t2c\"", "3a\t3b\t3c\"", "4a\t4b\t4c")
  ))

  test_vroom(
    data,
    col_names = c("a", "b", "c"),
    skip = 2,
    quote = "",
    delim = "\t",
    equals = tibble::tibble(
      a = c("2a", "3a", "4a"),
      b = c("2b", "3b", "4b"),
      c = c("2c\"", "3c\"", "4c")
    )
  )

  test_vroom(
    data,
    col_names = c("a", "b", "c"),
    skip = 3,
    quote = "",
    delim = "\t",
    equals = tibble::tibble(
      a = c("3a", "4a"),
      b = c("3b", "4b"),
      c = c("3c\"", "4c")
    )
  )

  test_vroom(
    data,
    col_names = c("a", "b", "c"),
    skip = 4,
    quote = "",
    delim = "\t",
    equals = tibble::tibble(
      a = c("4a"),
      b = c("4b"),
      c = c("4c")
    )
  )
})

test_that("skipped columns retain their name", {
  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = "x",
    col_types = "i__",
    equals = tibble::tibble(
      x = c(1L, 4L)
    )
  )

  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = "y",
    col_types = "_i_",
    equals = tibble::tibble(
      y = c(2L, 5L)
    )
  )

  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = "z",
    col_types = "__i",
    equals = tibble::tibble(
      z = c(3L, 6L)
    )
  )

  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = c("x", "z"),
    col_types = "i_i",
    equals = tibble::tibble(
      x = c(1L, 4L),
      z = c(3L, 6L)
    )
  )
})

test_that("skipped columns retain their name", {
  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = "x",
    col_types = "i__",
    equals = tibble::tibble(
      x = c(1L, 4L)
    )
  )

  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = "y",
    col_types = "_i_",
    equals = tibble::tibble(
      y = c(2L, 5L)
    )
  )

  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = "z",
    col_types = "__i",
    equals = tibble::tibble(
      z = c(3L, 6L)
    )
  )

  test_vroom(
    I("1,2,3\n4,5,6"),
    col_names = c("x", "z"),
    col_types = "i_i",
    equals = tibble::tibble(
      x = c(1L, 4L),
      z = c(3L, 6L)
    )
  )
})

test_that("unnamed column types can be less than the number of columns", {
  test_vroom(
    "x,y\n1,2\n",
    col_types = "i",
    equals = tibble::tibble(
      x = 1L,
      y = 2L
    )
  )
})

test_that("always include the last row when guessing (#352)", {
  # libvroom samples the first N rows sequentially; it does not include the

  # last row as a special case like the legacy parser did.
  skip("libvroom samples first N rows; does not special-case last row")
  f <- tempfile()
  on.exit(unlink(f))

  vroom_write(data.frame("x" = c(rep(NA, 10), 5)), delim = ",", file = f)

  x <- vroom(f, col_types = "?", guess_max = 5, delim = ",")

  expect_type(x[[1]], "double")
})

test_that("vroom works with quote even in the first two lines (#1262)", {
  text <-
    c(
      "1,'I
am
sam'
2,'sam
I
am'"
    )

  test_vroom(
    text,
    col_names = FALSE,
    quote = "'",
    delim = ",",
    equals = tibble::tibble(X1 = c(1, 2), X2 = c("I\nam\nsam", "sam\nI\nam"))
  )
})

test_that("vroom works when grouping_mark is empty (#1241)", {
  x <- vroom(
    I("foo\nbar"),
    locale = locale(grouping_mark = ""),
    delim = ",",
    col_names = FALSE,
    col_types = "c"
  )
  expect_equal(x[[1]], c("foo", "bar"))
})

test_that("vroom works if given col_names and col_types less than the number of columns (https://github.com/tidyverse/readr/issues/1271)", {
  x <- vroom(
    I("a\tb\n"),
    delim = "\t",
    col_names = c("x"),
    col_types = list("x" = "c")
  )

  expect_equal(x[["x"]], "a")
  expect_equal(x[["X2"]], "b")
})

test_that("vroom works with CR line endings only", {
  # libvroom does not support bare CR (\r without \n) as a line ending in I()
  # input. It only recognizes \n and \r\n as line terminators.
  skip("libvroom does not support bare CR line endings in I() input")
  test_vroom(
    I("a,b\r1,2\r3,4\r"),
    delim = ",",
    equals = tibble::tibble(a = c(1, 3), b = c(2, 4))
  )
})

test_that("vroom works with quotes in comments", {
  # libvroom's comment handling interacts with quote tracking, causing
  # misparse when comments contain unmatched quotes.
  skip(
    "libvroom comment handling does not account for quotes inside comment lines"
  )
  test_vroom(
    I("a,b\n#bar \" xyz\n1,2"),
    delim = ",",
    comment = "#",
    equals = tibble::tibble(a = 1, b = 2)
  )

  test_vroom(
    I("#foo \" \na,b\n#bar \" xyz\n1,2"),
    delim = ",",
    comment = "#",
    equals = tibble::tibble(a = 1, b = 2)
  )
})

test_that("vroom works with comments at end of lines (https://github.com/tidyverse/readr/issues/1309)", {
  # libvroom only strips full-line comments, not inline/end-of-field comments.
  skip(
    "libvroom only strips full-line comments, not inline/end-of-field comments"
  )
  test_vroom(
    I("foo,bar#\n1,#\n2#\n#\n3\n"),
    delim = ",",
    comment = "#",
    equals = tibble::tibble(foo = c(1, 2, 3), bar = c(NA, NA, NA))
  )
})

test_that("vroom does not erronously warn for problems when there are embedded newlines and parsing needs to be restarted (https://github.com/tidyverse/readr/issues/1313))", {
  # libvroom's multi-threaded parsing of embedded newlines may produce
  # different results from the legacy single-threaded parser.
  skip("libvroom embedded newline handling differs from legacy parser")
  withr::local_seed(1)

  sample_values <- function(n, p_safe) {
    sample(
      c("safe", "UNSAFE\n"),
      n,
      replace = TRUE,
      prob = c(p_safe, 1 - p_safe)
    )
  }

  n <- 300

  df <- tibble::tibble(
    a = sample_values(n, p_safe = .99),
    b = sample_values(n, p_safe = .01),
    c = sample_values(n, p_safe = .01)
  )

  # write to temp file
  path <- tempfile(pattern = "quoted_newlines_", fileext = ".csv")
  withr::defer(unlink(path))

  vroom_write(df, path, delim = ",")

  x <- vroom(path, delim = ",", col_types = list())
  y <- utils::read.csv(path, stringsAsFactors = FALSE)

  expect_warning(expect_equal(as.data.frame(x), y), NA)
})

test_that("n_max works with files without a trailing newline for file connections (https://github.com/tidyverse/readr/issues/1321)", {
  f <- tempfile()
  on.exit(unlink(f))

  writeBin(
    charToRaw(
      "foo,bar
1,2
3,4
5,6"
    ),
    f
  )

  x <- vroom(f, n_max = Inf, delim = ",", col_types = list())
  y <- vroom(f, n_max = 4, delim = ",", col_types = list())
  z <- vroom(f, n_max = 5, delim = ",", col_types = list())
  expect_equal(y, x)
  expect_equal(z, x)
})

# https://github.com/tidyverse/vroom/issues/453
test_that("vroom can read a date column with no data and skip 1", {
  test_vroom(
    "date\n",
    delim = ",",
    col_names = 'date',
    col_types = 'D',
    skip = 1,
    equals = tibble::tibble(date = as.Date(character()))
  )
})

# https://github.com/tidyverse/vroom/issues/453
test_that("vroom can read a datetime column with no data and skip 1", {
  test_vroom(
    "dt\n",
    delim = ",",
    col_names = 'dt',
    col_types = 'T',
    skip = 1,
    equals = tibble::tibble(dt = as.POSIXct(character()))
  )
})

# https://github.com/tidyverse/vroom/issues/554
# https://github.com/tidyverse/vroom/issues/534
test_that("vroom(col_select =) output has 'spec_tbl_df' class, spec, and problems when readr is attached", {
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

  # libvroom's PERMISSIVE error mode silently converts unparseable values to NA
  # without emitting a vroom_parse_issue warning, so we don't expect the warning.
  dat <- vroom(
    I("a,b\n1,2\nz,3\n4,5"),
    col_types = "dc",
    col_select = c(a, b),
    show_col_types = FALSE,
    altrep = FALSE
  )

  expect_s3_class(dat, "spec_tbl_df")
  expect_equal(
    attr(dat, "spec", exact = TRUE),
    cols(
      a = col_double(),
      b = col_character(),
      .delim = ","
    )
  )
  # libvroom silently converts "z" to NA_real_; problems() still works but
  # may report 0 rows since libvroom doesn't track individual parse errors.
  expect_no_error(probs <- problems(dat))
})

test_that("libvroom is used with col_types = list()", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines(c("x,y", "1,hello", "2,world"), tf)

  # Both should produce identical results via libvroom
  res_null <- vroom(tf, delim = ",", col_types = NULL, show_col_types = FALSE)
  res_list <- vroom(tf, delim = ",", col_types = list(), show_col_types = FALSE)
  expect_equal(res_null, res_list)
})

test_that("shows col_types by default", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines("x,y\n1,hello\n2,world", tf)

  expect_message(
    vroom(tf, delim = ",", show_col_types = NULL),
    "Column specification"
  )
})

test_that("suppresses col_types when show_col_types = FALSE", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines("x,y\n1,hello\n2,world", tf)

  expect_no_message(
    vroom(tf, delim = ",", show_col_types = FALSE)
  )
})

test_that("attaches spec attribute", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines("x,y\n1,hello\n2,world", tf)

  res <- vroom(tf, delim = ",", show_col_types = FALSE)
  s <- spec(res)
  expect_s3_class(s, "col_spec")
  expect_equal(length(s$cols), 2L)
})
