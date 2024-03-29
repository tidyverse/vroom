test_that("empty columns create an empty file", {
  out <- tempfile()

  no_cols <- mtcars[, FALSE]
  no_rows_or_cols <- mtcars[FALSE, FALSE]

  expect_equal(vroom_write(no_cols, out), no_cols)
  expect_true(file.exists(out))
  unlink(out)

  expect_equal(vroom_write(no_rows_or_cols, out), no_rows_or_cols)
  expect_true(file.exists(out))
})

test_that("empty rows print the headers", {
  out <- tempfile()
  on.exit(unlink(out))

  no_rows <- mtcars[FALSE, ]

  expect_equal(vroom_write(no_rows, out), no_rows)

  expect_equal(strsplit(readLines(out), "\t")[[1]], colnames(mtcars))
})

test_that("strings are only quoted if needed", {
  x <- c("a", ',')

  csv <- vroom_format(data.frame(x), delim = ",",col_names = FALSE)
  expect_equal(csv, 'a\n\",\"\n')
  ssv <- vroom_format(data.frame(x), delim = " ",col_names = FALSE)
  expect_equal(ssv, 'a\n,\n')
})

test_that("read_delim/csv/tsv and write_delim round trip special chars", {
  x <- stats::setNames(list("a", '"', ",", "\n","at\t"), paste0("V", seq_len(5)))

  output <- tibble::as_tibble(x)
  output_space <- vroom(I(vroom_format(output, delim = " ")), trim_ws = FALSE, progress = FALSE, col_types = list())
  output_csv <- vroom(I(vroom_format(output, delim = ",")), trim_ws = FALSE, progress = FALSE, col_types = list())
  output_tsv <- vroom(I(vroom_format(output, delim = "\t")), trim_ws = FALSE, progress = FALSE, col_types = list())
  expect_equal(output_space, output)
  expect_equal(output_csv, output)
  expect_equal(output_tsv, output)
})

test_that("special floating point values translated to text", {
  df <- data.frame(x = c(NaN, NA, Inf, -Inf))
  expect_equal(vroom_format(df), "x\nNA\nNA\nInf\n-Inf\n")
})

test_that("NA integer values translated to text", {
  df <- data.frame(x = c(NA, 1L, 5L, 1234567890L))
  expect_equal(vroom_format(df), "x\nNA\n1\n5\n1234567890\n")
})

test_that("logical values give long names", {
  df <- data.frame(x = c(NA, FALSE, TRUE))
  expect_equal(vroom_format(df), "x\nNA\nFALSE\nTRUE\n")
})

test_that("roundtrip preserved floating point numbers", {
  input <- data.frame(x = runif(100))
  output <- vroom(I(vroom_format(input, delim = " ")), delim = " ", col_types = list())

  expect_equal(input$x, output$x)
})

test_that("roundtrip preserves dates and datetimes", {
  x <- as.Date("2010-01-01") + 1:10
  y <- as.POSIXct(x)
  attr(y, "tzone") <- "UTC"

  input <- data.frame(x, y)
  output <- vroom(I(vroom_format(input, delim = " ")), delim = " ", col_types = list())

  expect_equal(output$x, x)
  expect_equal(output$y, y)
})

test_that("fails to create file in non-existent directory", {
  expect_error(vroom_write(mtcars, file.path(tempdir(), "/x/y"), "\t"),
    "Cannot open file for writing")
})

test_that("includes a byte order mark if desired", {
  tmp <- tempfile()
  on.exit(unlink(tmp))

  vroom_write(mtcars, tmp, bom = TRUE)

  output <- readBin(tmp, "raw", file.info(tmp)$size)

  # BOM is there
  expect_equal(output[1:3], charToRaw("\xEF\xBB\xBF"))

  # Rest of file also there
  expect_equal(output[4:6], charToRaw("mpg"))
})


test_that("does not writes a trailing .0 for whole number doubles", {
  expect_equal(vroom_format(tibble::tibble(x = 1)), "x\n1\n")

  expect_equal(vroom_format(tibble::tibble(x = 0)), "x\n0\n")

  expect_equal(vroom_format(tibble::tibble(x = -1)), "x\n-1\n")

  expect_equal(vroom_format(tibble::tibble(x = 999)), "x\n999\n")

  expect_equal(vroom_format(tibble::tibble(x = -999)), "x\n-999\n")

  expect_equal(vroom_format(tibble::tibble(x = 123456789)), "x\n123456789\n")

  expect_equal(vroom_format(tibble::tibble(x = -123456789)), "x\n-123456789\n")
})

test_that("can write windows eol characters if desired (#263)", {
  expect_equal(vroom_format(tibble::tibble(x = 1), eol = "\r\n"), "x\r\n1\r\n")
})

test_that("write_csv can write to compressed files", {
  mt <- vroom(vroom_example("mtcars.csv"), col_types = list())

  filename <- file.path(tempdir(), "mtcars.csv.bz2")
  on.exit(unlink(filename))
  vroom_write(mt, filename)

  is_bz2_file <- function(x) {

    # Magic number for bz2 is "BZh" in ASCII
    # https://en.wikipedia.org/wiki/Bzip2#File_format
    identical(charToRaw("BZh"), readBin(x, n = 3, what = "raw"))
  }

  expect_true(is_bz2_file(filename))

  expect_equal(vroom(filename, col_types = list()), mt)
})

test_that("write_csv writes large integers without scientific notation #671", {
  x <- data.frame(a = c(60150001022000, 60150001022001))
  expect_equal(vroom_format(x), "a\n60150001022000\n60150001022001\n")
})

test_that("write_csv writes large integers without scientific notation up to 1E15 #671", {
  x <- data.frame(a = c(1E13, 1E14, 1E15, 1E16))
  expect_equal(vroom_format(x), "a\n10000000000000\n100000000000000\n1e15\n1e16\n")
})

#test_that("write_csv2 and format_csv2 writes ; sep and , decimal mark", {
  #df <- tibble::tibble(x = c(0.5, 2, 1.2), y = c("a", "b", "c"))
  #expect_equal(format_csv2(df), "x;y\n0,5;a\n2,0;b\n1,2;c\n")

  #filename <- tempfile(pattern = "readr", fileext = ".csv")
  #on.exit(unlink(filename))
  #write_csv2(df, filename)

  #expect_equivalent(df, suppressMessages(read_csv2(filename)))
#})

#test_that("write_csv2 and format_csv2 writes NA appropriately", {
  #df <- tibble::tibble(x = c(0.5, NA, 1.2), y = c("a", "b", NA))
  #expect_equal(format_csv2(df), "x;y\n0,5;a\nNA;b\n1,2;NA\n")
#})

test_that("Can change the escape behavior for quotes", {
  df <- data.frame(x = c("a", '"', ",", "\n"))

  expect_error(vroom_format(df, "\t", escape = "invalid"), "should be one of")

  expect_equal(vroom_format(df, "\t"), 'x\na\n""""\n,\n"\n"\n')
  expect_equal(vroom_format(df, "\t", escape = "double"), "x\na\n\"\"\"\"\n,\n\"\n\"\n")
  expect_equal(vroom_format(df, "\t", escape = "backslash"), "x\na\n\"\\\"\"\n,\n\"\n\"\n")
  expect_equal(vroom_format(df, "\t", escape = "none"), "x\na\n\"\"\"\n,\n\"\n\"\n")
})

test_that("hms NAs are written without padding (#930)", {
  df <- data.frame(x = hms::as_hms(c(NA, 34.234)))
  expect_equal(vroom_format(df), "x\nNA\n00:00:34.234\n")
})

test_that("vroom_write equals the same thing as vroom_format", {
  df <- gen_tbl(100, 8, col_types = c("dilfcDtT"), missing = .1)
  tf <- tempfile()
  on.exit(unlink(tf))

  # Temporarily run with 2 lines per buffer, to test the multithreading
  withr::with_envvar(c("VROOM_WRITE_BUFFER_LINES" = "2"),
    vroom_write(df, tf, "\t")
  )

  expect_equal(readChar(tf, file.info(tf)$size), vroom_format(df))
})

test_that("vroom_format handles empty data frames", {
  df <- data.frame()
  expect_equal(vroom_format(df), "")

  df <- data.frame(a = 1:2, b = 2:3)
  df <- df[0, ]
  expect_equal(vroom_format(df), "a\tb\n")
})

test_that("vroom_write() / vroom_read() roundtrips an empty data frame", {
  df <- tibble::tibble()
  t <- tempfile(fileext = ".csv")
  on.exit(unlink(t))

  vroom_write(df, t)
  expect_equal(vroom(t, show_col_types = FALSE), df)
})

test_that("vroom_write(append = TRUE) works with R connections", {
  df <- data.frame(x = 1, y = 2)
  f <- tempfile(fileext = ".tsv.gz")
  on.exit(unlink(f))

  vroom::vroom_write(df, f)
  vroom::vroom_write(df, f, append = TRUE)

  expect_equal(vroom_lines(f), c("x\ty", "1\t2", "1\t2"))
})

test_that("vroom_write() works with an empty delimiter", {
  df <- data.frame(x = "foo", y = "bar")

  f <- tempfile(fileext = ".tsv.gz")
  on.exit(unlink(f))

  vroom::vroom_write(df, f, delim = "")
  expect_equal(vroom_lines(f), c("xy", "foobar"))
})

test_that("vroom_write_lines() works with empty", {
  f <- tempfile(fileext = ".txt")
  on.exit(unlink(f))

  vroom::vroom_write_lines(character(), f)
  expect_equal(vroom_lines(f), character())
})

test_that("vroom_write_lines() works with normal input", {
  f <- tempfile(fileext = ".txt")
  on.exit(unlink(f))

  vroom::vroom_write_lines(c("foo", "bar"), f)
  expect_equal(vroom_lines(f), c("foo", "bar"))
})

test_that("vroom_write_lines() does not escape or quote lines", {
  f <- tempfile(fileext = ".txt")
  on.exit(unlink(f))

  vroom::vroom_write_lines(c('"foo"', "bar"), f)
  expect_equal(vroom_lines(f), c('"foo"', "bar"))
})

test_that("vroom_write() always outputs in UTF-8", {
  x <- iconv(c("ao\u00FBt", "\u00E9l\u00E8ve", "\u00E7a va"), "UTF-8", "latin1")

  data <- tibble::tibble(X = x)
  names(data) <- iconv("fran\u00E7aise", "UTF-8", "latin1")

  f <- tempfile()
  vroom_write(data, f, delim = ",")

  expected_data <- charToRaw(
    paste0(collapse = "\n",
      c(
        enc2utf8(names(data)),
        enc2utf8(data[[1]]),
        "")
    )
  )

  actual_data <- readBin(f, "raw", file.size(f))

  expect_equal(actual_data, expected_data)
})

test_that("vroom_format() does not quote strings that start with the `na` string (#426)", {
  names_df <- tibble::tibble(x = c(NA, "NA", "NATHAN", "JONAH"))

  simple_output <- vroom_format(names_df)
  expect_equal(
    simple_output,
    "x\nNA\nNA\nNATHAN\nJONAH\n"
  )

  output_unique_NA <- vroom_format(names_df, na = "JON")
  expect_equal(
    output_unique_NA,
    "x\nJON\nNA\nNATHAN\nJONAH\n"
  )
})

test_that("na argument modifies how missing values are written", {
  df <- data.frame(x = c(NA, "x"), y = c(1, 2))
  expect_equal(vroom_format(df, ",", na = "None"), "x,y\nNone,1\nx,2\n")
})

test_that("vroom_write() does not overwrite file when appending empty data frame", {
  tf <- withr::local_tempfile()
  data <- tibble::tibble(
    a = "1",
    b = "2",
    c = "3"
  )

  vroom_write(data, file = tf, delim = ",")
  vroom_write(data.frame(), file = tf, append = TRUE, delim = ",")

  expect_equal(vroom_lines(tf, altrep = FALSE), c("a,b,c", "1,2,3"))
})
