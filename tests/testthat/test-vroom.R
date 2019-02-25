context("test-vroom.R")

test_that("vroom can read a tsv", {
  test_vroom("a\tb\tc\n1\t2\t3\n",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom can read a csv", {
  test_vroom("a,b,c\n1,2,3\n", delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom guesses columns with NAs", {
  test_vroom("a,b,c\nNA,2,3\n4,5,6\n", delim = ",",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom("a,b,c\nfoo,2,3\n4,5,6\n", delim = ",", na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom("a,b,c\nfoo,2,3\n4.0,5,6\n", delim = ",", na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom("a,b,c\nfoo,2,3\nbar,5,6\n", delim = ",", na = "foo",
    equals = tibble::tibble(a = c(NA, "bar"), b = c(2, 5), c = c(3, 6))
  )
})

test_that("vroom can trim whitespace", {
  test_vroom('a,b,c\n foo ,  bar  ,baz\n', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_vroom('a,b,c\n\tfoo\t,\t\tbar\t\t,baz\n', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # whitespace trimmed before quotes
  test_vroom('a,b,c\n "foo" ,  "bar"  ,"baz"\n', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # whitespace kept inside quotes
  test_vroom('a,b,c\n "foo" ,  " bar"  ,"\tbaz"\n', delim = ",",
    equals = tibble::tibble(a = "foo", b = " bar", c = "\tbaz")
  )
})

test_that("vroom can read files with quotes", {
  test_vroom('"a","b","c"\n"foo","bar","baz"\n', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_vroom('"a","b","c"\n",foo","bar","baz"\n', delim = ",",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )

  test_vroom("'a','b','c'\n',foo','bar','baz'\n", delim = ",", quote = "'",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )
})

test_that("vroom escapes double quotes", {
  test_vroom('"a","b","c"\n"""fo""o","b""""ar","baz"""\n', delim = ",",
    equals = tibble::tibble(a = "\"fo\"o", b = "b\"\"ar", c = "baz\"")
  )
})

test_that("vroom escapes backslashes", {
  test_vroom('a,b,c\n\\,foo,\\"ba\\"r,baz\\"\n', delim = ",", escape_backslash = TRUE,
    equals = tibble::tibble(a = ",foo", b = "\"ba\"r", c = "baz\"")
  )
})

test_that("vroom ignores leading whitespace", {
  test_vroom('\n\n   \t \t\n  \n\na,b,c\n1,2,3"\n', delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom ignores comments", {
  test_vroom('\n\n \t #a,b,c\na,b,c\n1,2,3"\n', delim = ",", comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects skip", {
  test_vroom('#a,b,c\na,b,c\n1,2,3"\n', delim = ",", skip = 1,
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('#a,b,c\na,b,c\n1,2,3"\n', delim = ",", skip = 1, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('#a,b,c\nasdfasdf\na,b,c\n1,2,3"\n', delim = ",", skip = 2, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('\n\n#a,b,c\nasdfasdf\na,b,c\n1,2,3"\n', delim = ",", skip = 4, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects col_types", {
  test_vroom('a,b,c\n1,2,3"\n', delim = ",", col_types = "idc",
    equals = tibble::tibble(a = 1L, b = 2, c = "3")
  )

  test_vroom('a,b,c,d\nT,2,3,4"\n', delim = ",", col_types = "lfc_",
    equals = tibble::tibble(a = TRUE, b = factor(2), c = "3")
  )
})

test_that("vroom handles UTF byte order marks", {
  # UTF-8
  expect_equal(
    vroom(as.raw(c(0xef, 0xbb, 0xbf, # BOM
                0x41, # A
                0x0A # newline
             )), col_names = FALSE
    )[[1]],
    "A")

  # UTF-16 Big Endian
  expect_equal(
    vroom(as.raw(c(0xfe, 0xff, # BOM
                0x41, # A
                0x0A # newline
             )), col_names = FALSE
    )[[1]],
    "A")

  # UTF-16 Little Endian
  expect_equal(
    vroom(as.raw(c(0xff, 0xfe, # BOM
                0x41, # A
                0x0A # newline
             )), col_names = FALSE
    )[[1]],
    "A")

  # UTF-32 Big Endian
  expect_equal(
    vroom(as.raw(c(0x00, 0x00, 0xfe, 0xff, # BOM
                0x41, # A
                0x0A # newline
             )), col_names = FALSE
    )[[1]],
    "A")

  # UTF-32 Little Endian
  expect_equal(
    vroom(as.raw(c(0xff, 0xfe, 0x00, 0x00, # BOM
                0x41, # A
                0x0A # newline
             )), col_names = FALSE
    )[[1]],
    "A")
})

test_that("vroom handles vectors shorter than the UTF byte order marks", {

  expect_equal(
    charToRaw(vroom(as.raw(c(0xef, 0xbb, 0x0A)), col_names = FALSE)[[1]]),
    as.raw(c(0xef, 0xbb))
  )

  expect_equal(
    charToRaw(vroom(as.raw(c(0xfe, 0x0A)), col_names = FALSE)[[1]]),
    as.raw(c(0xfe))
  )

  expect_equal(
    charToRaw(vroom(as.raw(c(0xff, 0x0A)), col_names = FALSE)[[1]]),
    as.raw(c(0xff))
  )
})

test_that("vroom handles windows newlines", {

  expect_equal(
    vroom("a\tb\r\n1\t2\r\n", trim_ws = FALSE)[[1]],
    1
  )
})

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

test_that("vroom can read a file with only headers", {
  test_vroom("a\n",
    equals = tibble::tibble(a = character())
  )

  test_vroom("a,b,c\n",
    equals = tibble::tibble(a = character(), b = character(), c = character())
  )
})

test_that("vroom can read an empty file", {
  test_vroom("\n",
    equals = tibble::tibble()
  )
})
