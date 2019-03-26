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
  test_vroom('\n\n   \t \t\n  \n\na,b,c\n1,2,3\n', delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom ignores comments", {
  test_vroom('\n\n \t #a,b,c\na,b,c\n1,2,3\n', delim = ",", comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects skip", {
  test_vroom('#a,b,c\na,b,c\n1,2,3\n', delim = ",", skip = 1,
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('#a,b,c\na,b,c\n1,2,3\n', delim = ",", skip = 1, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('#a,b,c\nasdfasdf\na,b,c\n1,2,3\n', delim = ",", skip = 2, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('\n\n#a,b,c\nasdfasdf\na,b,c\n1,2,3\n', delim = ",", skip = 4, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects col_types", {
  test_vroom('a,b,c\n1,2,3\n', delim = ",", col_types = "idc",
    equals = tibble::tibble(a = 1L, b = 2, c = "3")
  )

  test_vroom('a,b,c,d\nT,2,3,4\n', delim = ",", col_types = "lfc_",
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

  file.create("foo")
  on.exit(unlink("foo"))

  capture.output(type = "message",
    expect_equal(vroom("foo"), tibble::tibble())
  )

  expect_equal(vroom(character()), tibble::tibble())
})

test_that("vroom_example() returns the example files", {

  expect_equal(vroom_example(), list.files(system.file("extdata", package = "vroom")))
})

test_that("subsets work", {
  res <- vroom("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14", col_names = FALSE)
  expect_equal(head(res[[1]]), c(1:6))
  expect_equal(tail(res[[1]]), c(9:14))

  expect_equal(tail(res[[1]][3:8]), c(3:8))
})

test_that("col_keep works", {
  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_keep = 1)), "model")

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_keep = 1:3)), c("model", "mpg", "cyl"))

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_keep = c(1, 5, 7))), c("model", "hp", "wt"))

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_keep = c("model", "hp", "wt"))), c("model", "hp", "wt"))

  expect_equal(
    colnames(vroom(vroom_example("mtcars.csv"), col_keep = c(TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE))),
    c("model", "hp", "wt")
  )

  expect_equal(
    colnames(vroom(vroom_example("mtcars.csv"), col_keep = c(TRUE, FALSE))),
    c("model", "cyl", "hp", "wt", "vs", "gear")
  )
})

test_that("col_skip works", {
  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_skip = 1)),
    c("mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_skip = 1:3)),
    c("disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_skip = c(1, 5, 7))),
    c("mpg", "cyl", "disp", "drat", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_skip = c("model", "hp", "wt"))),
    c("mpg", "cyl", "disp", "drat", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_skip = c("model", "hp", "wt"))),
    c("mpg", "cyl", "disp", "drat", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(
    colnames(vroom(vroom_example("mtcars.csv"), col_skip = c(TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE))),
    c("mpg", "cyl", "disp", "drat", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(
    colnames(vroom(vroom_example("mtcars.csv"), col_skip = c(FALSE, TRUE))),
    c("model", "cyl", "hp", "wt", "vs", "gear")
  )
})

test_that("error if both col_skip and col_keep", {
  expect_error(
    vroom(vroom_example("mtcars.csv"), col_keep = 1, col_skip = 2),
    "Only one of `col_keep` and `col_skip` can be set")
})

test_that("n_max works with normal files", {
    expect_equal(
      NROW(vroom(vroom_example("mtcars.csv"), n_max = 2)),
      2
    )

    # headers don't count
    expect_equal(
      NROW(vroom(vroom_example("mtcars.csv"), n_max = 2, col_names = FALSE)),
      2
    )

    # Zero rows with headers should just have the headers
    expect_equal(
      dim(vroom(vroom_example("mtcars.csv"), n_max = 0)),
      c(0, 12)
    )

    # If you don't read the header or any rows it must be empty
    expect_equal(
      dim(vroom(vroom_example("mtcars.csv"), n_max = 0, col_names = FALSE)),
      c(0, 0)
    )
})

test_that("n_max works with connections files", {
    expect_equal(
      NROW(vroom(vroom_example("mtcars.csv.gz"), n_max = 2)),
      2
    )

    # headers don't count
    expect_equal(
      NROW(vroom(vroom_example("mtcars.csv.gz"), n_max = 2, col_names = FALSE)),
      2
    )

    # Zero rows with headers should just have the headers
    expect_equal(
      dim(vroom(vroom_example("mtcars.csv.gz"), n_max = 0)),
      c(0, 12)
    )

    # If you don't read the header or any rows it must be empty
    expect_equal(
      dim(vroom(vroom_example("mtcars.csv.gz"), n_max = 0, col_names = FALSE)),
      c(0, 0)
    )
})

# Figure out a better way to test progress bars...
#test_that("progress bars work", {
  #withr::with_options(c("vroom.show_after" = 0), {
    #expect_output_file(vroom(vroom_example("mtcars.csv"), progress = TRUE), "mtcars-progress")
  #})
#})
