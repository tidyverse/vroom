test_that("strings mapped to levels", {
  test_vroom("a\nb\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("a", "b"))),
    equals = tibble::tibble(X1 = factor(c("a", "b")))
  )
})

test_that("can generate ordered factor", {
  res <- test_vroom("a\nb\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("a", "b"), ordered = TRUE)),
    equals = tibble::tibble(X1 = ordered(c("a", "b")))
  )

  expect_true(is.ordered(res$X1))
})

test_that("NA if value not in levels", {
  test_vroom("a\nb\nc\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("a", "b"))),
    equals = tibble::tibble(X1 = factor(c("a", "b", NA)))
  )
})

test_that("NAs silently passed along", {
  test_vroom("a\nb\nNA\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("a", "b"), include_na = FALSE)),
    equals = tibble::tibble(X1 = factor(c("a", "b", NA)))
  )
})

test_that("levels = NULL", {
  test_vroom("a\nb\nc\nb\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = NULL)),
    equals = tibble::tibble(X1 = factor(c("a", "b", "c", "b")))
  )
})

test_that("levels = NULL orders by data", {
  test_vroom("b\na\nc\nb\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = NULL)),
    equals = tibble::tibble(X1 = factor(c("b", "a", "c", "b"), levels = c("b", "a", "c")))
  )
})

test_that("levels = NULL is the default", {
  test_vroom("a\nb\nc\nd\n", col_names = FALSE,
    col_types = list(X1 = "f"),
    equals = tibble::tibble(X1 = factor(c("a", "b", "c", "d")))
  )
})

test_that("NAs included in levels if desired", {
  test_vroom("NA\nb\na\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("a", "b", NA))),
    equals = tibble::tibble(X1 = factor(c(NA, "b", "a"), levels = c("a", "b", NA), exclude = NULL))
  )

  test_vroom("NA\nb\na\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("a", "b", NA), include_na = TRUE)),
    equals = tibble::tibble(X1 = factor(c(NA, "b", "a"), levels = c("a", "b", NA), exclude = NULL))
  )

  test_vroom("NA\nb\na\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = NULL, include_na = FALSE)),
    equals = tibble::tibble(X1 = factor(c("NA", "b", "a"), levels = c("NA", "b", "a")))
  )

  test_vroom("NA\nb\na\n", col_names = FALSE,
    col_types = list(X1 = col_factor(levels = NULL, include_na = TRUE)),
    equals = tibble::tibble(X1 = factor(c(NA, "b", "a"), levels = c(NA, "b", "a"), exclude = NULL))
  )
})

#test_that("Factors handle encodings properly (#615)", {
  #x <- test_vroom(encoded("test\nA\n\xC4\n", "latin1"),
    #col_types = cols(col_factor(c("A", "\uC4"))),
    #locale = locale(encoding = "latin1"), progress = FALSE)

  #expect_is(x$test, "factor")
  #expect_equal(x$test, factor(c("A", "\uC4")))
#})

test_that("factors parse like factor if trim_ws = FALSE", {
  test_vroom("a\na \n", col_names = FALSE, trim_ws = FALSE,
    col_types = list(X1 = col_factor(levels = "a")),
    equals = tibble::tibble(X1 = factor(c("a", "a "), levels = c("a")))
  )
  test_vroom("a\na \n", col_names = FALSE, trim_ws = FALSE,
    col_types = list(X1 = col_factor(levels = "a ")),
    equals = tibble::tibble(X1 = factor(c("a", "a "), levels = c("a ")))
  )
  test_vroom("a\na \n", col_names = FALSE, trim_ws = FALSE,
    col_types = list(X1 = col_factor(levels = c("a ", "a"))),
    equals = tibble::tibble(X1 = factor(c("a", "a "), levels = c("a ", "a")))
  )
})

test_that("Can parse a factor with levels of NA and empty string", {
  x <- c("NC", "NC", "NC", "", "", "NB", "NA", "", "", "NB", "NA",
    "NA", "NC", "NB", "NB", "NC", "NB", "NA", "NA")

  x_in <- paste0(paste(x, collapse = "\n"), "\n")

  test_vroom(x_in, col_names = FALSE,
    col_types = list(X1 = col_factor(levels = c("NA", "NB", "NC", ""))), na = character(),
    equals = tibble::tibble(X1 = factor(x, levels = c("NA", "NB", "NC", "")))
  )
})


test_that("encodings are respected", {
  loc <- locale(encoding = "ISO-8859-1")
  expected <- c("fran\u00e7ais", "\u00e9l\u00e8ve")

  x <- vroom(test_path("enc-iso-8859-1.txt"), delim = "\n", locale = loc, col_types = c(X1 = "f"), col_names = FALSE)
  expect_equal(x[[1]], factor(expected, levels = expected))

  y <- vroom(
    test_path("enc-iso-8859-1.txt"),
    delim = "\n",
    locale = loc,
    col_types = list(X1 = col_factor(levels = expected)),
    col_names = FALSE
  )

  expect_equal(y[[1]], factor(expected, levels = expected))
})

test_that("Results are correct with backslash escapes", {
  obj <- vroom("A,T\nB,F\n", col_names = FALSE, col_types = list("f", "f"), escape_backslash = TRUE)
  exp <- tibble::tibble(X1 = factor(c("A", "B")), X2 = factor(c("T", "F"), levels = c("T", "F")))
  expect_equal(obj, exp)

  obj2 <- vroom("A,T\nB,F\n", col_names = FALSE, col_types = list("f", "f"), escape_backslash = FALSE)
  expect_equal(obj2, exp)
})


test_that("subsetting works with both double and integer indexes", {
  x <- vroom("X1\nfoo", delim = ",", col_types = "f")
  expect_equal(x$X1[1L], factor("foo"))
  expect_equal(x$X1[1], factor("foo"))
  expect_equal(x$X1[NA_integer_], factor(NA_character_, levels = "foo"))
  expect_equal(x$X1[NA_real_], factor(NA_character_, levels = "foo"))
})
