test_that("vroom_lines works with normal files", {
  infile <- vroom_example("mtcars.csv")

  expected <- readLines(infile)

  actual <- vroom_lines(infile)

  expect_equal(length(actual), length(expected))

  expect_equal(head(actual), head(expected), ignore_attr = "problems")

  expect_equal(tail(actual), tail(expected), ignore_attr = "problems")

  expect_equal(actual, expected, ignore_attr = "problems")
})

test_that("vroom_lines works with connections files", {
  infile <- vroom_example("mtcars.csv")

  con <- file(infile)
  expected <- readLines(con)
  close(con)

  actual <- vroom_lines(file(infile))

  expect_equal(length(actual), length(expected))

  expect_equal(head(actual), head(expected), ignore_attr = "problems")

  expect_equal(tail(actual), tail(expected), ignore_attr = "problems")

  expect_equal(actual, expected, ignore_attr = "problems")
})


test_that("vroom_lines works with files with no trailing newline", {
  f <- tempfile()
  on.exit(unlink(f))

  writeBin(charToRaw("foo"), f)
  expect_equal(vroom_lines(f), "foo", ignore_attr = "problems")

  f2 <- tempfile()
  on.exit(unlink(f2), add = TRUE)

  writeBin(charToRaw("foo\nbar"), f2)
  expect_equal(vroom_lines(f2), c("foo", "bar"), ignore_attr = "problems")
})

test_that("vroom_lines respects n_max", {
  infile <- vroom_example("mtcars.csv")
  expect_equal(vroom_lines(infile, n_max = 2), readLines(infile, n = 2), ignore_attr = "problems")
})

test_that("vroom_lines works with empty files", {
  f <- tempfile()
  file.create(f)
  on.exit(unlink(f))

  expect_equal(vroom_lines(f), character())
})

test_that("vroom_lines uses na argument", {
  expect_equal(vroom_lines(I("abc\n123"), progress = FALSE), c("abc", "123"), ignore_attr = "problems")
  expect_equal(
    vroom_lines(I("abc\n123"), na = "abc", progress = FALSE),
    c(NA_character_, "123"),
    ignore_attr = "problems"
  )
  expect_equal(
    vroom_lines(I("abc\n123"), na = "123", progress = FALSE),
    c("abc", NA_character_),
    ignore_attr = "problems"
  )
  expect_equal(
    vroom_lines(I("abc\n123"), na = c("abc", "123"), progress = FALSE),
    c(NA_character_, NA_character_),
    ignore_attr = "problems"
  )
})

test_that("vroom_lines works with files with mixed line endings", {
  expect_equal(
    vroom_lines(I("foo\r\n\nbar\n\r\nbaz\r\n")),
    c("foo", "", "bar", "", "baz"),
    ignore_attr = "problems"
  )
})

test_that("vroom_lines propagates problems attribute from vroom_()", {
  lines <- vroom_lines(I("a\nb\nc\n"), progress = FALSE)

  expect_true(!is.null(attr(lines, "problems")))
})

test_that("vroom_lines preserves problems attribute for problems()", {
  tmp <- withr::local_tempfile()
  writeBin(c(charToRaw("line1\n"), as.raw(0x00), charToRaw("line2\n")), tmp)

  expect_warning(
    lines <- vroom_lines(tmp, progress = FALSE),
    class = "vroom_parse_issue"
  )

  expect_true(!is.null(attr(lines, "problems")))

  probs <- problems(lines)
  expect_s3_class(probs, "tbl_df")
  expect_true(nrow(probs) > 0)
  expect_true("embedded null" %in% probs$actual)
})

test_that("problems() errors informatively on bare character vectors", {
  expect_error(
    problems(c("a", "b")),
    "no.*problems.*attribute"
  )
})
