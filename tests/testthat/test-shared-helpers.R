# Tests that characterize the shared behavior between vroom() and vroom_fwf()
# before the refactoring to extract helpers.

test_that("vroom() libvroom path applies col_select correctly", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines("a,b,c\n1,2,3\n4,5,6", tf)

  out <- vroom(tf, delim = ",", col_select = c(b, c), show_col_types = FALSE)
  expect_equal(names(out), c("b", "c"))
  expect_equal(nrow(out), 2)
})

test_that("vroom_fwf() libvroom path applies col_select correctly", {
  tf <- tempfile(fileext = ".txt")
  on.exit(unlink(tf))
  writeLines(c("ab cd", "12 34", "56 78"), tf)

  out <- vroom_fwf(
    tf,
    col_positions = fwf_widths(c(3, 3), c("x", "y")),
    col_select = c(y),
    show_col_types = FALSE
  )
  expect_equal(names(out), "y")
})

test_that("vroom() legacy path post-processes correctly", {
  # Use I() to force legacy path
  out <- vroom(
    I("a,b,c\n1,2,3\n4,5,6\n"),
    col_select = c(a, c),
    show_col_types = FALSE
  )
  expect_equal(names(out), c("a", "c"))
  expect_s3_class(out, "spec_tbl_df")
})

test_that("vroom_fwf() legacy path post-processes correctly", {
  # Use I() to force legacy path
  out <- vroom_fwf(
    I("ab cd\n12 34\n56 78\n"),
    col_positions = fwf_widths(c(3, 3), c("x", "y")),
    col_select = c(y),
    show_col_types = FALSE
  )
  expect_equal(names(out), "y")
  expect_s3_class(out, "spec_tbl_df")
})

test_that("vroom() col_select with renaming works on libvroom path", {
  tf <- tempfile(fileext = ".csv")
  on.exit(unlink(tf))
  writeLines("a,b,c\n1,2,3\n4,5,6", tf)

  out <- vroom(
    tf,
    delim = ",",
    col_select = c(x = a, y = b),
    show_col_types = FALSE
  )
  expect_equal(names(out), c("x", "y"))
})

test_that("vroom_fwf() col_select with renaming works on libvroom path", {
  tf <- tempfile(fileext = ".txt")
  on.exit(unlink(tf))
  writeLines(c("ab cd", "12 34", "56 78"), tf)

  out <- vroom_fwf(
    tf,
    col_positions = fwf_widths(c(3, 3), c("x", "y")),
    col_select = c(a = x),
    show_col_types = FALSE
  )
  expect_equal(names(out), "a")
})
