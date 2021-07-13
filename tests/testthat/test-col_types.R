test_that("You can use !! in cols and cols_only", {
  var <- "xyz"
  expect_equal(
    cols(!!var := col_character()),
    cols(xyz = col_character())
  )

  expect_equal(
    cols_only(!!var := col_character()),
    cols_only(xyz = col_character())
  )
})

test_that("format(col_spec) contains the delimiter if specified", {
  expect_match(fixed = TRUE,
    format(cols(.delim = "\t")),
    '.delim = "\\t"'
  )
})

test_that("col_types are truncated if you pass too many (#355)", {
  res <- vroom::vroom("a,b,c,d\n1,2,3,4", col_types = "cccccccc")
  expect_equal(res, tibble::tibble(a = "1", b = "2", c = "3", d = "4"))
})
