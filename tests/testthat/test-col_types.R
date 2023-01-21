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
  res <- vroom(I("a,b,c,d\n1,2,3,4"), col_types = "cccccccc")
  expect_equal(res, tibble::tibble(a = "1", b = "2", c = "3", d = "4"))
})

test_that("all col_types can be reported with color", {
  local_reproducible_output(crayon = TRUE)
  dat <- vroom(
    I(glue::glue("
      skip,guess,character,factor,logical,double,integer,big_integer,\\
      number,date,datetime
      skip,a,b,c,TRUE,1.3,5,10,\"1,234.56\",2023-01-20,2018-01-01 10:01:01")),
    col_types = "_?cfldiInDT"
  )
  expect_snapshot(spec(dat))
})
