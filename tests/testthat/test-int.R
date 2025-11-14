test_that("integers are returned correctly", {
  # empty fields are returned as NAs
  test_vroom(
    "foo,bar,baz\n1,,3\n",
    col_types = list(.default = "i"),
    delim = ",",
    equals = tibble::tibble(foo = 1L, bar = NA_integer_, baz = 3L)
  )

  # numbers which are not integers are returned as NAs
  test_vroom(
    "foo,bar,baz\n1,1.5,3\n",
    col_types = list(.default = "i"),
    delim = ",",
    equals = tibble::tibble(foo = 1L, bar = NA_integer_, baz = 3L)
  )

  # fields with non-digits are returned as NAs
  test_vroom(
    "foo,bar,baz\n1,32xyz,3\n",
    col_types = list(.default = "i"),
    delim = ",",
    equals = tibble::tibble(foo = 1L, bar = NA_integer_, baz = 3L)
  )

  # 2^31 - 1 is the maximum representable integer with 32 bit ints
  test_vroom(
    "foo,bar,baz\n1,2147483647,3\n",
    col_types = list(.default = "i"),
    delim = ",",
    equals = tibble::tibble(foo = 1L, bar = 2147483647L, baz = 3L)
  )

  test_vroom(
    "foo,bar,baz\n1,2147483648,3\n",
    col_types = list(.default = "i"),
    delim = ",",
    equals = tibble::tibble(foo = 1L, bar = NA_integer_, baz = 3L)
  )
})

test_that("NA can be a int value", {
  test_vroom(
    I("x\n1\n2\n"),
    delim = ",",
    col_types = "i",
    na = "1",
    equals = tibble::tibble(x = c(NA_integer_, 2L))
  )
})
