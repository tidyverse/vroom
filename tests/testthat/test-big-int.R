as.integer64 <- bit64::as.integer64

test_that("integers are returned correctly", {
  # empty fields are returned as NAs
  test_vroom("foo,bar,baz\n1,,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64(NA), baz = as.integer64(3))
  )

  # numbers which are not integers are returned as NAs
  test_vroom("foo,bar,baz\n1,1.5,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64(NA), baz = as.integer64(3))
  )

  # fields with non-digits are returned as NAs
  test_vroom("foo,bar,baz\n1,32xyz,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64(NA), baz = as.integer64(3))
  )

  # 2^31 - 1 is the maximum representable integer with 32 bit ints
  test_vroom("foo,bar,baz\n1,2147483647,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64("2147483647"), baz = as.integer64(3))
  )

  # But 2^31 should also work
  test_vroom("foo,bar,baz\n1,2147483648,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64("2147483648"), baz = as.integer64(3))
  )

  # As well as -2^31
  test_vroom("foo,bar,baz\n1,9223372036854775807,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64("9223372036854775807"), baz = as.integer64(3))
  )

  # But 2^63 should be NA
  test_vroom("foo,bar,baz\n1,9223372036854775808,3\n", col_types = list(.default = "I"), delim = ",",
    equals = tibble::tibble(foo = as.integer64(1), bar = as.integer64(NA), baz = as.integer64(3))
  )
})
