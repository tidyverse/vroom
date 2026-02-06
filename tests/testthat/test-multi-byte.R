test_that("multi-character delimiters work", {
  test_vroom(
    test_path("multi-byte-ascii.txt"),
    delim = "||",
    equals = tibble::tibble(
      id = 1:3,
      name = c("ed", "leigh", "nathan"),
      age = c(36, NA, 14)
    )
  )
})

test_that("quoted fields with multi-character delimiters work", {
  test_vroom(
    I('a||b\n"hello||world"||other\n'),
    delim = "||",
    equals = tibble::tibble(
      a = "hello||world",
      b = "other"
    )
  )
})

test_that("multi-byte unicode delimiters work", {
  skip_on_os("solaris")

  test_vroom(
    test_path("multi-byte-unicode.txt"),
    delim = "\U2764",
    equals = tibble::tibble(
      id = 1:3,
      name = c("ed", "leigh", "nathan"),
      age = c(36, NA, 14)
    )
  )
})
