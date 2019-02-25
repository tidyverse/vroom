context("test-multi-byte.R")

test_that("multi-byte reading works with unicode delimiters and UTF-8 encoding", {
  test_vroom(test_path("multi-byte-ascii.txt"), delim = "||",
    equals = tibble::tibble(id = 1:3, name = c("ed", "leigh", "nathan"), age = c(36, NA, 14))
  )
})

test_that("multi-byte reading works with unicode delimiters and UTF-8 encoding", {
  test_vroom(test_path("multi-byte-unicode.txt"), delim = "\U2764",
    equals = tibble::tibble(id = 1:3, name = c("ed", "leigh", "nathan"), age = c(36, NA, 14))
  )
})
