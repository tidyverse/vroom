test_that("multi-byte reading works with unicode delimiters and UTF-8 encoding", {
  # libvroom does not yet support multi-character delimiters like "||";
  # it treats each "|" as a separate delimiter, producing extra columns.
  skip("libvroom does not yet support multi-character delimiters")
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

test_that("multi-byte reading works with unicode delimiters and UTF-8 encoding", {
  skip_on_os("solaris")

  # libvroom does not yet support multi-byte Unicode delimiters.
  skip("libvroom does not yet support multi-byte Unicode delimiters")
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
