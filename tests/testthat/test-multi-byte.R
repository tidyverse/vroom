test_that("multi-byte reading works with unicode delimiters and UTF-8 encoding", {
  test_vroom(test_path("multi-byte-ascii.txt"), delim = "||",
    equals = tibble::tibble(id = 1:3, name = c("ed", "leigh", "nathan"), age = c(36, NA, 14))
  )
})

test_that("multi-byte reading works with unicode delimiters and UTF-8 encoding", {
  skip_on_os("solaris")

  test_vroom(test_path("multi-byte-unicode.txt"), delim = "\U2764",
    equals = tibble::tibble(id = 1:3, name = c("ed", "leigh", "nathan"), age = c(36, NA, 14))
  )
})

test_that("we can read multi-byte file names", {

  expected <- tibble::tibble(
    id = 1:3,
    name = c("ed", "leigh", "nathan"),
    age = c(36, NA, 14)
  )

  # use the filename 'crème-brûlée'
  source <- test_path("multi-byte-unicode.txt")
  target <- paste(tempdir(), "cr\u00e8me-br\u00fbl\u00e9e.txt", sep = "/")
  file.copy(source, target)

  # test with the filename as UTF-8
  test_vroom(enc2utf8(target), delim = "\U2764", equals = expected)

  # only run the test if we can represent the filename in the native encoding
  if (target == enc2native(target)) {
    test_vroom(enc2native(target), delim = "\U2764", equals = expected)
  }

})
