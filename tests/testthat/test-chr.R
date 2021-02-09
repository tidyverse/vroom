# Encoding ----------------------------------------------------------------

test_that("locale encoding affects parsing", {
  x <- c("ao\u00FBt", "\u00E9l\u00E8ve", "\u00E7a va")
  #expect_equal(Encoding(x), rep("UTF-8", 3))

  y <- iconv(paste0(x, collapse = "\n"), "UTF-8", "latin1")
  #expect_equal(Encoding(y), "latin1")

  fr <- locale("fr", encoding = "latin1")
  z <- vroom(I(y), delim = ",", locale = fr, col_names = FALSE, col_types = list())
  # expect_equal(Encoding(z[[1]]), rep("UTF-8", 3))

  # identical coerces encodings to match, so need to compare raw values
  as_raw <- function(x) lapply(x, charToRaw)
  expect_identical(as_raw(x), as_raw(z[[1]]))
})

test_that("encodings are respected", {
  loc <- locale(encoding = "ISO-8859-1")
  expected <- c("fran\u00e7ais", "\u00e9l\u00e8ve")

  x <- vroom(test_path("enc-iso-8859-1.txt"), delim = "\n", locale = loc, col_names = FALSE, col_types = list())
  expect_equal(x[[1]], expected)
})
