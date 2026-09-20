# Encoding ----------------------------------------------------------------

test_that("locale encoding affects parsing", {
  x <- c("ao\u00FBt", "\u00E9l\u00E8ve", "\u00E7a va")
  #expect_equal(Encoding(x), rep("UTF-8", 3))

  y <- iconv(paste0(x, collapse = "\n"), "UTF-8", "latin1")
  #expect_equal(Encoding(y), "latin1")

  fr <- locale("fr", encoding = "latin1")
  z <- vroom(
    I(y),
    delim = ",",
    locale = fr,
    col_names = FALSE,
    col_types = list()
  )
  # expect_equal(Encoding(z[[1]]), rep("UTF-8", 3))

  # identical coerces encodings to match, so need to compare raw values
  as_raw <- function(x) lapply(x, charToRaw)
  expect_identical(as_raw(x), as_raw(z[[1]]))
})

test_that("encodings are respected", {
  loc <- locale(encoding = "ISO-8859-1")
  expected <- c("fran\u00e7ais", "\u00e9l\u00e8ve")

  x <- vroom(
    test_path("enc-iso-8859-1.txt"),
    delim = "\n",
    locale = loc,
    col_names = FALSE,
    col_types = list()
  )
  expect_equal(x[[1]], expected)
})

test_that("shorter UTF-8 output does not report an embedded null", {
  path <- withr::local_tempfile()
  bytes <- iconv("\u00c9", "UTF-8", "GB18030", toRaw = TRUE)[[1]]
  writeBin(c(charToRaw("x\n"), bytes, charToRaw("\n")), path)

  for (altrep in c(FALSE, TRUE)) {
    out <- suppressWarnings(vroom(
      path,
      delim = ",",
      col_types = "c",
      locale = locale(encoding = "GB18030"),
      altrep = altrep
    ))
    value <- suppressWarnings(out$x[[1]])
    probs <- suppressWarnings(problems(out))

    expect_equal(value, "\u00c9")
    expect_equal(nrow(probs), 0)
  }
})

test_that("character columns report actual embedded nulls", {
  path <- withr::local_tempfile()
  writeBin(c(charToRaw("x\na"), as.raw(0), charToRaw("b\n")), path)

  for (altrep in c(FALSE, TRUE)) {
    out <- suppressWarnings(vroom(
      path,
      delim = ",",
      col_types = "c",
      altrep = altrep
    ))
    value <- suppressWarnings(out$x[[1]])
    probs <- suppressWarnings(problems(out))

    expect_equal(value, "a")
    expect_equal(probs$actual, "embedded null")
  }
})
