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

test_that("ALTREP character elements stay alive across allocations", {
  path <- withr::local_tempfile()
  # Use raw bytes so no ordinary character vector keeps the input string alive.
  writeBin(
    as.raw(c(0x69L, rep.int(0x30L, 6L), 0x31L, 0x0aL)),
    path
  )
  input <- vroom(
    path,
    delim = ",",
    col_names = FALSE,
    col_types = cols(X1 = col_character()),
    altrep = "chr",
    num_threads = 1L,
    progress = FALSE,
    show_col_types = FALSE
  )[[1L]]

  target_value <- rawToChar(as.raw(c(0xe9L, rep.int(0x78L, 6L))))
  Encoding(target_value) <- "latin1"
  target <- rep.int(target_value, 256L)

  gctorture(TRUE)
  withr::defer(gctorture(FALSE))
  first <- charmatch(input, target, nomatch = NA_integer_)
  second <- charmatch(input, target, nomatch = NA_integer_)
  gctorture(FALSE)

  expect_identical(first, NA_integer_)
  expect_identical(second, NA_integer_)
})

test_that("ALTREP character problems report the correct row", {
  path <- withr::local_tempfile()
  writeBin(
    as.raw(c(
      0x61L, 0x0aL,
      0x62L, 0x0aL,
      0x63L, 0x00L, 0x64L, 0x0aL
    )),
    path
  )

  x <- vroom(
    path,
    delim = ",",
    col_names = FALSE,
    col_types = "c",
    altrep = "chr",
    num_threads = 1L,
    progress = FALSE,
    show_col_types = FALSE
  )

  expect_warning(x[[1L]][[3L]], class = "vroom_parse_issue")

  # The problem is recorded correctly before materialization.
  expect_equal(problems(x, lazy = TRUE)$row, 3)

  # `problems()` materializes by default. Cached values must not cause the
  # incorrect row to reappear or the correct row to be lost.
  expect_equal(problems(x)$row, 3)
})

test_that("ALTREP character Elt survives re-entrant materialization", {
  skip_on_os("windows")

  path <- withr::local_tempfile()
  writeBin(
    as.raw(c(
      0x61L, 0x0aL,
      0x62L, 0x0aL,
      0x63L, 0x00L, 0x64L, 0x0aL
    )),
    path
  )

  x <- vroom(
    path,
    delim = ",",
    col_names = FALSE,
    col_types = "c",
    altrep = "chr",
    num_threads = 1L,
    progress = FALSE,
    show_col_types = FALSE
  )
  col <- x[[1L]]

  value <- withCallingHandlers(
    col[[3L]],
    vroom_parse_issue = function(cnd) {
      # Change the mapped bytes before materializing from the warning handler.
      con <- file(path, "r+b")
      seek(con, where = 4L, origin = "start", rw = "write")
      writeBin(charToRaw("xyz"), con)
      close(con)

      problems(x)
      invokeRestart("muffleWarning")
    }
  )

  # The in-flight Elt() returns the value parsed before re-entry, while the
  # materialized vector reflects the modified backing bytes.
  expect_identical(value, "c")
  expect_identical(col[[3L]], "xyz")
})

test_that("partially cached ALTREP character vectors materialize correctly", {
  n <- 2500L
  expected <- sprintf("v%04d", seq_len(n))
  expected[c(3L, 1025L)] <- ""
  expected[c(4L, 1026L)] <- NA_character_

  path <- withr::local_tempfile()
  # Quote the empty strings so that they are not skipped as empty rows.
  lines <- ifelse(expected == "", '""', expected)
  writeLines(ifelse(is.na(lines), "NA", lines), path)

  x <- vroom(
    path,
    delim = ",",
    col_names = FALSE,
    col_types = "c",
    na = "NA",
    altrep = "chr",
    num_threads = 1L,
    progress = FALSE,
    show_col_types = FALSE
  )[[1L]]

  # Touch the first, middle and last chunks, including the empty and NA
  # elements, so that materializing has to merge cached and uncached chunks.
  idx <- c(1L, 3L, 4L, 1024L, 1025L, 1026L, 2049L, n)
  expect_identical(vapply(idx, function(i) x[[i]], ""), expected[idx])

  vroom_materialize(list(x), replace = FALSE)
  expect_identical(x, expected)
})
