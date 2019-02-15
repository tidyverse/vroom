test_vroom <- function(content, ..., equals) {
  expect_equivalent(
    vroom(content, ...),
    equals
  )

  if (!file.exists(content)) {
    tf <- tempfile()
    on.exit(unlink(tf))
    readr::write_lines(content, tf)

    con <- file(tf, "rb")
  } else {
    con <- file(content, "rb")
  }
  on.exit(close(con), add = TRUE)

  res <- vroom(con, ...)

  expect_equivalent(res, equals)

  ## Has a temp_file environment, with a filename
  #tf2 <- attr(res, "filename")
  #expect_true(is.character(tf2))
  #expect_true(file.exists(tf2))


  #rm(res)
  #gc()

  ## Which is removed after the object is deleted and the finalizer has run
  #expect_false(file.exists(tf2))
}

