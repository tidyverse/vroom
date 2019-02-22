test_vroom <- function(content, ..., equals) {
  expect_equal(
    vroom(content, ...),
    equals
  )

  withr::with_options(c("vroom.use_altrep" = FALSE), {
    expect_equal(
      vroom(content, ...),
      equals
    )
  })

  if (!file.exists(content)) {
    tf <- tempfile()
    on.exit(unlink(tf))
    readr::write_lines(content, tf, sep = "")

    con <- file(tf, "rb")
  } else {
    con <- file(content, "rb")
  }
  on.exit(close(con), add = TRUE)

  res <- vroom(con, ...)

  expect_equal(res, equals)
  for (i in seq_along(res)) {
    force_materialization(res[[i]])
  }
  expect_equal(res, equals)

  res
}
