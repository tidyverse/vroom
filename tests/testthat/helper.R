test_vroom <- function(content, ..., equals) {
  # with altrep
  withr::with_envvar(c("VROOM_USE_ALTREP_CHR" = "true", "VROOM_USE_ALTREP_NUMERICS" = "true"), {
    expect_equal(
      vroom(content, ...),
      equals
    )
  })

  # without altrep
  withr::with_envvar(c("VROOM_USE_ALTREP_CHR" = "false", "VROOM_USE_ALTREP_NUMERICS" = "false"), {
    expect_equal(
      vroom(content, ...),
      equals
    )
  })

  if (!file.exists(content)) {
    tf <- tempfile()
    on.exit(unlink(tf))
    out_con <- file(tf, "wb")
    writeLines(content, out_con, sep = "")
    close(out_con)

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

  invisible(res)
}

test_parse_number <- function(x, expected, ...) {
  test_vroom(paste0(paste0(x, collapse = "\n"), "\n"), delim = "\n",
    col_names = FALSE, col_types = "n", ...,
    equals = tibble::tibble(X1 = expected)
  )
}

test_parse_datetime <- function(x, format, expected, ...) {
  test_vroom(paste0(paste0(x, collapse = "\n"), "\n"), delim = "\n",
    col_names = FALSE, col_types = cols(X1 = col_datetime(format = format)), ...,
    equals = tibble::tibble(X1 = expected)
  )
}

test_parse_date <- function(x, format, expected, ...) {
  test_vroom(paste0(paste0(x, collapse = "\n"), "\n"), delim = "\n",
    col_names = FALSE, col_types = cols(X1 = col_date(format = format)), ...,
    equals = tibble::tibble(X1 = expected)
  )
}
