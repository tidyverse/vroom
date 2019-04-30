# These functions adapted from https://github.com/tidyverse/readr/blob/192cb1ca5c445e359f153d2259391e6d324fd0a2/R/source.R
standardise_path <- function(path) {
  if (is.raw(path)) {
    return(list(rawConnection(path, "rb")))
  }

  if (inherits(path, "connection")) {
    return(list(path))
  }

  if (is.character(path) && any(grepl("\n", path))) {
    return(list(chr_to_file(path, envir = parent.frame())))
  }

  as.list(path)
}

standardise_one_path <- function (path, check = TRUE) {
  if (is.raw(path)) {
    return(rawConnection(path, "rb"))
  }

  if (!is.character(path)) {
    return(path)
  }

  if (is_url(path)) {
    if (requireNamespace("curl", quietly = TRUE)) {
      con <- curl::curl(path)
    } else {
      message("`curl` package not installed, falling back to using `url()`")
      con <- url(path)
    }
    ext <- tolower(tools::file_ext(path))
    return(
      switch(ext,
        bz2 = ,
        xz = {
          close(con)
          stop("Reading from remote `", ext, "` compressed files is not supported,\n",
            "  download the files locally first.", call. = FALSE)
        },
        gz = gzcon(con),
        con
      )
    )
  }

  if (check) {
    path <- check_path(path)
  } else {
    path <- normalizePath(path, mustWork = FALSE)
  }

  switch(tolower(tools::file_ext(path)),
    gz = gzfile(path, ""),
    bz2 = bzfile(path, ""),
    xz = xzfile(path, ""),
    zip = zipfile(path, ""),
    path
  )
}

is_url <- function(path) {
  grepl("^((http|ftp)s?|sftp)://", path)
}

check_path <- function(path) {
  if (file.exists(path))
    return(normalizePath(path, "/", mustWork = FALSE))

  stop("'", path, "' does not exist",
    if (!is_absolute_path(path))
      paste0(" in current working directory ('", getwd(), "')"),
    ".",
    call. = FALSE
  )
}

is_absolute_path <- function(path) {
  grepl("^(/|[A-Za-z]:|\\\\|~)", path)
}

zipfile <- function(path, open = "r") {
  files <- utils::unzip(path, list = TRUE)
  file <- files$Name[[1]]

  if (nrow(files) > 1) {
    message("Multiple files in zip: reading '", file, "'")
  }

  unz(path, file, open = open)
}

utils::globalVariables("con")

chr_to_file <- function(x, envir = parent.frame()) {
  out <- tempfile()
  con <- file(out, "wb")
  writeLines(sub("\n$", "", x), con, useBytes = TRUE)
  close(con)

  withr::defer(unlink(out), envir = envir)

  normalizePath(out)
}
