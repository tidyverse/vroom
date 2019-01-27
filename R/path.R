standardise_path <- function(path) {
  if (inherits(path, "connection")) {
    return(list(path))
  }

  lapply(path, standardise_one_path, envir = parent.frame())
}

standardise_one_path <-function (path, envir = parent.frame()) {
  if (!is.character(path)) {
    return(path)
  }
  if (grepl("\n", path)) {
    return(chr_to_file(sub("\n$", "", path), envir = envir))
  }
  path.expand(path)
}

utils::globalVariables("con")

chr_to_file <- function(x, envir = parent.frame()) {
  out <- tempfile()
  con <- file(out, "wb")
  writeLines(x, con)
  close(con)

  withr::defer(unlink(out), envir = envir)

  normalizePath(out)
}
