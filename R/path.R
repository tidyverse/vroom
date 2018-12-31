standardise_path <-function (path) {
  if (!is.character(path)) {
    return(path)
  }
  if (length(path) > 1) {
    return(chr_to_file(path, parent.frame()))
  }
  if (grepl("\n", path)) {
    return(chr_to_file(sub("\n$", "", path), parent.frame()))
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
