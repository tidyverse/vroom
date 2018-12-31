standardise_path <-function (path) {
  if (length(path) > 1) {
    return(chr_to_file(path, parent.frame()))
  }
  if (grepl("\n", path)) {
    return(chr_to_file(sub("\n$", "", path), parent.frame()))
  }
  path.expand(path)
}

chr_to_file <- function(x, envir = parent.frame()) {
  out <- tempfile()
  withr::with_connection(list(con = file(out, "wb")),
    writeLines(x,con)
  )

  withr::defer(unlink(out), envir = envir)

  normalizePath(out)
}
