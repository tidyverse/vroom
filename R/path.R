is_ascii_compatible <- function(encoding) {
  identical(iconv(list(charToRaw("\n")), from = "ASCII", to = encoding, toRaw = TRUE)[[1]], charToRaw("\n"))
}

reencode_path <- function(path, encoding) {
  if (length(path) > 1) {
    stop(sprintf("Reading files of encoding '%s' can only be done for single files at a time", encoding), call. = FALSE)
  }

  if (inherits(path[[1]], "connection")) {
    in_con <- path[[1]]
  } else {
    in_con <- file(path[[1]])
  }
  out_file <- tempfile()
  out_con <- file(out_file)
  convert_connection(in_con, out_con, encoding, "UTF-8")
  withr::defer(unlink(out_file), envir = parent.frame())
  return(list(out_file))
}

# These functions adapted from https://github.com/tidyverse/readr/blob/192cb1ca5c445e359f153d2259391e6d324fd0a2/R/source.R
standardise_path <- function(path) {
  if (is.raw(path)) {
    return(list(rawConnection(path, "rb")))
  }

  if (inherits(path, "connection")) {
    # If the connection is `stdin()`, change it to `file("stdin")`, as we don't
    # support text mode connections.

    if (path == stdin()) {
      return(list(file("stdin")))
    }

    return(list(path))
  }

  if (is.character(path)) {
    if (inherits(path, "AsIs")) {
      if (length(path) > 1) {
        path <- paste(path, collapse = "\n")
      }
      return(list(chr_to_file(path, envir = parent.frame())))
    }

    if (any(grepl("\n", path))) {
      lifecycle::deprecate_soft("1.5.0", "vroom(file = 'must use `I()` for literal data')",
        details = glue::glue('

          # Bad:
          vroom("foo\\nbar\\n")

          # Good:
          vroom(I("foo\\nbar\\n"))
        ')
      )
      return(list(chr_to_file(path, envir = parent.frame())))
    }
  }

  as.list(path)
}

standardise_one_path <- function (path, write = FALSE) {

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
      rlang::inform("`curl` package not installed, falling back to using `url()`")
      con <- url(path)
    }
    ext <- tolower(tools::file_ext(path))
    return(
      switch(ext,
        zip = ,
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

  p <- split_path_ext(basename(path))

  if (write) {
    path <- normalizePath(path, mustWork = FALSE)
  } else {
    path <- check_path(path)
  }

  if (rlang::is_installed("archive")) {
    formats <- archive_formats(p$extension)
    extension <- p$extension
    while(is.null(formats) && nzchar(extension)) {
      extension <- split_path_ext(extension)$extension
      formats <- archive_formats(extension)
    }
    if (!is.null(formats)) {
      p$extension <- extension
      if (write) {
        if (is.null(formats[[1]])) {
          return(archive::file_write(path, filter = formats[[2]]))
        }
        return(archive::archive_write(path, p$path, format = formats[[1]], filter = formats[[2]]))
      }
      if (is.null(formats[[1]])) {
        return(archive::file_read(path, filter = formats[[2]]))
      }
      return(archive::archive_read(path, format = formats[[1]], filter = formats[[2]]))
    }
  }

  if (!write) {
    compression <- detect_compression(path)
  } else {
    compression <- NA
  }

  if (is.na(compression)) {
    compression <- tools::file_ext(path)
  }

  if (write && compression == "zip") {
    stop("Can only read from, not write to, .zip", call. = FALSE)
  }

  switch(compression,
    gz = gzfile(path, ""),
    bz2 = bzfile(path, ""),
    xz = xzfile(path, ""),
    zip = zipfile(path, ""),
    if (!has_trailing_newline(path)) {
      file(path)
    } else {
      path
    }
  )
}

split_path_ext <- function(path) {
  regex <- "^([^.]*)[.](.*)"
  res <- regexpr(regex, path, perl = TRUE)
  if (length(res) == 0 || res == -1) {
    return(list(path = path, extension = ""))
  }
  starts <- attr(res, "capture.start")[1, ]
  lengths <- attr(res, "capture.length")[, ]
  list(
    path = substr(path, starts[[1]], starts[[1]] + lengths[[1]] - 1),
    extension = substr(path, starts[[2]], starts[[2]] + lengths[[2]] - 1)
  )
}

# Adapted from archive:::format_and_filter_by_extension
# https://github.com/r-lib/archive/blob/125f9930798dc20fa12cda30319ca3e9a134a409/R/archive.R#L73
archive_formats <- function(ext) {
  switch(ext,
    "7z" = list("7zip", "none"),

    "cpio" = list("cpio", "none"),

    "iso" = list("iso9660", "none"),

    "mtree" = list("mtree", "none"),

    "tar" = list("tar", "none"),

    "tgz" = list("tar", "gzip"),
    "taz" = list("tar", "gzip"),
    "tar.gz" = list("tar", "gzip"),

    "tbz" = list("tar", "bzip2"),
    "tbz2" = list("tar", "bzip2"),
    "tz2" = list("tar", "bzip2"),
    "tar.bz2" = list("tar", "bzip2"),

    "tlz" = list("tar", "lzma"),
    "tar.lzma" = list("tar", "lzma"),

    "txz" = list("tar", "xz"),
    "tar.xz" = list("tar", "xz"),

    "tzo" = list("tar", "lzop"),

    "taZ" = list("tar", "compress"),
    "tZ" = list("tar", "compress"),

    "tar.zst"= list("tar", "zstd"),

    "warc" = list("warc", "none"),

    "jar" = list("zip", "none"),
    "zip" = list("zip", "none"),

    "Z" = list(NULL, "compress"),

    "zst" = list(NULL, "zst"),

    NULL)
}

is_url <- function(path) {
  grepl("^((http|ftp)s?|sftp)://", path)
}

check_path <- function(path) {
  if (file.exists(path))
    return(normalizePath(path, "/", mustWork = FALSE))

  stop("'", path, "' does not exist",
    if (!is_absolute_path(path)) {
      paste0(" in current working directory ('", getwd(), "')")
    },
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
    rlang::inform(paste0("Multiple files in zip: reading '", file, "'"))
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

detect_compression <- function(path) {
  bytes <- readBin(path, "raw", n = 6)
  if (length(bytes) >= 2 && bytes[[1]] == 0x1f && bytes[[2]] == 0x8b) {
    return("gz")
  }
  if (length(bytes) >= 6 &&
    bytes[[1]] == 0xFD &&
    bytes[[2]] == 0x37 &&
    bytes[[3]] == 0x7A &&
    bytes[[4]] == 0x58 &&
    bytes[[5]] == 0x5A &&
    bytes[[6]] == 0x00) {
    return("xz")
  }

  if (length(bytes) >= 3 &&
    bytes[[1]] == 0x42 &&
    bytes[[2]] == 0x5a &&
    bytes[[3]] == 0x68) {
    return("bz2")
  }

  # normal zip
  if (length(bytes) >= 4 &&
    bytes[[1]] == 0x50 &&
    bytes[[2]] == 0x4B &&
    bytes[[3]] == 0x03 &&
    bytes[[4]] == 0x04) {
    return("zip")
  }

  # empty zip
  if (length(bytes) >= 4 &&
    bytes[[1]] == 0x50 &&
    bytes[[2]] == 0x4B &&
    bytes[[3]] == 0x05 &&
    bytes[[4]] == 0x06) {
    return("zip")
  }

  # spanned zip
  if (length(bytes) >= 4 &&
    bytes[[1]] == 0x50 &&
    bytes[[2]] == 0x4B &&
    bytes[[3]] == 0x07 &&
    bytes[[4]] == 0x08) {
    return("zip")
  }

  NA_character_
}
