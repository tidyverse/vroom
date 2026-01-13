vroom_tempfile <- function(fileext = "", pattern = "vroom-") {
  dir <- Sys.getenv("VROOM_TEMP_PATH")
  if (!nzchar(dir)) {
    dir <- tempdir()
  }
  if (nzchar(fileext) && !startsWith(fileext, ".")) {
    fileext <- paste0(".", fileext)
  }
  tempfile(pattern = pattern, tmpdir = dir, fileext = fileext)
}

is_ascii_compatible <- function(encoding) {
  identical(
    iconv(list(charToRaw("\n")), from = "ASCII", to = encoding, toRaw = TRUE)[[
      1
    ]],
    charToRaw("\n")
  )
}

# this is about the encoding of the file (contents), not the filepath
reencode_file <- function(file, encoding, call = caller_env()) {
  if (length(file) > 1) {
    cli::cli_abort(
      c(
        "!" = "Encoding {.val {encoding}} is only supported when reading a single input.",
        "i" = "{.arg file} has length {length(file)}."
      ),
      call = call
    )
  }

  if (inherits(file[[1]], "connection")) {
    in_con <- file[[1]]
  } else {
    in_con <- file(file[[1]])
  }
  out_file <- vroom_tempfile(pattern = "vroom-reencode-file-")
  out_con <- file(out_file)
  convert_connection(in_con, out_con, encoding, "UTF-8")
  withr::defer(unlink(out_file), envir = parent.frame())
  return(list(out_file))
}

# These functions adapted from https://github.com/tidyverse/readr/blob/192cb1ca5c445e359f153d2259391e6d324fd0a2/R/source.R
standardise_path <- function(
  path,
  arg = caller_arg(path),
  call = caller_env(),
  user_env = caller_env(2)
) {
  if (is.raw(path)) {
    return(list(rawConnection(path, "rb")))
  }

  if (inherits(path, "connection")) {
    return(list(standardise_connection(path)))
  }

  if (is_list(path)) {
    is_connection <- vapply(
      path,
      function(x) inherits(x, "connection"),
      logical(1)
    )
    if (all(is_connection)) {
      return(lapply(path, standardise_connection))
    }
    if (any(is_connection)) {
      cli::cli_abort(
        "{.arg {arg}} cannot be a mix of connection and non-connection inputs",
        call = call
      )
    }
  }

  if (!is.character(path)) {
    cli::cli_abort(
      c(
        "{.arg {arg}} must be one of the supported input types:",
        "*" = "A filepath or character vector of filepaths",
        "*" = "A connection or list of connections",
        "*" = "Literal or raw input",
        "x" = "{.arg {arg}} is {obj_type_friendly(path)}."
      ),
      call = call
    )
  }

  if (inherits(path, "AsIs")) {
    if (length(path) > 1) {
      path <- paste(path, collapse = "\n")
    }
    return(list(chr_to_file(path, envir = parent.frame())))
  }

  if (any(grepl("\n", path))) {
    lifecycle::deprecate_warn(
      "1.5.0",
      "vroom(file = 'must use `I()` for literal data')",
      details = c(
        " " = "",
        " " = "# Bad:",
        " " = 'vroom("X,Y\\n1.5,2.3\\n")',
        " " = "",
        " " = "# Good:",
        " " = 'vroom(I("X,Y\\n1.5,2.3\\n"))'
      ),
      user_env = user_env
    )
    return(list(chr_to_file(path, envir = parent.frame())))
  }

  as.list(enc2utf8(path))
}

standardise_connection <- function(con) {
  # If the connection is `stdin()`, change it to `file("stdin")`, as we don't
  # support text mode connections.

  if (con == stdin()) {
    return(file("stdin"))
  }

  con
}

connection_or_filepath <- function(path, write = FALSE, call = caller_env()) {
  if (is.raw(path)) {
    return(rawConnection(path, "rb"))
  }

  if (!is.character(path)) {
    return(path)
  }

  if (is_url(path)) {
    ext <- tolower(tools::file_ext(path))

    # Download remote compressed files to a temporary local file. This gives
    # consistent handling across formats and avoids bugs / undesirable behaviour
    # of base::gzcon().
    # https://github.com/tidyverse/vroom/issues/400
    # https://github.com/tidyverse/vroom/issues/553
    if (ext %in% c("gz", "bz2", "xz", "zip")) {
      local_path <- download_url(path, ext, call = call)
      withr::defer(unlink(local_path), envir = parent.frame())
      return(
        switch(
          ext,
          gz = gzfile(local_path, ""),
          bz2 = bzfile(local_path, ""),
          xz = xzfile(local_path, ""),
          zip = zipfile(local_path, "")
        )
      )
    }

    # Non-compressed URLs: return connection directly
    if (requireNamespace("curl", quietly = TRUE)) {
      con <- curl::curl(path)
    } else {
      inform("`curl` package not installed, falling back to using `url()`")
      con <- url(path)
    }
    return(con)
  }

  path <- enc2utf8(path)

  p <- split_path_ext(basename_utf8(path))

  if (write) {
    path <- normalizePath_utf8(path, mustWork = FALSE)
  } else {
    path <- check_path(path)
  }

  if (is_installed("archive")) {
    formats <- archive_formats(p$extension)
    extension <- p$extension
    while (is.null(formats) && nzchar(extension)) {
      extension <- split_path_ext(extension)$extension
      formats <- archive_formats(extension)
    }
    if (!is.null(formats)) {
      if (write) {
        if (is.null(formats[[1]])) {
          return(archive::file_write(path, filter = formats[[2]]))
        }
        return(archive::archive_write(
          path,
          p$path,
          format = formats[[1]],
          filter = formats[[2]]
        ))
      }
      if (is.null(formats[[1]])) {
        return(archive::file_read(path, filter = formats[[2]]))
      }
      return(archive::archive_read(
        path,
        format = formats[[1]],
        filter = formats[[2]]
      ))
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
    cli::cli_abort(
      c(
        "Can only read from, not write to, {.val .zip} files.",
        "i" = "Install the {.pkg archive} package to write {.val .zip} files."
      ),
      call = call
    )
  }

  switch(
    compression,
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

# Safe wrapper around base::open() that ensures connections are cleaned up
# even when open() fails. We need this especially for opening connections from
# C++. If base::open() fails in that context, R will long jump and there aren't
# good options for arranging the necessary cleanup.
open_safely <- function(con, open_mode = "rb") {
  tryCatch(
    open(con, open_mode),
    error = function(e) {
      # The failed connection is already "closed", so this attempt to close() it
      # is really about removing it from R's connection table.
      try(close(con), silent = TRUE)

      stop(conditionMessage(e), call. = FALSE)
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
  lengths <- attr(res, "capture.length")[,]
  list(
    path = substr(path, starts[[1]], starts[[1]] + lengths[[1]] - 1),
    extension = substr(path, starts[[2]], starts[[2]] + lengths[[2]] - 1)
  )
}

# Adapted from archive:::format_and_filter_by_extension
# https://github.com/r-lib/archive/blob/125f9930798dc20fa12cda30319ca3e9a134a409/R/archive.R#L73
archive_formats <- function(ext) {
  switch(
    ext,
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

    "tar.zst" = list("tar", "zstd"),

    "warc" = list("warc", "none"),

    "jar" = list("zip", "none"),
    "zip" = list("zip", "none"),

    "Z" = list(NULL, "compress"),

    "zst" = list(NULL, "zst"),

    NULL
  )
}

is_url <- function(path) {
  grepl("^((http|ftp)s?|sftp)://", path)
}

download_url <- function(url, ext, call = caller_env()) {
  local_path <- vroom_tempfile(fileext = ext, pattern = "vroom-download-url-")
  show_progress <- vroom_progress()

  try_fetch(
    if (requireNamespace("curl", quietly = TRUE)) {
      curl::curl_download(url, local_path, quiet = !show_progress)
    } else {
      utils::download.file(url, local_path, mode = "wb", quiet = !show_progress)
    },
    error = function(cnd) {
      unlink(local_path)
      cli::cli_abort(
        c(
          "Failed to download {.url {url}}.",
          "x" = conditionMessage(cnd)
        ),
        parent = NA,
        error = cnd,
        call = call
      )
    }
  )

  local_path
}

check_path <- function(path, call = caller_env()) {
  if (file.exists(path)) {
    return(normalizePath_utf8(path, mustWork = FALSE))
  }

  where <- function(path) {
    if (is_absolute_path(path)) {
      ""
    } else {
      " in current working directory: {.file {getwd()}}"
    }
  }
  msg <- glue(
    "{.file {path}} does not exist<<where(path)>>.",
    .open = "<<",
    .close = ">>"
  )
  cli::cli_abort(msg, call = call)
}

is_absolute_path <- function(path) {
  grepl("^(/|[A-Za-z]:|\\\\|~)", path)
}

zipfile <- function(path, open = "r") {
  files <- utils::unzip(path, list = TRUE)
  file <- files$Name[[1]]

  if (nrow(files) > 1) {
    inform(paste0("Multiple files in zip: reading '", file, "'"))
  }

  unz(path, file, open = open)
}

utils::globalVariables("con")

chr_to_file <- function(x, envir = parent.frame()) {
  out <- vroom_tempfile(pattern = "vroom-chr-to-file-")
  con <- file(out, "wb")
  writeLines(sub("\n$", "", x), con, useBytes = TRUE)
  close(con)

  withr::defer(unlink(out), envir = envir)

  normalizePath_utf8(out)
}

detect_compression <- function(path) {
  bytes <- readBin(path, "raw", n = 6)
  if (length(bytes) >= 2 && bytes[[1]] == 0x1f && bytes[[2]] == 0x8b) {
    return("gz")
  }
  if (
    length(bytes) >= 6 &&
      bytes[[1]] == 0xFD &&
      bytes[[2]] == 0x37 &&
      bytes[[3]] == 0x7A &&
      bytes[[4]] == 0x58 &&
      bytes[[5]] == 0x5A &&
      bytes[[6]] == 0x00
  ) {
    return("xz")
  }

  if (
    length(bytes) >= 3 &&
      bytes[[1]] == 0x42 &&
      bytes[[2]] == 0x5a &&
      bytes[[3]] == 0x68
  ) {
    return("bz2")
  }

  # normal zip
  if (
    length(bytes) >= 4 &&
      bytes[[1]] == 0x50 &&
      bytes[[2]] == 0x4B &&
      bytes[[3]] == 0x03 &&
      bytes[[4]] == 0x04
  ) {
    return("zip")
  }

  # empty zip
  if (
    length(bytes) >= 4 &&
      bytes[[1]] == 0x50 &&
      bytes[[2]] == 0x4B &&
      bytes[[3]] == 0x05 &&
      bytes[[4]] == 0x06
  ) {
    return("zip")
  }

  # spanned zip
  if (
    length(bytes) >= 4 &&
      bytes[[1]] == 0x50 &&
      bytes[[2]] == 0x4B &&
      bytes[[3]] == 0x07 &&
      bytes[[4]] == 0x08
  ) {
    return("zip")
  }

  NA_character_
}

basename_utf8 <- function(path) {
  enc2utf8(basename(path))
}

normalizePath_utf8 <- function(path, winslash = "/", mustWork = NA) {
  enc2utf8(normalizePath(path, winslash = winslash, mustWork = mustWork))
}
