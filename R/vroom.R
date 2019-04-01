#' @useDynLib vroom, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' Read a delimited file into a tibble
#'
#' @inheritParams readr::read_delim
#' @param file path to a local file.
#' @param delim One of more characters used to delimiter fields within a
#'   record. If `NULL` the delimiter is guessed from the set of (",", "\\t", " ",
#'   "|", ":", ";", "\\n").
#' @param num_threads Number of threads to use when reading and materializing vectors.
#' @param escape_double Does the file escape quotes by doubling them?
#'   i.e. If this option is `TRUE`, the value `""` represents
#'   a single quote, `"`.
#' @param id Either a string or 'NULL'. If a string, the output will contain a
#'   variable with that name with the filename(s) as the value. If 'NULL', the
#'   default, no variable will be created.
#' @param col_keep Columns to keep in the output, all other columns will be
#'   skipped. Input can be a character vector of column names, a logical vector
#'   or a numeric vector of column indexes. Only one of `col_keep` or
#'   `col_drop` can be used.
#' @param col_skip Columns to skip in the output, all other columns will be
#'   kept. Input can be a character vector of column names, a logical vector
#'   or a numeric vector of column indexes. Only one of `col_keep` or
#'   `col_drop` can be used.
#' @param .name_repair Handling of column names. By default, vroom ensures
#'   column names are not empty and unique. See `.name_repair` as documented in
#'   [tibble::tibble()] for additional options including supplying user defined
#'   name repair functions.
#' @export
#' @examples
#' \dontshow{
#' .old_wd <- setwd(tempdir())
#' }
#'
#' readr::write_tsv(mtcars, "mtcars.tsv")
#' vroom("mtcars.tsv")
#'
#' \dontshow{
#' unlink("mtcars.tsv")
#' setwd(.old_wd)
#' }
vroom <- function(file, delim = NULL, col_names = TRUE, col_types = NULL,
  col_keep = NULL, col_skip = NULL, id = NULL, skip = 0, n_max = Inf,
  na = c("", "NA"), quote = '"', comment = "", trim_ws = TRUE,
  escape_double = TRUE, escape_backslash = FALSE, locale = readr::default_locale(),
  guess_max = 100, num_threads = vroom_threads(), progress = vroom_progress(),
  .name_repair = "unique") {

  if (!is.null(col_keep) && !is.null(col_skip)) {
    stop("Only one of `col_keep` and `col_skip` can be set", call. = FALSE)
  }

  file <- standardise_path(file)

  if (length(file) == 0 || (n_max == 0 & identical(col_names, FALSE))) {
    return(tibble::tibble())
  }

  if (n_max < 0 || is.infinite(n_max)) {
    n_max <- -1
  }

  # Workaround weird RStudio / Progress bug: https://github.com/r-lib/progress/issues/56#issuecomment-384232184
  if (
    isTRUE(progress) &&
    is_windows() &&
    identical(Sys.getenv("RSTUDIO"), "1")) {
    Sys.setenv("RSTUDIO" = "1")
  }

  out <- vroom_(file, delim = delim, col_names = col_names, col_types = col_types,
    col_keep = col_keep, col_skip = col_skip, id = id, skip = skip,
    na = na, quote = quote, trim_ws = trim_ws, escape_double = escape_double,
    escape_backslash = escape_backslash, comment = comment, locale = locale,
    guess_max = guess_max, n_max = n_max, altrep_opts = vroom_altrep_opts(),
    num_threads = num_threads, progress = progress)

  tibble::as_tibble(out, .name_repair = .name_repair)
}

#' Guess the type of a vector
#'
#' @inheritParams readr::guess_parser
guess_type <- function(x, na = c("", "NA"), locale = readr::default_locale(), guess_integer = FALSE) {

  x[x %in% na] <- NA

  type <- readr::guess_parser(x, locale = locale, guess_integer = guess_integer)
  get(paste0("col_", type), asNamespace("readr"))()
}

col_types_standardise <- function(col_types, col_names, col_keep = NULL, col_skip = NULL) {
  spec <- readr::as.col_spec(col_types)
  type_names <- names(spec$cols)

  if (length(spec$cols) == 0) {
    # no types specified so use defaults

    spec$cols <- rep(list(spec$default), length(col_names))
    names(spec$cols) <- col_names
  } else if (is.null(type_names)) {
    # unnamed types & names guessed from header: match exactly

    if (length(spec$cols) != length(col_names)) {
      warning("Unnamed `col_types` should have the same length as `col_names`. ",
        "Using smaller of the two.", call. = FALSE)
      n <- min(length(col_names), length(spec$cols))
      spec$cols <- spec$cols[seq_len(n)]
      col_names <- col_names[seq_len(n)]
    }

    names(spec$cols) <- col_names
  } else {
    # names types

    bad_types <- !(type_names %in% col_names)
    if (any(bad_types)) {
      warning("The following named parsers don't match the column names: ",
        paste0(type_names[bad_types], collapse = ", "), call. = FALSE)
      spec$cols <- spec$cols[!bad_types]
      type_names <- type_names[!bad_types]
    }

    default_types <- !(col_names %in% type_names)
    if (any(default_types)) {
      defaults <- rep(list(spec$default), sum(default_types))
      names(defaults) <- col_names[default_types]
      spec$cols[names(defaults)] <- defaults
    }

    spec$cols <- spec$cols[col_names]
  }

  if (!is.null(col_keep)) {
    if (is.character(col_keep)) {
      col_keep <- names(spec$cols) %in% col_keep
    } else if (is.numeric(col_keep)) {
      col_keep <- seq_along(spec$cols) %in% col_keep
    }
    col_skip <- !col_keep
  }

  if (!is.null(col_skip)) {
    if (is.character(col_skip)) {
      col_skip <- names(spec$cols) %in% col_skip
    } else if (is.numeric(col_skip)) {
      col_skip <- seq_along(spec$cols) %in% col_skip
    }

    spec$cols[col_skip] <- rep(list(col_skip()), sum(col_skip))
  }

  spec
}

make_names <- function(len) {
  make.names(seq_len(len))
}

#' Determine progress bars should be shown
#'
#' Progress bars are shown _unless_ one of the following is `TRUE`
#' - The bar is explicitly disabled by setting `Sys.getenv("VROOM_SHOW_PROGRESS"="false")`
#' - The code is run in a non-interactive session (`interactive()` is `FALSE`).
#' - The code is run in an RStudio notebook chunk.
#' - The code is run by knitr / rmarkdown.
#' - The code is run by testthat (the `TESTTHAT` envvar is `true`).
#' @export
vroom_progress <- function() {
  env_to_logical("VROOM_SHOW_PROGRESS", TRUE) &&
    interactive() &&
    !isTRUE(getOption("knitr.in.progress")) &&
    !isTRUE(getOption("rstudio.notebook.executing")) &&
    !isTRUE(as.logical(Sys.getenv("TESTTHAT", "false")))
}

#' @importFrom crayon blue cyan green bold reset col_nchar
pb_file_format <- function(filename) {
  glue::glue_col("{bold}indexing{reset} {blue}{basename(filename)}{reset} [:bar] {green}:rate{reset}, eta: {cyan}:eta{reset}")
}

pb_width <- function(format) {
  ansii_chars <- nchar(format) - col_nchar(format)
  getOption("width", 80L) + ansii_chars
}

pb_connection_format <- function(unused) {
  glue::glue_col("{bold}indexed{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}")
}

# Guess delimiter by splitting every line by each delimiter and choosing the
# delimiter which splits the lines into the highest number of consistent fields
guess_delim <- function(lines, delims = c(",", "\t", " ", "|", ":", ";", "\n")) {
  if (length(lines) == 0) {
    return("")
  }

  # blank text within quotes
  lines <- gsub('"[^"]+"', "", lines)

  splits <- lapply(delims, strsplit, x = lines, useBytes = TRUE, fixed = TRUE)

  counts <- lapply(splits, function(x) table(lengths(x)))

  choose_best <- function(i, j) {
    x <- counts[[i]]
    y <- counts[[j]]

    nx <- as.integer(names(counts[[i]]))
    ny <- as.integer(names(counts[[j]]))

    mx <- which.max(x)
    my <- which.max(y)

    if (x[[mx]] > y[[my]] ||
      x[[mx]] == y[[my]] && nx > ny) {
      i
    } else {
      j
    }
  }
  res <- Reduce(choose_best, seq_along(counts))
  delims[[res]]
}


vroom_threads <- function() {
  as.integer(Sys.getenv("VROOM_THREADS", parallel::detectCores()))
}

vroom_tempfile <- function() {
  dir <- Sys.getenv("VROOM_TEMP_PATH")
  if (!nzchar(dir)) {
    dir <- tempdir()
  }
  tempfile(pattern = "vroom-", tmpdir = dir)
}

#' Show which column types are using Altrep
#'
#' @param which A character vector of column types to use Altrep for.
#' @export
vroom_altrep_opts <- function(which = NULL) {
  which <- match.arg(which, names(altrep_opt_vals()), several.ok = TRUE)
  which <- as.list(stats::setNames(rep(TRUE, length(which)), which))

  args <- list(
    which$chr %||% vroom_use_altrep_chr(),
    which$fct %||% vroom_use_altrep_fct(),
    which$int %||% vroom_use_altrep_int(),
    which$dbl %||% vroom_use_altrep_dbl(),
    which$num %||% vroom_use_altrep_num(),
    which$lgl %||% vroom_use_altrep_lgl(),
    which$dttm %||% vroom_use_altrep_dttm(),
    which$date %||% vroom_use_altrep_date(),
    which$time %||% vroom_use_altrep_time()
  )

  out <-  0L
  for (i in seq_along(args)) {
    out <- bitwOr(out, bitwShiftL(as.integer(args[[i]]), i - 1L))
  }
  structure(out, class = "vroom_altrep_opts")
}

altrep_opt_vals <- function() c(
  "none" = 0L,
  "chr" = 1L,
  "fct" = 2L,
  "int" = 4L,
  "dbl" = 8L,
  "num" = 16L,
  "lgl" = 32L,
  "dttm" = 64L,
  "date" = 128L,
  "time" = 256L
)

#' @export
print.vroom_altrep_opts <- function(x, ...) {
  vals <- altrep_opt_vals()
  reps <- names(vals)[bitwAnd(vals, x) > 0]

  cat(glue::glue(
      "Using Altrep representations for:
        * {reps}
       ", reps = glue::glue_collapse(reps, "\n * ")), sep = "")
}

vroom_use_altrep_chr <- function() {
  getRversion() > "3.5.0" && env_to_logical("VROOM_USE_ALTREP_CHR", TRUE)
}

vroom_use_altrep_fct <- function() {
  # fct is a numeric internally
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_FCT", FALSE))
}

vroom_use_altrep_int <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_INT", FALSE))
}

vroom_use_altrep_dbl <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DBL", FALSE))
}

vroom_use_altrep_num <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_NUM", FALSE))
}

vroom_use_altrep_lgl <- function() {
  getRversion() > "3.6.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_LGL", FALSE))
}

vroom_use_altrep_dttm <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DTTM", FALSE))
}

vroom_use_altrep_date <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DATE", FALSE))
}

vroom_use_altrep_time <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_TIME", FALSE))
}
