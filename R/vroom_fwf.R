#' Read a fixed width file into a tibble
#'
#' @details
#' *Note*: `fwf_empty()` cannot take a R connection such as a URL as input, as
#' this would result in reading from the connection twice. In these cases it is
#' better to download the file first before reading.
#' @inheritParams readr::read_fwf
#' @inheritParams vroom
#' @export
#' @examples
#' fwf_sample <- vroom_example("fwf-sample.txt")
#' cat(readLines(fwf_sample))
#'
#' # You can specify column positions in several ways:
#' # 1. Guess based on position of empty columns
#' vroom_fwf(fwf_sample, fwf_empty(fwf_sample, col_names = c("first", "last", "state", "ssn")))
#' # 2. A vector of field widths
#' vroom_fwf(fwf_sample, fwf_widths(c(20, 10, 12), c("name", "state", "ssn")))
#' # 3. Paired vectors of start and end positions
#' vroom_fwf(fwf_sample, fwf_positions(c(1, 30), c(20, 42), c("name", "ssn")))
#' # 4. Named arguments with start and end positions
#' vroom_fwf(fwf_sample, fwf_cols(name = c(1, 20), ssn = c(30, 42)))
#' # 5. Named arguments with column widths
#' vroom_fwf(fwf_sample, fwf_cols(name = 20, state = 10, ssn = 12))
vroom_fwf <- function(file,
                      col_positions = fwf_empty(file, skip, n = guess_max),
                      col_types = NULL,
                      col_select = NULL, id = NULL,
                      locale = default_locale(), na = c("", "NA"),
                      comment = "",
                      skip_empty_rows = TRUE,
                      trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = 100,
                      altrep = TRUE,
                      altrep_opts = deprecated(),
                      num_threads = vroom_threads(),
                      progress = vroom_progress(),
                      show_col_types = NULL,
                      .name_repair = "unique") {

  verify_fwf_positions(col_positions)

  if (!is_missing(altrep_opts)) {
    deprecate_warn("1.1.0", "vroom_fwf(altrep_opts = )", "vroom_fwf(altrep = )")
    altrep <- altrep_opts
  }

  file <- standardise_path(file)

  if (!is_ascii_compatible(locale$encoding)) {
    file <- reencode_file(file, locale$encoding)
    locale$encoding <- "UTF-8"
  }

  if (length(file) == 0 || (n_max == 0 & identical(col_positions$col_names, FALSE))) {
    out <- tibble::tibble()
    class(out) <- c("spec_tbl_df", class(out))
    return(out)
  }

  if (n_max < 0 || is.infinite(n_max)) {
    n_max <- -1
  }

  if (guess_max < 0 || is.infinite(guess_max)) {
    guess_max <- -1
  }

  col_select <- vroom_enquo(enquo(col_select))

  has_col_types <- !is.null(col_types)

  col_types <- as.col_spec(col_types)

  out <- vroom_fwf_(file, as.integer(col_positions$begin), as.integer(col_positions$end),
    trim_ws = trim_ws, col_names = col_positions$col_names,
    col_types = col_types, col_select = col_select,
    name_repair = .name_repair,
    id = id, na = na, guess_max = guess_max, skip = skip, comment = comment,
    skip_empty_rows = skip_empty_rows,
    n_max = n_max, num_threads = num_threads,
    altrep = vroom_altrep(altrep), locale = locale,
    progress = progress)

  out <- tibble::as_tibble(out, .name_repair = .name_repair)

  out <- vroom_select(out, col_select, id)
  class(out) <- c("spec_tbl_df", class(out))

  if (should_show_col_types(has_col_types, show_col_types)) {
    show_col_types(out, locale)
  }

  out
}


#' @rdname vroom_fwf
#' @inheritParams readr::read_fwf
#' @export
#' @param n Number of lines the tokenizer will read to determine file structure. By default
#'      it is set to 100.
fwf_empty <- function(file, skip = 0, col_names = NULL, comment = "", n = 100L) {

  file <- standardise_one_path(standardise_path(file)[[1]])

  if (inherits(file, "connection")) {
    stop("`file` must be a regular file, not a connection", call. = FALSE)
  }

  if (n < 0 || is.infinite(n)) {
    n <- -1
  }

  out <- whitespace_columns_(file[[1]], skip, comment = comment, n = n)
  out$end[length(out$end)] <- NA

  col_names <- fwf_col_names(col_names, length(out$begin))
  out$col_names <- col_names
  out
}

#' @rdname vroom_fwf
#' @export
#' @param widths Width of each field. Use NA as width of last field when
#'    reading a ragged fwf file.
#' @param col_names Either NULL, or a character vector column names.
fwf_widths <- function(widths, col_names = NULL) {
  pos <- cumsum(c(1L, abs(widths)))
  fwf_positions(pos[-length(pos)], pos[-1] - 1L, col_names)
}

#' @rdname vroom_fwf
#' @export
#' @param start,end Starting and ending (inclusive) positions of each field.
#'    Use NA as last end field when reading a ragged fwf file.
fwf_positions <- function(start, end = NULL, col_names = NULL) {

  stopifnot(length(start) == length(end))
  col_names <- fwf_col_names(col_names, length(start))

  tibble::tibble(
    begin = start - 1L,
    end = end, # -1 to change to 0 offset, +1 to be exclusive,
    col_names = as.character(col_names)
  )
}


#' @rdname vroom_fwf
#' @export
#' @param ... If the first element is a data frame,
#'   then it must have all numeric columns and either one or two rows.
#'   The column names are the variable names. The column values are the
#'   variable widths if a length one vector, and if length two, variable start and end
#'   positions. The elements of `...` are used to construct a data frame
#'   with or or two rows as above.
fwf_cols <- function(...) {
  x <- lapply(list(...), as.integer)
  names(x) <- fwf_col_names(names(x), length(x))
  x <- tibble::as_tibble(x)
  if (nrow(x) == 2) {
    fwf_positions(as.integer(x[1, ]), as.integer(x[2, ]), names(x))
  } else if (nrow(x) == 1) {
    fwf_widths(as.integer(x[1, ]), names(x))
  } else {
    stop("All variables must have either one (width) two (start, end) values.",
         call. = FALSE)
  }
}

fwf_col_names <- function(nm, n) {
  nm <- nm %||% rep("", n)
  nm_empty <- (nm == "")
  nm[nm_empty] <- paste0("X", seq_len(n))[nm_empty]
  nm
}

verify_fwf_positions <- function(col_positions) {
  is_greater <- stats::na.omit(col_positions$begin > col_positions$end)
  if (any(is_greater)) {
    bad <- which(is_greater)
    stop("`col_positions` must have begin less than end.\n* Invalid values at position(s): ", paste0(collapse = ", ", bad), call. = FALSE)
  }
}
