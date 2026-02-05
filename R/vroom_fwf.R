#' Read a fixed-width file into a tibble
#'
#' @description
#' Fixed-width files store tabular data with each field occupying a specific
#' range of character positions in every line. Once the fields are identified,
#' converting them to the appropriate R types works just like for delimited
#' files. The unique challenge with fixed-width files is describing where each
#' field begins and ends. \pkg{vroom} tries to ease this pain by offering a
#' few different ways to specify the field structure:
#' - `fwf_empty()` - Guesses based on the positions of empty columns. This is
#'   the default. (Note that `fwf_empty()` returns 0-based positions, for
#'   internal use.)
#' - `fwf_widths()` - Supply the widths of the columns.
#' - `fwf_positions()` - Supply paired vectors of start and end positions. These
#'   are interpreted as 1-based positions, so are off-by-one compared to the
#'   output of `fwf_empty()`.
#' - `fwf_cols()` - Supply named arguments of paired start and end positions or
#'    column widths.
#'
#' Note: `fwf_empty()` cannot work with a connection or with any of the input
#' types that involve a connection internally, which includes remote and
#' compressed files. The reason is that this would necessitate reading from the
#' connection twice. In these cases, you'll have to either provide the field
#' structure explicitly with another `fwf_*()` function or download (and
#' decompress, if relevant) the file first.
#'
#' @details
#' Here's a enhanced example using the contents of the file accessed via
#' `vroom_example("fwf-sample.txt")`.
#'
#' ```
#'          1         2         3         4
#' 123456789012345678901234567890123456789012
#' [     name 20      ][state 10][  ssn 12  ]
#' John Smith          WA        418-Y11-4111
#' Mary Hartford       CA        319-Z19-4341
#' Evan Nolan          IL        219-532-c301
#' ```
#'
#' Here are some valid field specifications for the above (they aren't all
#' equivalent! but they are all valid):
#'
#' ```
#' fwf_widths(c(20, 10, 12), c("name", "state", "ssn"))
#' fwf_positions(c(1, 30), c(20, 42), c("name", "ssn"))
#' fwf_cols(state = c(21, 30), last = c(6, 20), first = c(1, 4), ssn = c(31, 42))
#' fwf_cols(name = c(1, 20), ssn = c(30, 42))
#' fwf_cols(name = 20, state = 10, ssn = 12)
#' ```
#'
#' @inheritParams vroom
#' @param col_positions Column positions, as created by [fwf_empty()],
#'   `fwf_widths()`, `fwf_positions()`, or `fwf_cols()`. To read in only
#'   selected fields, use `fwf_positions()`. If the width of the last column
#'   is variable (a ragged fwf file), supply the last end position as `NA`.
#' @param comment A string used to identify comments. Any line that starts
#'   with the comment string at the beginning of the file (before any data
#'   lines) will be ignored. Unlike [vroom()], comment lines in the middle
#'   of the file are not filtered out.
#' @export
#' @examples
#' fwf_sample <- vroom_example("fwf-sample.txt")
#' writeLines(vroom_lines(fwf_sample))
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
vroom_fwf <- function(
  file,
  col_positions = fwf_empty(file, skip, n = guess_max),
  col_types = NULL,
  col_select = NULL,
  id = NULL,
  locale = default_locale(),
  na = c("", "NA"),
  comment = "",
  skip_empty_rows = TRUE,
  trim_ws = TRUE,
  skip = 0,
  n_max = Inf,
  guess_max = 100,
  altrep = TRUE,
  num_threads = vroom_threads(),
  progress = vroom_progress(),
  show_col_types = NULL,
  .name_repair = "unique"
) {
  verify_fwf_positions(col_positions)

  file <- standardise_path(file)

  col_select <- vroom_enquo(enquo(col_select))

  use_libvroom <- can_use_libvroom_fwf(file, col_types, locale)
  if (use_libvroom) {
    input <- connection_or_filepath(file[[1]])
    # Non-ASCII paths need to go through R connection for proper encoding
    # handling (libvroom expects UTF-8 paths but non-UTF-8 locales mangle them)
    if (is.character(input) && !is_ascii_path(input)) {
      input <- file(input)
    }
    if (inherits(input, "connection")) {
      input <- read_connection_raw(input)
      if (length(input) == 0L) {
        return(tibble::tibble())
      }
    }

    n_max_int <- if (is.infinite(n_max) || n_max < 0) -1L else as.integer(n_max)
    na_str <- paste(na, collapse = ",")
    col_ends_int <- as.integer(col_positions$end)
    col_ends_int[is.na(col_ends_int)] <- -1L

    # Resolve col_types for libvroom
    col_types_int <- integer(0)
    col_type_names <- character(0)
    resolved_spec <- NULL
    if (!is.null(col_types) && !identical(col_types, list())) {
      resolved_spec <- as.col_spec(col_types)
      col_types_int <- col_types_to_libvroom(resolved_spec)
      spec_names <- names(resolved_spec$cols)
      if (!is.null(spec_names) && !all(spec_names == "")) {
        col_type_names <- spec_names
      }
    }

    out <- vroom_libvroom_fwf_(
      input = input,
      col_starts = as.integer(col_positions$begin),
      col_ends = col_ends_int,
      col_names = as.character(col_positions$col_names),
      trim_ws = trim_ws,
      comment = comment,
      skip_empty_rows = skip_empty_rows,
      na_values = na_str,
      skip = as.integer(skip),
      n_max = n_max_int,
      num_threads = as.integer(num_threads),
      col_types = col_types_int,
      col_type_names = col_type_names
    )

    # For cols_only(), drop columns not in the spec
    if (
      !is.null(resolved_spec) &&
        inherits(resolved_spec$default, "collector_skip")
    ) {
      spec_names <- names(resolved_spec$cols)
      keep_cols <- names(out) %in% spec_names
      out <- out[, keep_cols, drop = FALSE]
    }

    # Drop skipped columns from output (positional skip notation)
    if (length(col_types_int) > 0) {
      skip_mask <- col_types_int == -1L
      already_handled_by_cols_only <- !is.null(resolved_spec) &&
        inherits(resolved_spec$default, "collector_skip")
      if (any(skip_mask) && !already_handled_by_cols_only) {
        keep <- !skip_mask[seq_len(min(length(skip_mask), ncol(out)))]
        if (length(keep) < ncol(out)) {
          keep <- c(keep, rep(TRUE, ncol(out) - length(keep)))
        }
        out <- out[, keep, drop = FALSE]
      }
    }

    # Apply R-side post-processing
    out <- apply_col_postprocessing(
      out,
      resolved_spec,
      col_types_int,
      col_type_names
    )

    out <- tibble::as_tibble(out, .name_repair = .name_repair)
    if (!is.null(id)) {
      path_value <- if (is.character(file[[1]])) file[[1]] else NA_character_
      out <- tibble::add_column(
        out,
        !!id := rep(path_value, nrow(out)),
        .before = 1
      )
    }

    # Apply column selection using names directly (no spec attribute)
    if (inherits(col_select, "quosures") || !quo_is_null(col_select)) {
      all_names <- c(names(out), id)
      if (inherits(col_select, "quosures")) {
        vars <- tidyselect::vars_select(all_names, !!!col_select)
      } else {
        vars <- tidyselect::vars_select(all_names, !!col_select)
      }
      out <- out[vars]
      names(out) <- names(vars)
    }

    # Build and attach spec attribute
    all_col_names <- as.character(col_positions$col_names)
    attr(out, "spec") <- build_libvroom_spec(
      out,
      resolved_spec,
      col_types_int,
      all_col_names,
      delim = ""
    )

    # Add empty problems attribute (libvroom doesn't track parse errors yet)
    attr(out, "problems") <- tibble::tibble(
      row = integer(),
      col = integer(),
      expected = character(),
      actual = character(),
      file = character()
    )

    class(out) <- c("spec_tbl_df", class(out))
    return(out)
  }

  if (!is_ascii_compatible(locale$encoding)) {
    file <- reencode_file(file, locale$encoding)
    locale$encoding <- "UTF-8"
  }

  if (
    length(file) == 0 ||
      (n_max == 0 & identical(col_positions$col_names, FALSE))
  ) {
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

  has_col_types <- !is.null(col_types)

  col_types <- as.col_spec(col_types)

  out <- vroom_fwf_(
    file,
    as.integer(col_positions$begin),
    as.integer(col_positions$end),
    trim_ws = trim_ws,
    col_names = col_positions$col_names,
    col_types = col_types,
    col_select = col_select,
    name_repair = .name_repair,
    id = id,
    na = na,
    guess_max = guess_max,
    skip = skip,
    comment = comment,
    skip_empty_rows = skip_empty_rows,
    n_max = n_max,
    num_threads = num_threads,
    altrep = vroom_altrep(altrep),
    locale = locale,
    progress = progress
  )

  out <- tibble::as_tibble(out, .name_repair = .name_repair)

  out <- vroom_select(out, col_select, id)
  class(out) <- c("spec_tbl_df", class(out))

  if (should_show_col_types(has_col_types, show_col_types)) {
    show_col_types(out, locale)
  }

  out
}


#' @rdname vroom_fwf
#' @export
#' @param n Number of lines the tokenizer will read to determine file structure. By default
#'      it is set to 100.
fwf_empty <- function(
  file,
  skip = 0,
  col_names = NULL,
  comment = "",
  n = 100L
) {
  file <- connection_or_filepath(standardise_path(file)[[1]])

  if (inherits(file, "connection")) {
    cli::cli_abort("{.arg file} must be a regular file, not a connection.")
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
#' @param widths Width of each field. Use `NA` as the width of the last field
#'   when reading a ragged fixed-width file.
#' @param col_names Either NULL, or a character vector column names.
fwf_widths <- function(widths, col_names = NULL) {
  pos <- cumsum(c(1L, abs(widths)))
  fwf_positions(pos[-length(pos)], pos[-1] - 1L, col_names)
}

#' @rdname vroom_fwf
#' @export
#' @param start,end Starting and ending (inclusive) positions of each field.
#'    **Positions are 1-based**: the first character in a line is at position 1.
#'    Use `NA` as the last value of `end` when reading a ragged fixed-width
#'    file.
fwf_positions <- function(start, end = NULL, col_names = NULL) {
  if (length(start) != length(end)) {
    cli::cli_abort(
      c(
        "{.arg start} and {.arg end} must have the same length.",
        "i" = "{.arg start} has length {length(start)}.",
        "i" = "{.arg end} has length {length(end)}."
      )
    )
  }

  if (any(start <= 0, na.rm = TRUE)) {
    cli::cli_abort(
      c(
        "{.arg start} positions must be >= 1, i.e. use 1-based indexing.",
        "i" = "The first character in a line is at position 1, not 0.",
        "i" = "If you got these positions from {.fn fwf_empty}, note that its output uses 0-based indexing."
      )
    )
  }

  col_names <- fwf_col_names(col_names, length(start))

  tibble::tibble(
    begin = start - 1L,
    end = end, # -1 to change to 0 offset, +1 to be exclusive,
    col_names = as.character(col_names)
  )
}


#' @rdname vroom_fwf
#' @export
#' @param ... Named or unnamed arguments, each addressing one column. Each input
#' should be either a single integer (a column width) or a pair of integers
#' (column start and end positions). All arguments must have the same shape,
#' i.e. all widths or all positions.
fwf_cols <- function(...) {
  x <- lapply(list(...), as.integer)

  # Check that all inputs have the same length (1 or 2)
  lengths <- lengths(x)
  unique_lengths <- unique(lengths)

  if (length(unique_lengths) > 1) {
    cli::cli_abort(
      c(
        "All inputs must have the same shape.",
        "x" = "Found inputs with different lengths: {unique_lengths}.",
        "i" = "Provide either single values (widths) or pairs of values (positions)."
      )
    )
  }

  if (!unique_lengths %in% c(1, 2)) {
    cli::cli_abort(
      c(
        "All inputs must be either a single value or a pair of values.",
        "x" = "The provided inputs each have length {unique_lengths}.",
        "i" = "Single values specify column widths: {.code fwf_cols(a = 10, b = 5)}.",
        "i" = "Pairs of values specify start and end positions: {.code fwf_cols(a = c(1, 10), b = c(11, 15))}."
      )
    )
  }

  names(x) <- fwf_col_names(names(x), length(x))
  x <- tibble::as_tibble(x)

  if (nrow(x) == 2) {
    fwf_positions(as.integer(x[1, ]), as.integer(x[2, ]), names(x))
  } else {
    fwf_widths(as.integer(x[1, ]), names(x))
  }
}

fwf_col_names <- function(nm, n) {
  nm <- nm %||% rep("", n)
  nm_empty <- (nm == "")
  nm[nm_empty] <- paste0("X", seq_len(n))[nm_empty]
  nm
}

can_use_libvroom_fwf <- function(file, col_types, locale) {
  if (length(file) != 1) {
    return(FALSE)
  }
  if (!is_ascii_compatible(locale$encoding)) {
    return(FALSE)
  }
  if (!can_libvroom_handle_col_types(col_types)) {
    return(FALSE)
  }
  TRUE
}

verify_fwf_positions <- function(col_positions) {
  is_greater <- stats::na.omit(col_positions$begin > col_positions$end)
  if (any(is_greater)) {
    bad_cols <- col_positions$col_names[is_greater]
    cli::cli_abort(
      c(
        "{.arg begin} cannot be greater than {.arg end}.",
        "x" = "Problem with column{?s}: {.val {bad_cols}}."
      )
    )
  }
}
