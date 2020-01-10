#' Create column specification
#'
#' `cols()` includes all columns in the input data, guessing the column types
#' as the default. `cols_only()` includes only the columns you explicitly
#' specify, skipping the rest.
#'
#' The available specifications are: (with string abbreviations in brackets)
#'
#' * `col_logical()` \[l\], containing only `T`, `F`, `TRUE` or `FALSE`.
#' * `col_integer()` \[i\], integers.
#' * `col_big_integer()` \[I\], Big Integers (64bit), requires the `bit64` package.
#' * `col_double()` \[d\], doubles.
#' * `col_character()` \[c\], everything else.
#' * `col_factor(levels, ordered)` \[f\], a fixed set of values.
#' * `col_date(format = "")` \[D\]: with the locale's `date_format`.
#' * `col_time(format = "")` \[t\]: with the locale's `time_format`.
#' * `col_datetime(format = "")` \[T\]: ISO8601 date times
#' * `col_number()` \[n\], numbers containing the `grouping_mark`
#' * `col_skip()` \[_, -\], don't import this column.
#' * `col_guess()` \[?\], parse using the "best" type based on the input.
#'
#' @param ... Either column objects created by `col_*()`, or their abbreviated
#'   character names (as described in the `col_types` argument of
#'   [vroom()]). If you're only overriding a few columns, it's
#'   best to refer to columns by name. If not named, the column types must match
#'   the column names exactly. In `col_*()` functions these are stored in the
#'   object.
#' @param .default Any named columns not explicitly overridden in `...`
#'   will be read with this column type.
#' @param .delim The delimiter to use when parsing. If the `delim` argument
#'   used in the call to `vroom()` it takes precedence over the one specified in
#'   `col_types`.
#' @export
#' @examples
#' cols(a = col_integer())
#' cols_only(a = col_integer())
#'
#' # You can also use the standard abbreviations
#' cols(a = "i")
#' cols(a = "i", b = "d", c = "_")
#'
#' # You can also use multiple sets of column definitions by combining
#' # them like so:
#'
#' t1 <- cols(
#'   column_one = col_integer(),
#'   column_two = col_number())
#'
#' t2 <- cols(
#'  column_three = col_character())
#'
#' t3 <- t1
#' t3$cols <- c(t1$cols, t2$cols)
#' t3
cols <- function(..., .default = col_guess(), .delim = NULL) {
  col_types <- rlang::list2(...)
  is_character <- vapply(col_types, is.character, logical(1))
  col_types[is_character] <- lapply(col_types[is_character], col_concise)

  if (is.character(.default)) {
    .default <- col_concise(.default)
  }

  col_spec(col_types, .default, .delim)
}

#' @export
#' @rdname cols
cols_only <- function(...) {
  cols(..., .default = col_skip())
}


# col_spec ----------------------------------------------------------------

col_spec <- function(col_types, default = col_guess(), delim) {
  stopifnot(is.list(col_types))
  stopifnot(is.collector(default))

  is_collector <- vapply(col_types, is.collector, logical(1))
  if (any(!is_collector)) {
    stop("Some `col_types` are not S3 collector objects: ",
      paste(which(!is_collector), collapse = ", "), call. = FALSE)
  }

  structure(
    list(
      cols = col_types,
      default = default,
      delim = delim
    ),
    class = "col_spec"
  )
}

is.col_spec <- function(x) inherits(x, "col_spec")


#' Coerce to a column specification
#'
#' This is most useful for generating a specification using the short form or coercing from a list.
#'
#' @param x Input object
#' @keywords internal
#' @examples
#' as.col_spec("cccnnn")
#' @export
as.col_spec <- function(x) UseMethod("as.col_spec")

#' @export
as.col_spec.character <- function(x) {
  if (is_named(x)) {
    return(as.col_spec(as.list(x)))
  }
  letters <- strsplit(x, "")[[1]]
  col_spec(lapply(letters, col_concise), col_guess(), delim = NULL)
}

#' @export
as.col_spec.NULL <- function(x) {
  col_spec(list(), delim = NULL)
}

#' @export
as.col_spec.list <- function(x) {
  do.call(cols, x)
}
#' @export
as.col_spec.col_spec <- function(x) x

#' @export
as.col_spec.default <- function(x) {
  stop("`col_types` must be NULL, a list or a string", call. = FALSE)
}

# Conditionally exported in zzz.R
# @export
print.col_spec <- function(x, n = Inf, condense = NULL, colour = crayon::has_color(), ...) {
  cat(format.col_spec(x, n = n, condense = condense, colour = colour, ...))

  invisible(x)
}

#' @description
#' `cols_condense()` takes a spec object and condenses its definition by setting
#' the default column type to the most frequent type and only listing columns
#' with a different type.
#' @rdname spec
#' @export
cols_condense <- function(x) {
  types <- vapply(x$cols, function(xx) class(xx)[[1]], character(1))
  counts <- table(types)
  most_common <- names(counts)[counts == max(counts)][[1]]

  x$default <- x$cols[types == most_common][[1]]
  x$cols <- x$cols[types != most_common]
  x
}

# Conditionally exported in zzz.R
# @export
format.col_spec <- function(x, n = Inf, condense = NULL, colour = crayon::has_color(), ...) {

  if (n == 0) {
    return("")
  }

  # condense if cols >= n
  condense <- condense %||% (length(x$cols) >= n)
  if (isTRUE(condense)) {
    x <- cols_condense(x)
  }

  # truncate to minumum of n or length
  cols <- x$cols[seq_len(min(length(x$cols), n))]

  default <- NULL
  if (inherits(x$default, "collector_guess")) {
    fun_type <- "cols"
  } else if (inherits(x$default, "collector_skip")) {
    fun_type <- "cols_only"
  } else {
    fun_type <- "cols"
    type <- sub("^collector_", "", class(x$default)[[1]])
    default <- paste0(".default = col_", type, "()")
  }

  delim <- x$delim

  if (!is.null(delim) && nzchar(delim)) {
    delim <- paste0('.delim = ', double_quote(delim), '')
  }

  cols_args <- c(
    default,
    vapply(seq_along(cols),
      function(i) {
        col_funs <- sub("^collector_", "col_", class(cols[[i]])[[1]])
        args <- vapply(cols[[i]], deparse2, character(1), sep = "\n    ")
        args <- paste(names(args), args, sep = " = ", collapse = ", ")

        col_funs <- paste0(col_funs, "(", args, ")")
        col_funs <- colourise_cols(col_funs, colour)

        col_names <- names(cols)[[i]] %||% ""

        # Need to handle unnamed columns and columns with non-syntactic names
        named <- col_names != ""

        non_syntactic <- !is_syntactic(col_names) & named
        col_names[non_syntactic] <- paste0("`", gsub("`", "\\\\`", col_names[non_syntactic]), "`")

        out <- paste0(col_names, " = ", col_funs)
        out[!named] <- col_funs[!named]
        out
      },
      character(1)
    ),
    delim
  )
  if (length(x$cols) == 0 && length(cols_args) == 0) {
    return(paste0(fun_type, "()\n"))
  }


  out <- paste0(fun_type, "(\n  ", paste(collapse = ",\n  ", cols_args))

  if (length(x$cols) > n) {
    out <- paste0(out, "\n  # ... with ", length(x$cols) - n, " more columns")
  }
  out <- paste0(out, "\n)\n")

  out
}

colourise_cols <- function(cols, colourise = crayon::has_color()) {

  if (!isTRUE(colourise)) {
    return(cols)
  }

  fname <- sub("[(].*", "", cols)
  for (i in seq_along(cols)) {
    cols[[i]] <- switch(fname,
      col_skip = ,
      col_guess = cols[[i]],

      col_character = ,
      col_factor = crayon::red(cols[[i]]),

      col_logical = crayon::yellow(cols[[i]]),

      col_double = ,
      col_integer = ,
      col_number = crayon::green(cols[[i]]),

      col_date = ,
      col_datetime = ,
      col_time = crayon::blue(cols[[i]])
      )
  }
  cols
}

# This allows str() on a tibble object to print a little nicer.
# Conditionally exported in zzz.R
# @export
str.col_spec <- function(object, ..., indent.str = "") {

  # Split the formatted column spec into strings
  specs <- strsplit(format(object), "\n")[[1]]
  cat(sep = "",
    "\n",

    # Append the current indentation string to the specs
    paste(indent.str, specs, collapse = "\n"),

    "\n")
}


#' Examine the column specifications for a data frame
#'
#' `spec()` extracts the full column specification from a tibble
#' created by readr.
#'
#' @family parsers
#' @param x The data frame object to extract from
#' @return A col_spec object.
#' @export
#' @examples
#' df <- vroom(vroom_example("mtcars.csv"))
#' s <- spec(df)
#' s
#'
#' cols_condense(s)
spec <- function(x) {
  stopifnot(inherits(x, "tbl_df"))
  attr(x, "spec")
}

col_concise <- function(x) {
  switch(x,
    "_" = ,
    "-" = col_skip(),
    "?" = col_guess(),
    c = col_character(),
    f = col_factor(),
    d = col_double(),
    i = col_integer(),
    I = col_big_integer(),
    l = col_logical(),
    n = col_number(),
    D = col_date(),
    T = col_datetime(),
    t = col_time(),
    stop("Unknown shortcut: ", x, call. = FALSE)
  )
}

vroom_enquo <- function(x) {
  if (rlang::quo_is_call(x, "c") || rlang::quo_is_call(x, "list")) {
    return(rlang::as_quosures(rlang::get_expr(x)[-1], rlang::get_env(x)))
  }
  x
}

vroom_select <- function(x, col_select, id) {
  # reorder and rename columns
  if (inherits(col_select, "quosures") || !rlang::quo_is_null(col_select)) {
    if (inherits(col_select, "quosures")) {
      vars <- tidyselect::vars_select(c(id, names(spec(x)$cols)), !!!col_select)
    } else {
      vars <- tidyselect::vars_select(c(id, names(spec(x)$cols)), !!col_select)
    }
    # This can't be just names(x) as we need to have skipped
    # names as well to pass to vars_select()
    x <- x[vars]
    names(x) <- names(vars)
  }
  x
}

col_types_standardise <- function(spec, col_names, col_select) {
  type_names <- names(spec$cols)

  if (length(spec$cols) == 0) {
    # no types specified so use defaults

    spec$cols <- rep(list(spec$default), length(col_names))
    names(spec$cols) <- col_names
  } else if (is.null(type_names)) {
    # unnamed types & names guessed from header: match exactly

    if (length(spec$cols) != length(col_names)) {
      stop("Unnamed `col_types` must have the same length as `col_names`.", call. = FALSE)
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

  if (inherits(col_select, "quosures") || !rlang::quo_is_null(col_select)) {
    if (inherits(col_select, "quosures")) {
      to_keep <- names(spec$cols) %in% tidyselect::vars_select(names(spec$cols), !!!col_select, .strict = FALSE)
    } else {
      to_keep <- names(spec$cols) %in% tidyselect::vars_select(names(spec$cols), !!col_select, .strict = FALSE)
    }

    spec$cols[!to_keep] <- rep(list(col_skip()), sum(!to_keep))
  }

  spec
}


#' Guess the type of a vector
#'
#' @inheritParams readr::guess_parser
#' @examples
#'  # Logical vectors
#'  guess_type(c("FALSE", "TRUE", "F", "T"))

#'  # Integers and doubles
#'  guess_type(c("1","2","3"))
#'  guess_type(c("1.6","2.6","3.4"))

#'  # Numbers containing grouping mark
#'  guess_type("1,234,566")

#'  # ISO 8601 date times
#'  guess_type(c("2010-10-10"))
#'  guess_type(c("2010-10-10 01:02:03"))
#'  guess_type(c("01:02:03 AM"))
#' @export
guess_type <- function(x, na = c("", "NA"), locale = default_locale(), guess_integer = FALSE) {
  type <- guess_type_(x, na = na, locale = locale, guess_integer = guess_integer)
  get(paste0("col_", type), asNamespace("vroom"))()
}

guess_parser <- function(x, na = c("", "NA"), locale = default_locale(), guess_integer = FALSE) {
  guess_type_(x, na = na, locale = locale, guess_integer = guess_integer)
}

#' @importFrom crayon silver
#' @importFrom glue double_quote
show_spec_summary <- function(x, width = getOption("width"), locale = default_locale()) {
  spec <- spec(x)
  if (length(spec$cols) == 0) {
    return(invisible(x))
  }

  type_map <- c("collector_character" = "chr", "collector_double" = "dbl",
    "collector_integer" = "int", "collector_num" = "num", "collector_logical" = "lgl",
    "collector_factor" = "fct", "collector_datetime" = "dttm", "collector_date" = "date",
    "collector_time" = "time")

  col_types <- vapply(spec$cols, function(x) class(x)[[1]], character(1))
  col_types <- droplevels(factor(type_map[col_types], levels = unname(type_map)))
  type_counts <- table(col_types)

  n <- length(type_counts)

  types <- format(vapply(names(type_counts), color_type, character(1)))
  counts <- format(type_counts)
  col_width <- min(width - crayon::col_nchar(types) + nchar(counts) + 4)
  columns <- vapply(split(names(spec$cols), col_types), function(x) glue::glue_collapse(x, ", ", width = col_width), character(1))

  fmt_num <- function(x) {
    prettyNum(x, big.mark = locale$grouping_mark, decimal.mark = locale$decimal_mark)
  }

  delim <- spec$delim %||% ""

  message(
    glue::glue(
      .transformer = collapse_transformer(sep = "\n"),
      entries = glue::glue("{format(types)} [{format(type_counts)}]: {columns}"),

      '
      {bold("Rows:")} {fmt_num(NROW(x))}
      {bold("Columns:")} {fmt_num(NCOL(x))}
      {if (nzchar(delim)) paste(bold("Delimiter:"), double_quote(delim)) else ""}
      {entries*}

      {silver("Use `spec()` to retrieve the guessed column specification")}
      {silver("Pass a specification to the `col_types` argument to quiet this message")}
      '
    )
  )

  invisible(x)
}

color_type <- function(type) {
  switch(type,
    chr = ,
    fct = crayon::red(type),
    lgl = crayon::yellow(type),
    dbl = ,
    int = ,
    num = crayon::green(type),
    date = ,
    dttm = ,
    time = crayon::blue(type)
  )
}

#' @rdname cols
#' @export
col_logical <- function(...) {
  collector("logical", ...)
}

#' @rdname cols
#' @export
col_integer <- function(...) {
  collector("integer", ...)
}

#' @rdname cols
#' @export
col_big_integer <- function(...) {
  collector("big_integer", ...)
}

#' @rdname cols
#' @export
col_double <- function(...) {
  collector("double", ...)
}

#' @rdname cols
#' @export
col_character <- function(...) {
  collector("character", ...)
}

#' @rdname cols
#' @export
col_skip <- function(...) {
  collector("skip", ...)
}

#' @rdname cols
#' @export
col_number <- function(...) {
  collector("number", ...)
}

#' @rdname cols
#' @export
col_guess <- function(...) {
  collector("guess", ...)
}

#' @inheritParams readr::col_factor
#' @rdname cols
#' @export
col_factor <- function(levels = NULL, ordered = FALSE, include_na = FALSE, ...) {
  collector("factor", levels = levels, ordered = ordered, include_na = include_na, ...)
}

#' @inheritParams readr::col_datetime
#' @rdname cols
#' @export
col_datetime <- function(format = "", ...) {
  collector("datetime", format = format, ...)
}

#' @rdname cols
#' @export
col_date <- function(format = "", ...) {
  collector("date", format = format, ...)
}

#' @rdname cols
#' @export
col_time <- function(format = "", ...) {
  collector("time", format = format, ...)
}
