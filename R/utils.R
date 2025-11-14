env_to_logical <- function(var, default = TRUE) {
  res <- Sys.getenv(var, default)
  if (res %in% c("1", "yes", "true")) {
    TRUE
  } else if (res %in% c("0", "no", "false")) {
    FALSE
  } else {
    default
  }
}

is_windows <- function() {
  identical(tolower(Sys.info()[["sysname"]]), "windows")
}

is_loaded <- function(pkg) {
  isTRUE(pkg[[1]] %in% loadedNamespaces())
}

`%||%` <- function(x, y) if (is.null(x)) y else x

collapse_transformer <- function(regex = "[*]$", ...) {
  function(text, envir) {
    if (grepl(regex, text)) {
      text <- sub(regex, "", text)
      res <- eval(parse(text = text, keep.source = FALSE), envir)
      glue::glue_collapse(res, ...)
    } else {
      glue::identity_transformer(text, envir)
    }
  }
}

is_named <- function(x) {
  nms <- names(x)
  if (is.null(nms)) {
    return(FALSE)
  }
  all(nms != "" & !is.na(nms))
}

deparse2 <- function(expr, ..., sep = "\n") {
  paste(deparse(expr, ...), collapse = sep)
}

is_syntactic <- function(x) make.names(x) == x

# Conditionally exported in zzz.R
# @export
compare.spec_tbl_df <- function(x, y, ...) {
  attr(x, "spec") <- NULL
  attr(x, "problems") <- NULL
  attr(y, "spec") <- NULL
  attr(y, "problems") <- NULL
  class(x) <- setdiff(class(x), "spec_tbl_df")
  class(y) <- setdiff(class(y), "spec_tbl_df")
  NextMethod("compare")
}

# Conditionally exported in zzz.R
# @export
compare_proxy.spec_tbl_df <- function(x, path) {
  attr(x, "spec") <- NULL
  attr(x, "problems") <- NULL
  class(x) <- setdiff(class(x), "spec_tbl_df")

  if ("path" %in% names(formals(waldo::compare_proxy))) {
    list(object = x, path = path)
  } else {
    x
  }
}

# Conditionally exported in zzz.R
# @export
as_tibble.spec_tbl_df <- function(x, ...) {
  attr(x, "spec") <- NULL
  attr(x, "problems") <- NULL
  class(x) <- setdiff(class(x), "spec_tbl_df")
  NextMethod("as_tibble")
}

# Conditionally exported in zzz.R
# @export
all.equal.spec_tbl_df <- function(target, current, ...) {
  attr(target, "spec") <- NULL
  attr(target, "problems") <- NULL
  attr(current, "spec") <- NULL
  attr(current, "problems") <- NULL
  class(target) <- setdiff(class(target), "spec_tbl_df")
  class(current) <- setdiff(class(current), "spec_tbl_df")
  NextMethod("all.equal")
}

# Conditionally exported in zzz.R
# @export
as.data.frame.spec_tbl_df <- function(x, ...) {
  attr(x, "spec") <- NULL
  attr(x, "problems") <- NULL
  class(x) <- setdiff(class(x), "spec_tbl_df")
  NextMethod("as.data.frame")
}

is_rstudio_console <- function() {
  !(Sys.getenv("RSTUDIO", "") == "" || Sys.getenv("RSTUDIO_TERM", "") != "")
}

is_rstudio_version <- function(min, max = .Machine$integer.max) {
  tryCatch(
    expr = {
      version <- rstudioapi::getVersion()
      version >= min && version < max
    },
    error = function(e) FALSE
  )
}

#' @importFrom methods setOldClass
setOldClass(c("spec_tbl_df", "tbl_df", "tbl", "data.frame"))

utctime <- function(year, month, day, hour, min, sec, psec) {
  utctime_(
    as.integer(year),
    as.integer(month),
    as.integer(day),
    as.integer(hour),
    as.integer(min),
    as.integer(sec),
    as.numeric(psec)
  )
}
