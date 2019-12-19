env_to_logical <- function (var, default = TRUE) {
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

is_named <- function (x) {
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
compare.tbl_df <- function (x, y, ...) {
  attr(x, "spec") <- NULL
  attr(y, "spec") <- NULL
  NextMethod("compare")
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
