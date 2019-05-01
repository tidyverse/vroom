.onUnload <- function(libpath) {
    library.dynam.unload("vroom", libpath)
}

.onLoad <- function(...) {
  # only register conflicting S3 methods if readr is not already loaded.
  if (!"readr" %in% loadedNamespaces()) {
    s3_register("base::format", "col_spec")
    s3_register("base::print", "col_spec")
    s3_register("base::print", "collector")
    s3_register("base::print", "date_names")
    s3_register("base::print", "locale")
    s3_register("utils::str", "col_spec")
    s3_register("testthat::compare", "tbl_df")
  }
}

.onAttach <- function(libname, pkgname) {
  env <- as.environment(paste0("package:", pkgname))
  env[[".conflicts.OK"]] <- TRUE
}

s3_register <- function(generic, class, method = NULL) {
  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)

  pieces <- strsplit(generic, "::")[[1]]
  stopifnot(length(pieces) == 2)
  package <- pieces[[1]]
  generic <- pieces[[2]]

  if (is.null(method)) {
    method <- get(paste0(generic, ".", class), envir = parent.frame())
  }
  stopifnot(is.function(method))

  if (package %in% loadedNamespaces()) {
    registerS3method(generic, class, method, envir = asNamespace(package))
  }

  # Always register hook in case package is later unloaded & reloaded
  setHook(
    packageEvent(package, "onLoad"),
    function(...) {
      registerS3method(generic, class, method, envir = asNamespace(package))
    }
  )
}
