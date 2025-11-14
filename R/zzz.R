.onUnload <- function(libpath) {
  library.dynam.unload("vroom", libpath)
}

.onLoad <- function(...) {
  tzdb::tzdb_initialize()

  # only register conflicting S3 methods if readr is not already loaded.
  if (!"readr" %in% loadedNamespaces()) {
    s3_register("base::format", "col_spec")
    s3_register("base::print", "col_spec")
    s3_register("base::print", "collector")
    s3_register("base::print", "date_names")
    s3_register("base::print", "locale")
    s3_register("utils::str", "col_spec")
    s3_register("base::all.equal", "spec_tbl_df")
    s3_register("base::as.data.frame", "spec_tbl_df")
    s3_register("tibble::as_tibble", "spec_tbl_df")
    s3_register("testthat::compare", "spec_tbl_df")
    s3_register("waldo::compare_proxy", "spec_tbl_df")
  }
}

.conflicts.OK <- TRUE

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
