#' Read a fixed width file into a tibble
#'
#' @inheritParams vroom
#' @export
vroom_fwf <- function(file, col_positions, col_types = NULL,
                      col_keep = NULL, col_skip = NULL, id = NULL,
                      locale = default_locale(), na = c("", "NA"),
                      comment = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(n_max, 100),
                      num_threads = vroom_threads(),
                      progress = show_progress(),
                      .name_repair = "unique") {

  file <- standardise_path(file)

  if (length(file) == 0 || (n_max == 0 & identical(col_positions$col_names, FALSE))) {
    return(tibble::tibble())
  }

  if (n_max < 0 || is.infinite(n_max)) {
    n_max <- -1
  }

  out <- vroom_fwf_(file, col_positions$begin, col_positions$end,
    trim_ws = trim_ws, col_names = col_positions$col_names,
    col_types = col_types, col_keep = col_keep, col_skip = col_skip,
    id = id, na = na, guess_max = guess_max, skip = skip, comment = comment,
    n_max = n_max, num_threads = num_threads,
    altrep_opts = vroom_altrep_opts(), locale = locale)

  tibble::as_tibble(out, .name_repair = .name_repair)
}
