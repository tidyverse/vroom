#!/usr/bin/env Rscript

# Cross-engine CSV reader benchmark
#
# Compares CSV reading performance across R and Python engines:
#   R:      vroom_arrow, arrow::read_csv_arrow, vroom (libvroom->tibble), vroom (old->tibble)
#   Python: libvroom (vroom_csv), polars, pyarrow, duckdb
#
# Usage:
#   Rscript csv_reader_comparison.R
#   Rscript csv_reader_comparison.R --rows 1000000 --iterations 5
#   Rscript csv_reader_comparison.R --python /path/to/python3
#
# The Python engines require: pip install vroom-csv polars pyarrow duckdb

# -- Configuration -----------------------------------------------------------

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  opts <- list(
    rows = 5e6,
    iterations = 5,
    warmup = 1,
    python = NULL, # auto-detect
    output = NULL, # optional CSV output path
    skip_python = FALSE
  )

  i <- 1
  while (i <= length(args)) {
    switch(
      args[i],
      "--rows" = {
        opts$rows <- as.numeric(args[i + 1])
        i <- i + 2
      },
      "--iterations" = {
        opts$iterations <- as.integer(args[i + 1])
        i <- i + 2
      },
      "--warmup" = {
        opts$warmup <- as.integer(args[i + 1])
        i <- i + 2
      },
      "--python" = {
        opts$python <- args[i + 1]
        i <- i + 2
      },
      "--output" = {
        opts$output <- args[i + 1]
        i <- i + 2
      },
      "--skip-python" = {
        opts$skip_python <- TRUE
        i <- i + 1
      },
      {
        i <- i + 1
      }
    )
  }
  opts
}

# -- Data generation ---------------------------------------------------------

generate_test_files <- function(n, dir) {
  files <- list()

  # Mixed types: int, double, string, logical
  f <- file.path(dir, "mixed.csv")
  if (!file.exists(f)) {
    set.seed(42)
    df <- data.frame(
      id = 1:n,
      x = rnorm(n),
      y = rnorm(n),
      z = rnorm(n),
      name = sample(
        c("alice", "bob", "charlie", "diana", "eve"),
        n,
        replace = TRUE
      ),
      city = sample(
        c("new york", "london", "tokyo", "paris", "sydney"),
        n,
        replace = TRUE
      ),
      active = sample(c(TRUE, FALSE), n, replace = TRUE),
      score = runif(n, 0, 100)
    )
    data.table::fwrite(df, f)
  }
  files$mixed <- f

  # String-heavy: 4 string columns + 1 integer
  f <- file.path(dir, "string_heavy.csv")
  if (!file.exists(f)) {
    set.seed(123)
    make_words <- function(k) {
      vapply(
        seq_len(k),
        function(i) {
          paste0(
            sample(letters, sample(4:12, 1), replace = TRUE),
            collapse = ""
          )
        },
        character(1)
      )
    }
    words <- make_words(5000)
    df <- data.frame(
      a = sample(words, n, replace = TRUE),
      b = sample(words, n, replace = TRUE),
      c = sample(words, n, replace = TRUE),
      d = sample(words, n, replace = TRUE),
      e = sample(1:1000, n, replace = TRUE)
    )
    data.table::fwrite(df, f)
  }
  files$string_heavy <- f

  # Numeric-heavy: 10 double columns
  f <- file.path(dir, "numeric_heavy.csv")
  if (!file.exists(f)) {
    set.seed(456)
    df <- data.frame(
      a = rnorm(n),
      b = rnorm(n),
      c = rnorm(n),
      d = rnorm(n),
      e = rnorm(n),
      f = rnorm(n),
      g = rnorm(n),
      h = rnorm(n),
      i = rnorm(n),
      j = rnorm(n)
    )
    data.table::fwrite(df, f)
  }
  files$numeric_heavy <- f

  files
}

# -- Benchmark helpers -------------------------------------------------------

bench_median <- function(fn, warmup = 1, iterations = 5) {
  for (i in seq_len(warmup)) {
    fn()
  }
  times <- numeric(iterations)
  for (i in seq_len(iterations)) {
    times[i] <- system.time(fn())[["elapsed"]]
  }
  sort(times)[ceiling(iterations / 2)]
}

# -- R benchmarks ------------------------------------------------------------

run_r_benchmarks <- function(files, opts) {
  results <- list()

  has_arrow <- requireNamespace("arrow", quietly = TRUE)

  for (name in names(files)) {
    path <- files[[name]]
    size_mb <- file.size(path) / 1e6

    cat(sprintf("\n  %s (%.0f MB):\n", name, size_mb))

    # vroom_arrow (libvroom -> Arrow Table)
    if (has_arrow) {
      ms <- bench_median(
        function() vroom::vroom_arrow(path),
        warmup = opts$warmup,
        iterations = opts$iterations
      ) *
        1000
      cat(sprintf(
        "    vroom_arrow:       %7.1f ms  (%5.1f GB/s)\n",
        ms,
        size_mb / ms
      ))
      results[[length(results) + 1]] <- list(
        file = name,
        engine = "vroom_arrow",
        ms = ms,
        size_mb = size_mb
      )
    }

    # arrow::read_csv_arrow
    if (has_arrow) {
      ms <- bench_median(
        function() arrow::read_csv_arrow(path),
        warmup = opts$warmup,
        iterations = opts$iterations
      ) *
        1000
      cat(sprintf(
        "    arrow::read_csv:   %7.1f ms  (%5.1f GB/s)\n",
        ms,
        size_mb / ms
      ))
      results[[length(results) + 1]] <- list(
        file = name,
        engine = "arrow_read_csv",
        ms = ms,
        size_mb = size_mb
      )
    }

    # vroom with libvroom backend -> tibble
    ms <- bench_median(
      function() vroom::vroom(path, show_col_types = FALSE),
      warmup = opts$warmup,
      iterations = opts$iterations
    ) *
      1000
    cat(sprintf(
      "    vroom (libvroom):  %7.1f ms  (%5.1f GB/s)\n",
      ms,
      size_mb / ms
    ))
    results[[length(results) + 1]] <- list(
      file = name,
      engine = "vroom_libvroom",
      ms = ms,
      size_mb = size_mb
    )

    # vroom with old parser -> tibble (altrep=FALSE for fair materialized comparison)
    ms <- bench_median(
      function() vroom::vroom(path, show_col_types = FALSE, altrep = FALSE),
      warmup = opts$warmup,
      iterations = opts$iterations
    ) *
      1000
    cat(sprintf(
      "    vroom (old):       %7.1f ms  (%5.1f GB/s)\n",
      ms,
      size_mb / ms
    ))
    results[[length(results) + 1]] <- list(
      file = name,
      engine = "vroom_old",
      ms = ms,
      size_mb = size_mb
    )
  }

  results
}

# -- Python benchmarks -------------------------------------------------------

run_python_benchmarks <- function(files, opts) {
  # Find Python with required packages
  python <- opts$python
  if (is.null(python)) {
    # Try common locations
    candidates <- c(
      Sys.which("python3"),
      Sys.which("python"),
      "~/.local/bin/python3"
    )
    for (p in candidates) {
      if (nchar(p) > 0 && file.exists(p)) {
        # Check if at least one engine is available
        rc <- system2(
          p,
          c("-c", shQuote("import polars")),
          stdout = FALSE,
          stderr = FALSE
        )
        if (rc == 0) {
          python <- p
          break
        }
        rc <- system2(
          p,
          c("-c", shQuote("import vroom_csv")),
          stdout = FALSE,
          stderr = FALSE
        )
        if (rc == 0) {
          python <- p
          break
        }
      }
    }
  }

  if (is.null(python) || !file.exists(python)) {
    cat("\n  (Python not found or no CSV engines installed, skipping)\n")
    return(list())
  }

  py_script <- system.file(
    "bench",
    "csv_reader_comparison.py",
    package = "vroom"
  )
  if (!nzchar(py_script) || !file.exists(py_script)) {
    # Fallback: look relative to this script via commandArgs
    script_path <- sub(
      "--file=",
      "",
      grep("--file=", commandArgs(FALSE), value = TRUE)
    )
    if (length(script_path) == 1 && nzchar(script_path)) {
      py_script <- file.path(
        dirname(normalizePath(script_path)),
        "csv_reader_comparison.py"
      )
    }
  }

  if (!file.exists(py_script)) {
    cat("\n  (Python benchmark script not found, skipping)\n")
    return(list())
  }

  file_paths <- unlist(files)
  cmd <- c(python, py_script, file_paths)
  cat(sprintf("\n  Running Python benchmarks via: %s\n", python))

  json_output <- tryCatch(
    system2(cmd[1], cmd[-1], stdout = TRUE, stderr = FALSE),
    error = function(e) NULL
  )

  if (is.null(json_output)) {
    cat("  (Python benchmark failed)\n")
    return(list())
  }

  py_results <- jsonlite::fromJSON(paste(json_output, collapse = "\n"))

  results <- list()
  engine_map <- c(
    libvroom_py = "libvroom_py",
    polars = "polars",
    pyarrow = "pyarrow",
    duckdb = "duckdb"
  )

  for (i in seq_len(nrow(py_results))) {
    row <- py_results[i, ]
    # Match file back to label
    file_label <- names(files)[match(row$file, files)]
    size_mb <- row$size_bytes / 1e6

    for (eng_col in names(engine_map)) {
      if (
        eng_col %in%
          names(row) &&
          !is.null(row[[eng_col]]) &&
          !is.na(row[[eng_col]])
      ) {
        ms <- row[[eng_col]]
        cat(sprintf(
          "    %-18s %7.1f ms  (%5.1f GB/s)  [%s]\n",
          paste0(engine_map[eng_col], ":"),
          ms,
          size_mb / ms,
          file_label
        ))
        results[[length(results) + 1]] <- list(
          file = file_label,
          engine = engine_map[eng_col],
          ms = ms,
          size_mb = size_mb
        )
      }
    }
  }

  results
}

# -- Output formatting -------------------------------------------------------

format_results <- function(all_results, files) {
  df <- do.call(
    rbind,
    lapply(all_results, as.data.frame, stringsAsFactors = FALSE)
  )

  # Pivot: one row per engine, columns per file type
  engines <- unique(df$engine)
  file_labels <- names(files)

  # Display order
  engine_order <- c(
    "libvroom_py",
    "polars",
    "vroom_arrow",
    "pyarrow",
    "duckdb",
    "arrow_read_csv",
    "vroom_libvroom",
    "vroom_old"
  )
  engine_names <- c(
    libvroom_py = "libvroom (Python)",
    polars = "polars (Python)",
    vroom_arrow = "vroom_arrow (R)",
    pyarrow = "pyarrow (Python)",
    duckdb = "duckdb (Python)",
    arrow_read_csv = "arrow::read_csv (R)",
    vroom_libvroom = "vroom->tibble (R)",
    vroom_old = "vroom old->tibble (R)"
  )
  engines <- intersect(engine_order, engines)

  # Get file sizes for header
  sizes <- vapply(files, function(f) file.size(f) / 1e6, numeric(1))

  # Header
  cat("\n")
  cat(strrep("=", 80), "\n")
  cat("CSV Reader Benchmark Comparison\n")
  cat(strrep("=", 80), "\n\n")

  # Table header
  header <- sprintf("| %-26s |", "Engine")
  divider <- sprintf("| %s |", strrep("-", 26))
  for (fl in file_labels) {
    col_label <- sprintf("%s (%.0fMB)", fl, sizes[fl])
    header <- paste0(header, sprintf(" %22s |", col_label))
    divider <- paste0(divider, sprintf(" %s:|", strrep("-", 21)))
  }
  cat(header, "\n")
  cat(divider, "\n")

  # Rows
  for (eng in engines) {
    row <- sprintf("| %-26s |", engine_names[eng])
    for (fl in file_labels) {
      val <- df$ms[df$engine == eng & df$file == fl]
      sz <- sizes[fl]
      if (length(val) == 1 && !is.na(val)) {
        cell <- sprintf("%6.0fms (%4.1f GB/s)", val, sz / val)
        row <- paste0(row, sprintf(" %22s |", cell))
      } else {
        row <- paste0(row, sprintf(" %22s |", "--"))
      }
    }
    cat(row, "\n")
  }
  cat("\n")

  invisible(df)
}

# -- Main --------------------------------------------------------------------

main <- function() {
  opts <- parse_args()

  cat(sprintf(
    "CSV Reader Benchmark (%s rows, %d iterations)\n",
    format(opts$rows, big.mark = ","),
    opts$iterations
  ))
  cat(strrep("-", 50), "\n")

  # Generate test data
  bench_dir <- tempdir()
  cat(sprintf("\nGenerating test files in %s ...\n", bench_dir))
  files <- generate_test_files(opts$rows, bench_dir)

  for (name in names(files)) {
    cat(sprintf("  %s: %.0f MB\n", name, file.size(files[[name]]) / 1e6))
  }

  # R benchmarks
  cat("\nR engines:\n")
  r_results <- run_r_benchmarks(files, opts)

  # Python benchmarks
  py_results <- list()
  if (!opts$skip_python) {
    cat("\nPython engines:\n")
    py_results <- run_python_benchmarks(files, opts)
  }

  # Combined results
  all_results <- c(r_results, py_results)
  df <- format_results(all_results, files)

  # Optionally save
  if (!is.null(opts$output)) {
    utils::write.csv(df, opts$output, row.names = FALSE)
    cat(sprintf("Results saved to: %s\n", opts$output))
  }

  invisible(df)
}

if (!interactive()) {
  main()
}
