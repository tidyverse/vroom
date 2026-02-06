#!/usr/bin/env Rscript

# Benchmark script for vroom CSV parser performance
# Usage: Rscript libvroom_benchmark.R [output_file]

library(bench)
library(vroom)
library(tibble)

# Configuration
TEST_DIR <- getwd()
DEFAULT_ITERATIONS <- 3

# Helper to generate test files
generate_test_file <- function(rows, cols, filename, seed = 42) {
  filepath <- file.path(TEST_DIR, filename)
  if (!file.exists(filepath)) {
    set.seed(seed)
    cat(sprintf("Generating %s (%d rows x %d cols)...\n", filename, rows, cols))

    # Create varied column types
    df <- data.frame(
      id = 1:rows,
      stringsAsFactors = FALSE
    )

    # Add string columns
    words <- c(
      "apple",
      "banana",
      "cherry",
      "date",
      "elderberry",
      "fig",
      "grape",
      "honeydew",
      "kiwi",
      "lemon"
    )
    for (i in seq_len(min(cols - 1, 5))) {
      df[[paste0("str", i)]] <- sample(words, rows, replace = TRUE)
    }

    # Add numeric columns
    for (i in seq_len(max(0, min(cols - 6, 10)))) {
      df[[paste0("num", i)]] <- round(rnorm(rows, mean = 100, sd = 50), 6)
    }

    # Fill remaining with integers
    remaining <- cols - ncol(df)
    if (remaining > 0) {
      for (i in seq_len(remaining)) {
        df[[paste0("int", i)]] <- sample.int(10000, rows, replace = TRUE)
      }
    }

    vroom::vroom_write(df, filepath, delim = ",")
    cat(sprintf("  File size: %.1f MB\n", file.size(filepath) / 1e6))
  }
  filepath
}

# Benchmark construction time
benchmark_construction <- function(
  filepath,
  threads,
  iterations = DEFAULT_ITERATIONS
) {
  filename <- basename(filepath)

  bench::mark(
    vroom = vroom(
      filepath,
      num_threads = threads,
      show_col_types = FALSE,
      altrep = FALSE
    ),
    check = FALSE,
    iterations = iterations,
    filter_gc = FALSE
  )
}

# Main benchmark runner
run_benchmarks <- function() {
  cat("=== vroom Performance Benchmark ===\n\n")

  # Generate test files
  files <- list(
    small = generate_test_file(100000, 10, "bench_100k_10col.csv"),
    medium = generate_test_file(500000, 10, "bench_500k_10col.csv"),
    large = generate_test_file(1000000, 10, "bench_1M_10col.csv"),
    wide = generate_test_file(100000, 50, "bench_100k_50col.csv")
  )

  cat("\n")

  results <- list()

  # Test thread scaling
  for (threads in c(1, 2, 4, 8)) {
    cat(sprintf("\n=== %d Thread(s) ===\n", threads))

    for (name in names(files)) {
      filepath <- files[[name]]
      filesize_mb <- round(file.size(filepath) / 1e6, 1)
      cat(sprintf(
        "\n[%s] %s (%.1f MB)\n",
        name,
        basename(filepath),
        filesize_mb
      ))

      # Construction benchmark
      cat("  Construction: ")
      result <- tryCatch(
        {
          bm <- benchmark_construction(filepath, threads)

          # Extract median time
          median_time <- as.numeric(bm$median[1])

          cat(sprintf("%.0fms\n", median_time * 1000))

          list(
            file = name,
            threads = threads,
            test = "construction",
            median_ms = median_time * 1000
          )
        },
        error = function(e) {
          cat(sprintf("ERROR: %s\n", e$message))
          NULL
        }
      )

      if (!is.null(result)) {
        results[[length(results) + 1]] <- result
      }
    }
  }

  # Summary
  cat("\n\n=== Summary ===\n")
  summary_df <- do.call(rbind, lapply(results, as.data.frame))
  print(as_tibble(summary_df))

  invisible(summary_df)
}

# Quick benchmark for development
quick_benchmark <- function() {
  cat("=== Quick Benchmark ===\n\n")

  filepath <- generate_test_file(100000, 10, "bench_quick.csv")
  filesize_mb <- round(file.size(filepath) / 1e6, 1)

  cat(sprintf("File: %s (%.1f MB)\n\n", basename(filepath), filesize_mb))

  for (threads in c(1, 4)) {
    cat(sprintf("Threads: %d\n", threads))

    # Warm up
    invisible(vroom(
      filepath,
      num_threads = threads,
      show_col_types = FALSE
    ))

    # Benchmark
    bm <- benchmark_construction(filepath, threads, iterations = 5)

    median_time <- as.numeric(bm$median[1])

    cat(sprintf("  Time: %.0f ms\n\n", median_time * 1000))
  }
}

# Run if executed directly
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)

  if (length(args) > 0 && args[1] == "quick") {
    quick_benchmark()
  } else {
    results <- run_benchmarks()

    if (length(args) > 0 && args[1] != "quick") {
      saveRDS(results, args[1])
      cat(sprintf("\nResults saved to: %s\n", args[1]))
    }
  }
}
