#pragma once

#include <cpp11.hpp>
#include <libvroom/arrow_column_builder.h>
#include <libvroom/types.h>
#include <memory>
#include <vector>

// Convert a vector of ArrowColumnBuilders to an R data frame (tibble).
// String column handling:
//   use_altrep=true (default): Arrow-backed ALTREP (deferred materialization,
//     near-instant creation by wrapping ArrowStringColumnBuilder directly)
//   strings_as_factors=true: R factors with parallel dict building
//   both false: sequential Rf_mkCharLenCE per row (baseline for benchmarking)
// Note: takes non-const ref because ALTREP mode moves ownership of string
// columns into ALTREP vectors.
cpp11::writable::list columns_to_r(
    std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>& columns,
    const std::vector<libvroom::ColumnSchema>& schema,
    size_t nrows,
    bool strings_as_factors = false,
    bool use_altrep = true);

// Convert a single ArrowColumnBuilder to an R SEXP based on its type
SEXP column_to_r(const libvroom::ArrowColumnBuilder& column, size_t nrows,
                 bool strings_as_factors = false);

// Convert ParsedChunks directly to an R data frame without merging.
// String columns are wrapped in multi-chunk Arrow ALTREP (zero-copy).
// Numeric columns are copied from chunks directly into R vectors.
cpp11::writable::list columns_to_r_chunked(
    std::vector<std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>>& chunks,
    const std::vector<libvroom::ColumnSchema>& schema,
    size_t total_rows);
