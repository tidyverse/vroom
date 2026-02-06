#include <cpp11.hpp>
#include <libvroom/encoding.h>
#include <libvroom/vroom.h>

#include "libvroom_helpers.h"
#include "vroom_arrow_chr.h"

[[cpp11::register]] SEXP vroom_lines_libvroom_(
    SEXP input,
    int skip,
    int n_max,
    const std::string& na_values,
    bool skip_empty_rows,
    int num_threads,
    bool use_altrep) {

  libvroom::CsvOptions opts;
  opts.separator = '\x01'; // SOH — never appears in text
  opts.quote = '\0';       // No quoting
  opts.has_header = false;
  opts.skip_empty_rows = skip_empty_rows;
  opts.trim_ws = false;
  if (skip > 0)
    opts.skip = static_cast<size_t>(skip);
  // Always set null_values, even when empty, to override the default
  // which includes empty string as a null value
  opts.null_values = na_values;
  if (num_threads > 0)
    opts.num_threads = static_cast<size_t>(num_threads);

  opts.encoding = libvroom::CharEncoding::UTF8;

  libvroom::CsvReader reader(opts);

  open_input_source(reader, input);

  // Force the single column to STRING type (skip type inference)
  auto schema_copy = reader.schema();
  if (!schema_copy.empty()) {
    schema_copy[0].type = libvroom::DataType::STRING;
    reader.set_schema(schema_copy);
  }

  auto stream_result = reader.start_streaming();
  if (!stream_result) {
    cpp11::stop("Failed to start streaming: %s", stream_result.error.c_str());
  }

  size_t total_rows = reader.row_count();

  if (total_rows == 0) {
    // Drain any remaining chunks
    while (reader.next_chunk()) {}
    return Rf_allocVector(STRSXP, 0);
  }

  // n_max < 0 means unlimited
  bool has_limit = (n_max >= 0);
  size_t row_limit = has_limit ? static_cast<size_t>(n_max) : SIZE_MAX;

  if (use_altrep) {
    // Accumulate string builders for ALTREP wrapping.
    // Stop collecting chunks once we have enough rows for n_max.
    std::vector<std::shared_ptr<libvroom::ArrowStringColumnBuilder>> accum;
    size_t rows_collected = 0;

    while (auto chunk = reader.next_chunk()) {
      auto& columns = chunk.value();
      if (columns.empty())
        continue;

      rows_collected += columns[0]->size();
      accum.push_back(
          std::shared_ptr<libvroom::ArrowStringColumnBuilder>(
              static_cast<libvroom::ArrowStringColumnBuilder*>(
                  columns[0].release())));

      if (has_limit && rows_collected >= row_limit)
        break;
    }

    // Drain remaining chunks to clean up reader state
    while (reader.next_chunk()) {}

    if (accum.empty()) {
      return Rf_allocVector(STRSXP, 0);
    }

    // Compute total from actual chunk sizes, capped by n_max.
    // vroom_arrow_chr uses this as the reported Length() — element access
    // via string_Elt still works correctly for indices < actual data size.
    size_t actual_rows = 0;
    for (const auto& c : accum) {
      actual_rows += c->size();
    }
    if (has_limit && actual_rows > row_limit) {
      actual_rows = row_limit;
    }

    return vroom_arrow_chr::Make(std::move(accum), actual_rows);
  }

  // Non-ALTREP: materialize immediately with early termination
  std::vector<std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>> chunks;
  size_t rows_collected = 0;

  while (auto chunk = reader.next_chunk()) {
    auto& columns = chunk.value();
    if (!columns.empty()) {
      rows_collected += columns[0]->size();
    }
    chunks.push_back(std::move(columns));

    if (has_limit && rows_collected >= row_limit)
      break;
  }

  // Drain remaining chunks
  while (reader.next_chunk()) {}

  // Count actual rows, capped by n_max
  size_t actual_rows = 0;
  for (const auto& chunk : chunks) {
    if (!chunk.empty()) {
      actual_rows += chunk[0]->size();
    }
  }
  if (has_limit && actual_rows > row_limit) {
    actual_rows = row_limit;
  }

  if (actual_rows == 0) {
    return Rf_allocVector(STRSXP, 0);
  }

  // Build materialized STRSXP
  SEXP result = PROTECT(Rf_allocVector(STRSXP, static_cast<R_xlen_t>(actual_rows)));
  R_xlen_t dest_idx = 0;
  size_t rows_remaining = actual_rows;

  for (auto& chunk_cols : chunks) {
    if (chunk_cols.empty() || rows_remaining == 0)
      continue;
    auto& col = static_cast<libvroom::ArrowStringColumnBuilder&>(*chunk_cols[0]);
    const libvroom::StringBuffer& buf = col.values();
    const libvroom::NullBitmap& nulls = col.null_bitmap();
    size_t chunk_size = std::min(col.size(), rows_remaining);

    if (!nulls.has_nulls()) {
      for (size_t j = 0; j < chunk_size; j++) {
        std::string_view sv = buf.get(j);
        SET_STRING_ELT(result, dest_idx++,
                       Rf_mkCharLenCE(sv.data(),
                                      static_cast<int>(sv.size()), CE_UTF8));
      }
    } else {
      for (size_t j = 0; j < chunk_size; j++) {
        if (nulls.is_valid(j)) {
          std::string_view sv = buf.get(j);
          SET_STRING_ELT(result, dest_idx++,
                         Rf_mkCharLenCE(sv.data(),
                                        static_cast<int>(sv.size()), CE_UTF8));
        } else {
          SET_STRING_ELT(result, dest_idx++, NA_STRING);
        }
      }
    }
    rows_remaining -= chunk_size;
  }

  UNPROTECT(1);
  return result;
}
