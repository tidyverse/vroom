#include "arrow_to_r.h"

#include <cpp11.hpp>
#include <libvroom/arrow_buffer.h>
#include <libvroom/arrow_column_builder.h>
#include <libvroom/types.h>

#include "vroom_arrow_chr.h"
#include "vroom_dict_chr.h"

#include <cstring>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace libvroom;

namespace {

SEXP int32_column_to_r(const ArrowInt32ColumnBuilder& col, size_t nrows) {
  cpp11::writable::integers result(nrows);
  const int32_t* src = col.values().data();
  int* dest = INTEGER(result);

  if (!col.null_bitmap().has_nulls()) {
    std::memcpy(dest, src, nrows * sizeof(int32_t));
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? src[i] : NA_INTEGER;
    }
  }
  return result;
}

SEXP int64_column_to_r(const ArrowInt64ColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const int64_t* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<double>(src[i]);
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<double>(src[i]) : NA_REAL;
    }
  }
  return result;
}

SEXP float64_column_to_r(const ArrowFloat64ColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const double* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    std::memcpy(dest, src, nrows * sizeof(double));
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? src[i] : NA_REAL;
    }
  }
  return result;
}

SEXP bool_column_to_r(const ArrowBoolColumnBuilder& col, size_t nrows) {
  cpp11::writable::logicals result(nrows);
  const uint8_t* src = col.values().data();
  int* dest = LOGICAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<int>(src[i]);
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<int>(src[i]) : NA_LOGICAL;
    }
  }
  return result;
}

SEXP string_column_to_r(const ArrowStringColumnBuilder& col, size_t nrows) {
  cpp11::writable::strings result(nrows);
  const StringBuffer& buf = col.values();

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      std::string_view sv = buf.get(i);
      SET_STRING_ELT(result, i,
                     Rf_mkCharLenCE(sv.data(), static_cast<int>(sv.size()),
                                    CE_UTF8));
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      if (nulls.is_valid(i)) {
        std::string_view sv = buf.get(i);
        SET_STRING_ELT(result, i,
                       Rf_mkCharLenCE(sv.data(), static_cast<int>(sv.size()),
                                      CE_UTF8));
      } else {
        SET_STRING_ELT(result, i, NA_STRING);
      }
    }
  }
  return result;
}

// Build dictionary and fill integer codes in a single pass.
// Pure C++, no R API calls, safe to call from worker threads.
// codes_dest must point to a pre-allocated int array of size nrows.
void build_factor_codes(const ArrowStringColumnBuilder& col, size_t nrows,
                        int* codes_dest,
                        std::vector<std::string_view>& levels_out) {
  const StringBuffer& buf = col.values();
  const NullBitmap& nulls = col.null_bitmap();
  bool has_nulls = nulls.has_nulls();

  std::unordered_map<std::string_view, int> dict;
  dict.reserve(256);
  levels_out.reserve(256);

  // Single pass: build dict + fill codes simultaneously
  for (size_t i = 0; i < nrows; i++) {
    if (has_nulls && !nulls.is_valid(i)) {
      codes_dest[i] = NA_INTEGER;
      continue;
    }
    std::string_view sv = buf.get(i);
    auto [it, inserted] = dict.try_emplace(sv, 0);
    if (inserted) {
      it->second = static_cast<int>(levels_out.size()) + 1;
      levels_out.push_back(sv);
    }
    codes_dest[i] = it->second;
  }
}

// Phase 2: Set R factor attributes on a pre-filled INTSXP (main thread only)
void finalize_factor(SEXP codes_sexp,
                     const std::vector<std::string_view>& levels_vec) {
  cpp11::writable::strings levels(levels_vec.size());
  for (size_t i = 0; i < levels_vec.size(); i++) {
    SET_STRING_ELT(
        levels, i,
        Rf_mkCharLenCE(levels_vec[i].data(),
                        static_cast<int>(levels_vec[i].size()), CE_UTF8));
  }
  Rf_setAttrib(codes_sexp, R_LevelsSymbol, levels);
  Rf_setAttrib(codes_sexp, R_ClassSymbol, Rf_mkString("factor"));
}

// Factor conversion: single pass builds dict + fills codes, then sets R attrs
SEXP string_column_to_factor_r(const ArrowStringColumnBuilder& col,
                                size_t nrows) {
  cpp11::writable::integers codes(nrows);
  std::vector<std::string_view> levels_vec;
  build_factor_codes(col, nrows, INTEGER(codes), levels_vec);
  finalize_factor(codes, levels_vec);
  return codes;
}

SEXP date_column_to_r(const ArrowDateColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const int32_t* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<double>(src[i]);
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<double>(src[i]) : NA_REAL;
    }
  }

  result.attr("class") = "Date";
  return result;
}

SEXP timestamp_column_to_r(const ArrowTimestampColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const int64_t* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<double>(src[i]) / 1e6;
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<double>(src[i]) / 1e6 : NA_REAL;
    }
  }

  result.attr("class") = cpp11::writable::strings({"POSIXct", "POSIXt"});
  result.attr("tzone") = "UTC";
  return result;
}

SEXP time_column_to_r(const ArrowTimeColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const double* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    std::memcpy(dest, src, nrows * sizeof(double));
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? src[i] : NA_REAL;
    }
  }

  result.attr("class") = cpp11::writable::strings({"hms", "difftime"});
  result.attr("units") = "secs";
  return result;
}

} // anonymous namespace

SEXP column_to_r(const ArrowColumnBuilder& column, size_t nrows,
                 bool strings_as_factors) {
  switch (column.type()) {
  case DataType::INT32:
    return int32_column_to_r(
        static_cast<const ArrowInt32ColumnBuilder&>(column), nrows);
  case DataType::INT64:
    return int64_column_to_r(
        static_cast<const ArrowInt64ColumnBuilder&>(column), nrows);
  case DataType::FLOAT64:
    return float64_column_to_r(
        static_cast<const ArrowFloat64ColumnBuilder&>(column), nrows);
  case DataType::BOOL:
    return bool_column_to_r(
        static_cast<const ArrowBoolColumnBuilder&>(column), nrows);
  case DataType::STRING:
    if (strings_as_factors) {
      return string_column_to_factor_r(
          static_cast<const ArrowStringColumnBuilder&>(column), nrows);
    }
    return string_column_to_r(
        static_cast<const ArrowStringColumnBuilder&>(column), nrows);
  case DataType::DATE:
    return date_column_to_r(
        static_cast<const ArrowDateColumnBuilder&>(column), nrows);
  case DataType::TIMESTAMP:
    return timestamp_column_to_r(
        static_cast<const ArrowTimestampColumnBuilder&>(column), nrows);
  case DataType::TIME:
    return time_column_to_r(
        static_cast<const ArrowTimeColumnBuilder&>(column), nrows);
  default:
    // For UNKNOWN/NA types, return character column
    if (auto* str_col =
            dynamic_cast<const ArrowStringColumnBuilder*>(&column)) {
      if (strings_as_factors) {
        return string_column_to_factor_r(*str_col, nrows);
      }
      return string_column_to_r(*str_col, nrows);
    }
    cpp11::stop("Unsupported column type: %d", static_cast<int>(column.type()));
  }
}

// Intern string_views into an R STRSXP (main thread only)
SEXP intern_levels(const std::vector<std::string_view>& levels_vec) {
  SEXP levels = PROTECT(Rf_allocVector(STRSXP, levels_vec.size()));
  for (size_t i = 0; i < levels_vec.size(); i++) {
    SET_STRING_ELT(
        levels, i,
        Rf_mkCharLenCE(levels_vec[i].data(),
                        static_cast<int>(levels_vec[i].size()), CE_UTF8));
  }
  UNPROTECT(1);
  return levels;
}

// ============================================================================
// Chunked numeric column conversion helpers
// Copy data from multiple chunks directly into a pre-allocated R vector.
// Avoids the O(n) merge_from() step entirely.
// ============================================================================

namespace {

// With converter function
template <typename ColType, typename SrcT, typename DestT, typename ConvertFn>
void copy_numeric_chunks(
    const std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>>& chunks,
    size_t col_idx, DestT* dest, DestT na_value, ConvertFn convert) {
  size_t dest_offset = 0;
  for (auto& chunk_cols : chunks) {
    auto& col = static_cast<const ColType&>(*chunk_cols[col_idx]);
    const SrcT* src = col.values().data();
    size_t n = col.size();
    const NullBitmap& nulls = col.null_bitmap();

    if (!nulls.has_nulls()) {
      for (size_t i = 0; i < n; i++) {
        dest[dest_offset++] = convert(src[i]);
      }
    } else {
      for (size_t i = 0; i < n; i++) {
        dest[dest_offset++] = nulls.is_valid(i) ? convert(src[i]) : na_value;
      }
    }
  }
}

// Without converter — uses memcpy when SrcT == DestT and no nulls
template <typename ColType, typename T>
void copy_numeric_chunks_direct(
    const std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>>& chunks,
    size_t col_idx, T* dest, T na_value) {
  size_t dest_offset = 0;
  for (auto& chunk_cols : chunks) {
    auto& col = static_cast<const ColType&>(*chunk_cols[col_idx]);
    const T* src = col.values().data();
    size_t n = col.size();
    const NullBitmap& nulls = col.null_bitmap();

    if (!nulls.has_nulls()) {
      std::memcpy(dest + dest_offset, src, n * sizeof(T));
      dest_offset += n;
    } else {
      for (size_t i = 0; i < n; i++) {
        dest[dest_offset++] = nulls.is_valid(i) ? src[i] : na_value;
      }
    }
  }
}

} // anonymous namespace (chunked helpers)

// Convert ParsedChunks directly to an R data frame without merging.
// String columns wrapped in multi-chunk ALTREP. Numeric columns copied directly.
cpp11::writable::list columns_to_r_chunked(
    std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>>& chunks,
    const std::vector<ColumnSchema>& schema,
    size_t total_rows) {

  size_t ncols = schema.size();
  cpp11::writable::list result(ncols);
  cpp11::writable::strings names(ncols);

  for (size_t i = 0; i < ncols; i++) {
    names[static_cast<R_xlen_t>(i)] = schema[i].name;
    DataType type = chunks[0][i]->type();

    if (type == DataType::STRING) {
      // Collect string column builders from all chunks into shared_ptrs
      std::vector<std::shared_ptr<ArrowStringColumnBuilder>> str_chunks;
      str_chunks.reserve(chunks.size());
      for (auto& chunk_cols : chunks) {
        str_chunks.push_back(std::shared_ptr<ArrowStringColumnBuilder>(
            static_cast<ArrowStringColumnBuilder*>(chunk_cols[i].release())));
      }
      result[static_cast<R_xlen_t>(i)] =
          vroom_arrow_chr::Make(std::move(str_chunks), total_rows);

    } else if (type == DataType::INT32) {
      cpp11::writable::integers r_vec(total_rows);
      copy_numeric_chunks_direct<ArrowInt32ColumnBuilder, int>(
          chunks, i, INTEGER(r_vec), NA_INTEGER);
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else if (type == DataType::INT64) {
      cpp11::writable::doubles r_vec(total_rows);
      copy_numeric_chunks<ArrowInt64ColumnBuilder, int64_t, double>(
          chunks, i, REAL(r_vec), NA_REAL,
          [](int64_t v) { return static_cast<double>(v); });
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else if (type == DataType::FLOAT64) {
      cpp11::writable::doubles r_vec(total_rows);
      copy_numeric_chunks_direct<ArrowFloat64ColumnBuilder, double>(
          chunks, i, REAL(r_vec), NA_REAL);
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else if (type == DataType::BOOL) {
      cpp11::writable::logicals r_vec(total_rows);
      copy_numeric_chunks<ArrowBoolColumnBuilder, uint8_t, int>(
          chunks, i, LOGICAL(r_vec), NA_LOGICAL,
          [](uint8_t v) { return static_cast<int>(v); });
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else if (type == DataType::DATE) {
      cpp11::writable::doubles r_vec(total_rows);
      copy_numeric_chunks<ArrowDateColumnBuilder, int32_t, double>(
          chunks, i, REAL(r_vec), NA_REAL,
          [](int32_t v) { return static_cast<double>(v); });
      r_vec.attr("class") = "Date";
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else if (type == DataType::TIMESTAMP) {
      cpp11::writable::doubles r_vec(total_rows);
      copy_numeric_chunks<ArrowTimestampColumnBuilder, int64_t, double>(
          chunks, i, REAL(r_vec), NA_REAL,
          [](int64_t v) { return static_cast<double>(v) / 1e6; });
      r_vec.attr("class") =
          cpp11::writable::strings({"POSIXct", "POSIXt"});
      r_vec.attr("tzone") = "UTC";
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else if (type == DataType::TIME) {
      cpp11::writable::doubles r_vec(total_rows);
      copy_numeric_chunks_direct<ArrowTimeColumnBuilder, double>(
          chunks, i, REAL(r_vec), NA_REAL);
      r_vec.attr("class") =
          cpp11::writable::strings({"hms", "difftime"});
      r_vec.attr("units") = "secs";
      result[static_cast<R_xlen_t>(i)] = r_vec;

    } else {
      // Unknown type: try as string (same ALTREP path)
      std::vector<std::shared_ptr<ArrowStringColumnBuilder>> str_chunks;
      str_chunks.reserve(chunks.size());
      for (auto& chunk_cols : chunks) {
        auto* str_col =
            dynamic_cast<ArrowStringColumnBuilder*>(chunk_cols[i].get());
        if (str_col) {
          chunk_cols[i].release();
          str_chunks.push_back(std::shared_ptr<ArrowStringColumnBuilder>(str_col));
        }
      }
      if (!str_chunks.empty()) {
        result[static_cast<R_xlen_t>(i)] =
            vroom_arrow_chr::Make(std::move(str_chunks), total_rows);
      } else {
        cpp11::stop("Unsupported column type: %d", static_cast<int>(type));
      }
    }
  }

  result.attr("names") = names;
  result.attr("class") =
      cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
  result.attr("row.names") =
      cpp11::writable::integers({NA_INTEGER, -static_cast<int>(total_rows)});
  return result;
}

cpp11::writable::list columns_to_r(
    std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns,
    const std::vector<ColumnSchema>& schema,
    size_t nrows,
    bool strings_as_factors,
    bool use_altrep) {

  size_t ncols = columns.size();
  cpp11::writable::list result(ncols);
  cpp11::writable::strings names(ncols);

  // When neither ALTREP nor factors are requested, use the old sequential path
  if (!strings_as_factors && !use_altrep) {
    for (size_t i = 0; i < ncols; i++) {
      result[static_cast<R_xlen_t>(i)] =
          column_to_r(*columns[i], nrows, false);
      names[static_cast<R_xlen_t>(i)] = schema[i].name;
    }

    result.attr("names") = names;
    result.attr("class") =
        cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
    result.attr("row.names") =
        cpp11::writable::integers({NA_INTEGER, -static_cast<int>(nrows)});
    return result;
  }

  // Arrow-backed ALTREP: wrap string columns directly, no dict building.
  // Creation is near-instant. Rf_mkCharLenCE calls are deferred until access.
  if (use_altrep && !strings_as_factors) {
    for (size_t i = 0; i < ncols; i++) {
      names[static_cast<R_xlen_t>(i)] = schema[i].name;
      if (columns[i]->type() == DataType::STRING) {
        // Move ownership of the ArrowStringColumnBuilder into the ALTREP vector
        auto str_col = std::shared_ptr<ArrowStringColumnBuilder>(
            static_cast<ArrowStringColumnBuilder*>(columns[i].release()));
        result[static_cast<R_xlen_t>(i)] =
            vroom_arrow_chr::Make(std::move(str_col), nrows);
      } else {
        result[static_cast<R_xlen_t>(i)] =
            column_to_r(*columns[i], nrows, false);
      }
    }

    result.attr("names") = names;
    result.attr("class") =
        cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
    result.attr("row.names") =
        cpp11::writable::integers({NA_INTEGER, -static_cast<int>(nrows)});
    return result;
  }

  // Factor path with parallel dict-building: one thread per string column
  // builds hash map + fills integer codes (pure C++, no R API, zero contention).

  struct StringColTask {
    size_t col_idx;
    const ArrowStringColumnBuilder* col;
    SEXP codes_sexp;
    std::vector<std::string_view> levels;
  };
  std::vector<StringColTask> string_tasks;

  // Pre-allocate R integer vectors for string columns (main thread)
  for (size_t i = 0; i < ncols; i++) {
    names[static_cast<R_xlen_t>(i)] = schema[i].name;
    if (columns[i]->type() == DataType::STRING) {
      cpp11::writable::integers codes(nrows);
      SEXP codes_sexp = codes;
      result[static_cast<R_xlen_t>(i)] = codes_sexp; // GC-protect
      string_tasks.push_back(
          {i,
           static_cast<const ArrowStringColumnBuilder*>(columns[i].get()),
           codes_sexp, {}});
    }
  }

  // Launch one thread per string column (pure C++, no R API)
  std::vector<std::thread> threads;
  for (auto& task : string_tasks) {
    threads.emplace_back([&task, nrows]() {
      build_factor_codes(*task.col, nrows, INTEGER(task.codes_sexp),
                         task.levels);
    });
  }

  // Main thread: convert non-string columns while string threads run
  for (size_t i = 0; i < ncols; i++) {
    if (columns[i]->type() != DataType::STRING) {
      result[static_cast<R_xlen_t>(i)] =
          column_to_r(*columns[i], nrows, false);
    }
  }

  // Wait for string threads
  for (auto& t : threads) {
    t.join();
  }

  // Finalize factors on main thread (R API: create levels STRSXP)
  for (auto& task : string_tasks) {
    finalize_factor(task.codes_sexp, task.levels);
  }

  result.attr("names") = names;
  result.attr("class") =
      cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
  result.attr("row.names") =
      cpp11::writable::integers({NA_INTEGER, -static_cast<int>(nrows)});

  return result;
}
