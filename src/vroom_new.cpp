#include <cpp11.hpp>
#include <libvroom/encoding.h>
#include <libvroom/vroom.h>

#include "arrow_to_r.h"
#include "vroom_arrow_chr.h"

#include <cstring>

[[cpp11::register]] cpp11::sexp vroom_libvroom_(
    SEXP input,
    const std::string& delim,
    char quote,
    bool has_header,
    int skip,
    const std::string& comment,
    bool skip_empty_rows,
    bool trim_ws,
    const std::string& na_values,
    int num_threads,
    bool strings_as_factors,
    bool use_altrep) {

  libvroom::CsvOptions opts;
  if (!delim.empty())
    opts.separator = delim[0];
  opts.quote = quote;
  opts.has_header = has_header;
  opts.skip_empty_rows = skip_empty_rows;
  opts.trim_ws = trim_ws;
  if (!comment.empty())
    opts.comment = comment[0];
  if (!na_values.empty())
    opts.null_values = na_values;
  if (num_threads > 0)
    opts.num_threads = static_cast<size_t>(num_threads);

  // Skip full-file encoding detection (simdutf::validate_utf8 scans entire
  // file). R already handles encoding at the connection level.
  opts.encoding = libvroom::CharEncoding::UTF8;

  libvroom::CsvReader reader(opts);

  if (TYPEOF(input) == RAWSXP) {
    // Raw vector from connection - create aligned buffer
    size_t data_size = Rf_xlength(input);
    auto buffer = libvroom::AlignedBuffer::allocate(data_size);
    std::memcpy(buffer.data(), RAW(input), data_size);
    auto open_result = reader.open_from_buffer(std::move(buffer));
    if (!open_result) {
      cpp11::stop("Failed to open buffer: %s", open_result.error.c_str());
    }
  } else {
    // File path
    std::string path = cpp11::as_cpp<std::string>(input);
    auto open_result = reader.open(path);
    if (!open_result) {
      cpp11::stop("Failed to open file: %s", open_result.error.c_str());
    }
  }

  const auto& schema = reader.schema();

  // Start streaming: runs SIMD analysis synchronously, dispatches parse tasks
  auto stream_result = reader.start_streaming();
  if (!stream_result) {
    cpp11::stop("Failed to start streaming: %s", stream_result.error.c_str());
  }

  size_t total_rows = reader.row_count();
  size_t ncols = schema.size();

  if (total_rows == 0) {
    // Return empty tibble with correct column names
    cpp11::writable::list result(ncols);
    cpp11::writable::strings names(ncols);
    for (size_t i = 0; i < ncols; i++) {
      switch (schema[i].type) {
      case libvroom::DataType::INT32:
        result[static_cast<R_xlen_t>(i)] = Rf_allocVector(INTSXP, 0);
        break;
      case libvroom::DataType::FLOAT64:
        result[static_cast<R_xlen_t>(i)] = Rf_allocVector(REALSXP, 0);
        break;
      case libvroom::DataType::BOOL:
        result[static_cast<R_xlen_t>(i)] = Rf_allocVector(LGLSXP, 0);
        break;
      default:
        result[static_cast<R_xlen_t>(i)] = Rf_allocVector(STRSXP, 0);
        break;
      }
      names[static_cast<R_xlen_t>(i)] = schema[i].name;
    }
    result.attr("names") = names;
    result.attr("class") =
        cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
    result.attr("row.names") =
        cpp11::writable::integers({NA_INTEGER, 0});
    // Drain any remaining chunks
    while (reader.next_chunk()) {}
    return result;
  }

  // ALTREP path: stream chunks incrementally.
  // Pre-allocate R vectors for numerics, accumulate string builders for ALTREP.
  if (use_altrep && !strings_as_factors) {
    cpp11::writable::list result(ncols);
    cpp11::writable::strings names(ncols);

    // Pre-allocate numeric R vectors and string builder accumulators
    std::vector<SEXP> numeric_vecs(ncols, R_NilValue);
    std::vector<std::vector<std::shared_ptr<libvroom::ArrowStringColumnBuilder>>>
        string_accumulators(ncols);

    for (size_t i = 0; i < ncols; i++) {
      names[static_cast<R_xlen_t>(i)] = schema[i].name;
      switch (schema[i].type) {
      case libvroom::DataType::INT32: {
        cpp11::writable::integers v(total_rows);
        numeric_vecs[i] = v;
        result[static_cast<R_xlen_t>(i)] = v; // GC-protect
        break;
      }
      case libvroom::DataType::INT64:
      case libvroom::DataType::FLOAT64:
      case libvroom::DataType::DATE:
      case libvroom::DataType::TIMESTAMP: {
        cpp11::writable::doubles v(total_rows);
        numeric_vecs[i] = v;
        result[static_cast<R_xlen_t>(i)] = v; // GC-protect
        break;
      }
      case libvroom::DataType::BOOL: {
        cpp11::writable::logicals v(total_rows);
        numeric_vecs[i] = v;
        result[static_cast<R_xlen_t>(i)] = v; // GC-protect
        break;
      }
      default:
        // String columns: will accumulate builders for ALTREP
        break;
      }
    }

    // Stream chunks, copying numeric data at running offset
    size_t row_offset = 0;
    while (auto chunk = reader.next_chunk()) {
      auto& columns = chunk.value();
      if (columns.empty())
        continue;
      size_t chunk_rows = columns[0]->size();

      for (size_t i = 0; i < ncols; i++) {
        auto type = columns[i]->type();

        if (type == libvroom::DataType::STRING) {
          // Accumulate string column builder for later ALTREP wrapping
          string_accumulators[i].push_back(
              std::shared_ptr<libvroom::ArrowStringColumnBuilder>(
                  static_cast<libvroom::ArrowStringColumnBuilder*>(
                      columns[i].release())));

        } else if (type == libvroom::DataType::INT32) {
          auto& col = static_cast<libvroom::ArrowInt32ColumnBuilder&>(*columns[i]);
          int* dest = INTEGER(numeric_vecs[i]) + row_offset;
          const int32_t* src = col.values().data();
          if (!col.null_bitmap().has_nulls()) {
            std::memcpy(dest, src, chunk_rows * sizeof(int32_t));
          } else {
            const auto& nulls = col.null_bitmap();
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = nulls.is_valid(r) ? src[r] : NA_INTEGER;
            }
          }

        } else if (type == libvroom::DataType::INT64) {
          auto& col = static_cast<libvroom::ArrowInt64ColumnBuilder&>(*columns[i]);
          double* dest = REAL(numeric_vecs[i]) + row_offset;
          const int64_t* src = col.values().data();
          if (!col.null_bitmap().has_nulls()) {
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = static_cast<double>(src[r]);
            }
          } else {
            const auto& nulls = col.null_bitmap();
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = nulls.is_valid(r) ? static_cast<double>(src[r]) : NA_REAL;
            }
          }

        } else if (type == libvroom::DataType::FLOAT64) {
          auto& col = static_cast<libvroom::ArrowFloat64ColumnBuilder&>(*columns[i]);
          double* dest = REAL(numeric_vecs[i]) + row_offset;
          const double* src = col.values().data();
          if (!col.null_bitmap().has_nulls()) {
            std::memcpy(dest, src, chunk_rows * sizeof(double));
          } else {
            const auto& nulls = col.null_bitmap();
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = nulls.is_valid(r) ? src[r] : NA_REAL;
            }
          }

        } else if (type == libvroom::DataType::BOOL) {
          auto& col = static_cast<libvroom::ArrowBoolColumnBuilder&>(*columns[i]);
          int* dest = LOGICAL(numeric_vecs[i]) + row_offset;
          const uint8_t* src = col.values().data();
          if (!col.null_bitmap().has_nulls()) {
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = static_cast<int>(src[r]);
            }
          } else {
            const auto& nulls = col.null_bitmap();
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = nulls.is_valid(r) ? static_cast<int>(src[r]) : NA_LOGICAL;
            }
          }

        } else if (type == libvroom::DataType::DATE) {
          auto& col = static_cast<libvroom::ArrowDateColumnBuilder&>(*columns[i]);
          double* dest = REAL(numeric_vecs[i]) + row_offset;
          const int32_t* src = col.values().data();
          if (!col.null_bitmap().has_nulls()) {
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = static_cast<double>(src[r]);
            }
          } else {
            const auto& nulls = col.null_bitmap();
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = nulls.is_valid(r) ? static_cast<double>(src[r]) : NA_REAL;
            }
          }

        } else if (type == libvroom::DataType::TIMESTAMP) {
          auto& col = static_cast<libvroom::ArrowTimestampColumnBuilder&>(*columns[i]);
          double* dest = REAL(numeric_vecs[i]) + row_offset;
          const int64_t* src = col.values().data();
          if (!col.null_bitmap().has_nulls()) {
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = static_cast<double>(src[r]) / 1e6;
            }
          } else {
            const auto& nulls = col.null_bitmap();
            for (size_t r = 0; r < chunk_rows; r++) {
              dest[r] = nulls.is_valid(r) ? static_cast<double>(src[r]) / 1e6 : NA_REAL;
            }
          }

        } else {
          // Unknown type: try as string (same accumulator path)
          auto* str_col = dynamic_cast<libvroom::ArrowStringColumnBuilder*>(
              columns[i].get());
          if (str_col) {
            (void)columns[i].release();
            string_accumulators[i].push_back(
                std::shared_ptr<libvroom::ArrowStringColumnBuilder>(str_col));
          }
        }
      }

      row_offset += chunk_rows;
    }

    // Set Date/Timestamp class attributes on numeric vectors
    for (size_t i = 0; i < ncols; i++) {
      if (schema[i].type == libvroom::DataType::DATE) {
        Rf_setAttrib(numeric_vecs[i], R_ClassSymbol, Rf_mkString("Date"));
      } else if (schema[i].type == libvroom::DataType::TIMESTAMP) {
        cpp11::writable::strings cls({"POSIXct", "POSIXt"});
        Rf_setAttrib(numeric_vecs[i], R_ClassSymbol, cls);
        Rf_setAttrib(numeric_vecs[i], Rf_install("tzone"), Rf_mkString("UTC"));
      }
    }

    // Wrap string columns in multi-chunk ALTREP
    for (size_t i = 0; i < ncols; i++) {
      if (!string_accumulators[i].empty()) {
        result[static_cast<R_xlen_t>(i)] =
            vroom_arrow_chr::Make(std::move(string_accumulators[i]), total_rows);
      }
    }

    result.attr("names") = names;
    result.attr("class") =
        cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
    result.attr("row.names") =
        cpp11::writable::integers({NA_INTEGER, -static_cast<int>(total_rows)});
    return result;
  }

  // Non-ALTREP paths: collect all chunks, then use existing conversion.
  // This unifies factor and non-ALTREP paths on the streaming API.
  std::vector<std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>> chunks;
  while (auto chunk = reader.next_chunk()) {
    chunks.push_back(std::move(chunk.value()));
  }

  if (chunks.empty()) {
    // Edge case: no data chunks despite non-zero row_count
    cpp11::writable::list result(ncols);
    cpp11::writable::strings names(ncols);
    for (size_t i = 0; i < ncols; i++) {
      result[static_cast<R_xlen_t>(i)] = Rf_allocVector(STRSXP, 0);
      names[static_cast<R_xlen_t>(i)] = schema[i].name;
    }
    result.attr("names") = names;
    result.attr("class") =
        cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
    result.attr("row.names") =
        cpp11::writable::integers({NA_INTEGER, 0});
    return result;
  }

  // Merge chunks and convert via existing path
  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>& merged = chunks[0];
  for (size_t c = 1; c < chunks.size(); c++) {
    for (size_t col = 0; col < merged.size(); col++) {
      merged[col]->merge_from(*chunks[c][col]);
    }
  }

  return columns_to_r(merged, schema, total_rows, strings_as_factors,
                      use_altrep);
}
