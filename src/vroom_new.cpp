#include <cpp11.hpp>
#include <libvroom/encoding.h>
#include <libvroom/error.h>
#include <libvroom/format_locale.h>
#include <libvroom/format_parser.h>
#include <libvroom/vroom.h>

#include "arrow_to_r.h"
#include "libvroom_helpers.h"
#include "vroom_arrow_chr.h"

namespace {

// Convert libvroom ParseErrors to an R data frame (tibble-compatible).
// Returns a list with vectors: row (integer), col (integer),
// expected (character), actual (character).
cpp11::writable::list
errors_to_r_problems(const std::vector<libvroom::ParseError>& errors) {
  R_xlen_t n = static_cast<R_xlen_t>(errors.size());
  cpp11::writable::integers rows(n);
  cpp11::writable::integers cols(n);
  cpp11::writable::strings expected(n);
  cpp11::writable::strings actual(n);

  for (R_xlen_t i = 0; i < n; i++) {
    const auto& err = errors[static_cast<size_t>(i)];
    rows[i] = err.line > 0 ? static_cast<int>(err.line) : NA_INTEGER;
    cols[i] = err.column > 0 ? static_cast<int>(err.column) : NA_INTEGER;
    expected[i] = err.message;
    actual[i] = err.context;
  }

  cpp11::writable::list df(
      {cpp11::named_arg("row") = rows, cpp11::named_arg("col") = cols,
       cpp11::named_arg("expected") = expected,
       cpp11::named_arg("actual") = actual});

  df.attr("class") =
      cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
  df.attr("row.names") =
      cpp11::writable::integers({NA_INTEGER, -static_cast<int>(n)});

  return df;
}

} // anonymous namespace

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
    bool use_altrep,
    const std::vector<int>& col_types,
    const cpp11::strings& col_type_names,
    const cpp11::strings& col_formats,
    int default_col_type,
    bool escape_backslash,
    const cpp11::strings& locale_mon_ab,
    const cpp11::strings& locale_mon,
    const cpp11::strings& locale_day_ab,
    const cpp11::strings& locale_am_pm,
    const std::string& locale_date_format,
    const std::string& locale_time_format,
    const std::string& locale_decimal_mark,
    const std::string& locale_tz,
    int guess_max) {

  libvroom::CsvOptions opts;
  opts.decimal_mark = locale_decimal_mark.empty() ? '.' : locale_decimal_mark[0];
  opts.escape_backslash = escape_backslash;
  if (!delim.empty())
    opts.separator = delim;
  opts.quote = quote;
  opts.has_header = has_header;
  opts.skip_empty_rows = skip_empty_rows;
  opts.trim_ws = trim_ws;
  if (skip > 0)
    opts.skip = static_cast<size_t>(skip);
  if (!comment.empty())
    opts.comment = comment;
  opts.null_values = na_values;
  if (num_threads > 0)
    opts.num_threads = static_cast<size_t>(num_threads);

  // Skip full-file encoding detection (simdutf::validate_utf8 scans entire
  // file). R already handles encoding at the connection level.
  opts.encoding = libvroom::CharEncoding::UTF8;

  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  if (guess_max > 0)
    opts.sample_rows = static_cast<size_t>(guess_max);
  else if (guess_max < 0)
    opts.sample_rows = SIZE_MAX;

  libvroom::CsvReader reader(opts);

  open_input_source(reader, input);
  apply_schema_overrides(reader, col_types, col_type_names);

  // Build FormatLocale from R locale parameters
  libvroom::FormatLocale fmt_locale;
  if (locale_mon_ab.size() >= 12) {
    fmt_locale.month_abbr.clear();
    for (R_xlen_t i = 0; i < 12; ++i)
      fmt_locale.month_abbr.push_back(std::string(locale_mon_ab[i]));
  }
  if (locale_mon.size() >= 12) {
    fmt_locale.month_full.clear();
    for (R_xlen_t i = 0; i < 12; ++i)
      fmt_locale.month_full.push_back(std::string(locale_mon[i]));
  }
  if (locale_day_ab.size() >= 7) {
    fmt_locale.day_abbr.clear();
    for (R_xlen_t i = 0; i < 7; ++i)
      fmt_locale.day_abbr.push_back(std::string(locale_day_ab[i]));
  }
  if (locale_am_pm.size() >= 2) {
    fmt_locale.am_pm.clear();
    for (R_xlen_t i = 0; i < 2; ++i)
      fmt_locale.am_pm.push_back(std::string(locale_am_pm[i]));
  }
  if (!locale_date_format.empty())
    fmt_locale.date_format = locale_date_format;
  if (!locale_time_format.empty())
    fmt_locale.time_format = locale_time_format;
  if (!locale_decimal_mark.empty())
    fmt_locale.decimal_mark = locale_decimal_mark[0];
  if (!locale_tz.empty())
    fmt_locale.default_tz = locale_tz;

  // Create FormatParser and attach to reader
  auto format_parser = std::make_unique<libvroom::FormatParser>(fmt_locale);
  reader.set_format_parser(std::move(format_parser));

  // Apply format strings from R col_types to the schema
  if (col_formats.size() > 0) {
    auto schema_copy = reader.schema();
    if (col_type_names.size() > 0) {
      // Named matching
      for (size_t i = 0; i < schema_copy.size(); ++i) {
        for (R_xlen_t j = 0; j < col_type_names.size(); ++j) {
          if (schema_copy[i].name == std::string(col_type_names[j])) {
            if (j < col_formats.size()) {
              schema_copy[i].format = std::string(col_formats[j]);
            }
            break;
          }
        }
      }
    } else {
      // Positional matching
      for (R_xlen_t j = 0; j < col_formats.size() &&
                            static_cast<size_t>(j) < schema_copy.size(); ++j) {
        schema_copy[static_cast<size_t>(j)].format = std::string(col_formats[j]);
      }
    }
    reader.set_schema(schema_copy);
  }

  // Apply default column type to columns not explicitly typed
  if (default_col_type > 0) {
    auto schema_copy = reader.schema();
    for (size_t i = 0; i < schema_copy.size(); ++i) {
      bool has_explicit = false;
      if (!col_types.empty()) {
        if (col_type_names.size() > 0) {
          // Named: check if this column was in the named list
          for (R_xlen_t j = 0; j < col_type_names.size(); ++j) {
            if (schema_copy[i].name == std::string(col_type_names[j])) {
              has_explicit = true;
              break;
            }
          }
        } else {
          // Positional: columns within col_types range are explicit
          has_explicit = (i < col_types.size());
        }
      }
      if (!has_explicit) {
        schema_copy[i].type = static_cast<libvroom::DataType>(default_col_type);
      }
    }
    reader.set_schema(schema_copy);
  }

  const auto& schema = reader.schema();

  // Start streaming: runs SIMD analysis synchronously, dispatches parse tasks
  auto stream_result = reader.start_streaming();
  if (!stream_result) {
    cpp11::stop("Failed to start streaming: %s", stream_result.error.c_str());
  }

  auto attach_problems = [&reader](cpp11::sexp result) -> cpp11::sexp {
    const auto& errors = reader.errors();
    if (!errors.empty()) {
      Rf_setAttrib(result, Rf_install("problems"), errors_to_r_problems(errors));
    }
    return result;
  };

  size_t total_rows = reader.row_count();
  size_t ncols = schema.size();

  if (total_rows == 0) {
    auto result = empty_tibble_from_schema(schema);
    // Drain any remaining chunks
    while (reader.next_chunk()) {}
    return attach_problems(result);
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
      case libvroom::DataType::TIMESTAMP:
      case libvroom::DataType::TIME: {
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

        } else if (type == libvroom::DataType::TIME) {
          auto& col = static_cast<libvroom::ArrowTimeColumnBuilder&>(*columns[i]);
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

    // Set Date/Timestamp/Time class attributes on numeric vectors
    for (size_t i = 0; i < ncols; i++) {
      if (schema[i].type == libvroom::DataType::DATE) {
        Rf_setAttrib(numeric_vecs[i], R_ClassSymbol, Rf_mkString("Date"));
      } else if (schema[i].type == libvroom::DataType::TIMESTAMP) {
        cpp11::writable::strings cls({"POSIXct", "POSIXt"});
        Rf_setAttrib(numeric_vecs[i], R_ClassSymbol, cls);
        Rf_setAttrib(numeric_vecs[i], Rf_install("tzone"), Rf_mkString("UTC"));
      } else if (schema[i].type == libvroom::DataType::TIME) {
        cpp11::writable::strings cls({"hms", "difftime"});
        Rf_setAttrib(numeric_vecs[i], R_ClassSymbol, cls);
        Rf_setAttrib(numeric_vecs[i], Rf_install("units"), Rf_mkString("secs"));
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
    return attach_problems(result);
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
    return attach_problems(result);
  }

  // Merge chunks and convert via existing path
  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>& merged = chunks[0];
  for (size_t c = 1; c < chunks.size(); c++) {
    for (size_t col = 0; col < merged.size(); col++) {
      merged[col]->merge_from(*chunks[c][col]);
    }
  }

  return attach_problems(columns_to_r(merged, schema, total_rows, strings_as_factors,
                                      use_altrep));
}
