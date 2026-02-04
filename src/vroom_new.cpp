#include <cpp11.hpp>
#include <libvroom/vroom.h>

#include "arrow_to_r.h"

[[cpp11::register]] cpp11::sexp vroom_libvroom_(
    const std::string& path,
    const std::string& delim,
    char quote,
    bool has_header,
    int skip,
    const std::string& comment,
    bool skip_empty_rows,
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
  if (!comment.empty())
    opts.comment = comment[0];
  if (!na_values.empty())
    opts.null_values = na_values;
  if (num_threads > 0)
    opts.num_threads = static_cast<size_t>(num_threads);

  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(path);
  if (!open_result) {
    cpp11::stop("Failed to open file: %s", open_result.error.c_str());
  }

  auto parsed = reader.read_all();
  if (!parsed) {
    cpp11::stop("Failed to parse CSV: %s", parsed.error.c_str());
  }

  const auto& schema = reader.schema();
  size_t total_rows = parsed.value.total_rows;
  auto& chunks = parsed.value.chunks;

  if (chunks.empty()) {
    // Return empty tibble with correct column names
    size_t ncols = schema.size();
    cpp11::writable::list result(ncols);
    cpp11::writable::strings names(ncols);
    for (size_t i = 0; i < ncols; i++) {
      // Create empty vectors of the appropriate type
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
    return result;
  }

  // ALTREP path: skip chunk merging entirely.
  // String columns wrapped in multi-chunk ALTREP (zero-copy).
  // Numeric columns copied directly from chunks into R vectors.
  if (use_altrep && !strings_as_factors) {
    return columns_to_r_chunked(chunks, schema, total_rows);
  }

  // Non-ALTREP paths: merge chunks first
  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>& merged =
      chunks[0];
  for (size_t c = 1; c < chunks.size(); c++) {
    for (size_t col = 0; col < merged.size(); col++) {
      merged[col]->merge_from(*chunks[c][col]);
    }
  }

  return columns_to_r(merged, schema, total_rows, strings_as_factors,
                      use_altrep);
}
