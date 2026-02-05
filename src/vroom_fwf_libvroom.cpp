#include <cpp11.hpp>
#include <libvroom/encoding.h>
#include <libvroom/vroom.h>

#include "arrow_to_r.h"

#include <cstring>

[[cpp11::register]] cpp11::sexp vroom_libvroom_fwf_(
    SEXP input,
    const std::vector<int>& col_starts,
    const std::vector<int>& col_ends,
    const cpp11::strings& col_names,
    bool trim_ws,
    const std::string& comment,
    bool skip_empty_rows,
    const std::string& na_values,
    int skip,
    int n_max,
    int num_threads) {

  libvroom::FwfOptions opts;
  opts.col_starts = col_starts;
  opts.col_ends = col_ends;
  for (R_xlen_t i = 0; i < col_names.size(); ++i) {
    opts.col_names.push_back(std::string(col_names[i]));
  }
  opts.trim_ws = trim_ws;
  if (!comment.empty())
    opts.comment = comment[0];
  opts.skip_empty_rows = skip_empty_rows;
  if (!na_values.empty())
    opts.null_values = na_values;
  if (skip > 0)
    opts.skip = static_cast<size_t>(skip);
  if (n_max >= 0)
    opts.max_rows = static_cast<int64_t>(n_max);
  if (num_threads > 0)
    opts.num_threads = static_cast<size_t>(num_threads);

  // Skip full-file encoding detection — R handles encoding at the connection level
  opts.encoding = libvroom::CharEncoding::UTF8;

  libvroom::FwfReader reader(opts);

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

  auto stream_result = reader.start_streaming();
  if (!stream_result) {
    cpp11::stop("Failed to start streaming: %s", stream_result.error.c_str());
  }

  size_t ncols = schema.size();

  // Collect all chunks
  std::vector<std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>> chunks;
  while (auto chunk = reader.next_chunk()) {
    chunks.push_back(std::move(chunk.value()));
  }

  // Compute actual total rows from column builder sizes (count_newlines
  // overestimates when there are empty/comment lines in the data)
  size_t actual_rows = 0;
  for (const auto& chunk : chunks) {
    if (!chunk.empty()) {
      actual_rows += chunk[0]->size();
    }
  }

  if (actual_rows == 0) {
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
    return result;
  }

  return columns_to_r_chunked(chunks, schema, actual_rows);
}
