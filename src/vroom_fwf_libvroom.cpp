#include <cpp11.hpp>
#include <libvroom/encoding.h>
#include <libvroom/vroom.h>

#include "arrow_to_r.h"
#include "libvroom_helpers.h"

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
    int num_threads,
    const std::vector<int>& col_types,
    const cpp11::strings& col_type_names) {

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

  open_input_source(reader, input);
  apply_schema_overrides(reader, col_types, col_type_names);

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
    return empty_tibble_from_schema(schema);
  }

  return columns_to_r_chunked(chunks, schema, actual_rows);
}
