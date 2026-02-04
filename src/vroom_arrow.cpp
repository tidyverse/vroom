#include <cpp11.hpp>
#include <libvroom/arrow_c_data.h>
#include <libvroom/table.h>
#include <libvroom/vroom.h>

// Prevent the ArrowArrayStream from leaking if R errors during import.
// The release callback will be called by the arrow package when it's done
// consuming the stream, but we need to ensure cleanup if something goes
// wrong before that point.
struct StreamGuard {
  libvroom::ArrowArrayStream* stream = nullptr;
  ~StreamGuard() {
    if (stream && stream->release) {
      stream->release(stream);
    }
    delete stream;
  }
  void release() { stream = nullptr; } // transfer ownership
};

[[cpp11::register]] cpp11::sexp vroom_arrow_(
    const std::string& path,
    const std::string& delim,
    char quote,
    bool has_header,
    int skip,
    const std::string& comment,
    bool skip_empty_rows,
    const std::string& na_values,
    int num_threads) {

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

  // Create Table from parsed chunks (O(1) - just moves vectors)
  auto table = libvroom::Table::from_parsed_chunks(
      reader.schema(), std::move(parsed.value));

  // Create ArrowArrayStream and export table into it
  auto* stream = new libvroom::ArrowArrayStream();
  StreamGuard guard;
  guard.stream = stream;

  table->export_to_stream(stream);

  // Encode stream pointer as R double (following DuckDB pattern)
  cpp11::sexp stream_ptr_sexp(
      Rf_ScalarReal(static_cast<double>(
          reinterpret_cast<uintptr_t>(stream))));

  // Call arrow::ImportRecordBatchReader(stream_ptr)
  cpp11::function get_namespace = cpp11::package("base")["getNamespace"];
  cpp11::sexp arrow_ns = get_namespace("arrow");
  cpp11::function import_rbr(
      Rf_findFun(Rf_install("ImportRecordBatchReader"), arrow_ns));

  // Transfer stream ownership to the arrow package
  guard.release();

  return import_rbr(stream_ptr_sexp);
}
