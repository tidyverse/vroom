#pragma once

#include "arrow_column_builder.h"
#include "error.h"
#include "options.h"
#include "types.h"

#include <deque>
#include <istream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace libvroom {

// Forward declarations
class Table;

// Options for streaming parsing
struct StreamingOptions {
  CsvOptions csv;
  size_t batch_size = 8192; // Rows per batch (0 = all available rows per call)
};

// A batch of parsed rows as columnar builders
struct StreamBatch {
  std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
  size_t num_rows = 0;
  bool is_last = false; // True if this is the final batch from finish()
};

// Streaming CSV parser - accepts chunked input and produces columnar batches.
//
// Pull-model API:
//   feed(data, size)  -- provide input chunks
//   next_batch()      -- get next batch of parsed rows
//   finish()          -- flush remaining partial row
//
// Output: StreamBatch containing vector<unique_ptr<ArrowColumnBuilder>> + row count,
// directly compatible with Table::from_parsed_chunks() and Arrow export.
//
// Schema: Auto-inferred from header + first rows (default), or explicitly
// set via set_schema().
class StreamingParser {
public:
  explicit StreamingParser(const StreamingOptions& options = StreamingOptions{});
  ~StreamingParser();

  StreamingParser(const StreamingParser&) = delete;
  StreamingParser& operator=(const StreamingParser&) = delete;
  StreamingParser(StreamingParser&&) noexcept;
  StreamingParser& operator=(StreamingParser&&) noexcept;

  // Provide input data. May be called multiple times with partial chunks.
  // Returns failure if a fatal parsing error occurs.
  Result<void> feed(const char* data, size_t size);

  // Get the next complete batch, or nullopt if no batch is ready.
  std::optional<StreamBatch> next_batch();

  // Signal end of input. Flushes any remaining buffered data as the final batch.
  // Returns failure if a fatal parsing error occurs.
  Result<void> finish();

  // Explicitly set the schema (column names + types).
  // Must be called before feed() if used. Disables auto-inference.
  void set_schema(const std::vector<ColumnSchema>& schema);

  // Check if schema has been determined (either auto-inferred or set explicitly).
  bool schema_ready() const;

  // Get the current schema (empty if not yet determined).
  const std::vector<ColumnSchema>& schema() const;

  // Check if any errors were collected.
  bool has_errors() const;

  // Get collected errors.
  const std::vector<ParseError>& errors() const;

  // Get the error collector (for advanced usage).
  const ErrorCollector& error_collector() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Convenience function: read from an istream and return a Table.
// Reads in 64KB chunks, feeds to StreamingParser, assembles all batches into a Table.
std::shared_ptr<Table> read_csv_stream(std::istream& input,
                                       const StreamingOptions& options = StreamingOptions{});

} // namespace libvroom
