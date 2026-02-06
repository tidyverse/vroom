#pragma once

#include "arrow_c_data.h"
#include "arrow_column_builder.h"
#include "arrow_export.h"
#include "types.h"

#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace libvroom {

// Forward declaration
struct ParsedChunks;

/// Table - holds parsed CSV data as multiple chunks for zero-copy Arrow export.
///
/// Instead of merging all parsed chunks into a single set of column builders
/// (O(n) data copy), the Table stores each chunk separately and the Arrow
/// stream iterates over chunks (O(1) construction).
class Table : public std::enable_shared_from_this<Table> {
public:
  /// Construct a Table from schema and pre-built chunks.
  /// Note: export_to_stream() requires the Table to be managed by shared_ptr
  /// (uses shared_from_this). Prefer from_parsed_chunks() which returns shared_ptr.
  Table(std::vector<ColumnSchema> schema,
        std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>> chunks,
        std::vector<size_t> chunk_row_counts, size_t total_rows)
      : schema_(std::move(schema)), chunks_(std::move(chunks)),
        chunk_row_counts_(std::move(chunk_row_counts)), total_rows_(total_rows) {}

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  Table(Table&&) = delete;
  Table& operator=(Table&&) = delete;

  /// Create a Table from ParsedChunks (O(1) - just moves vectors).
  static std::shared_ptr<Table> from_parsed_chunks(const std::vector<ColumnSchema>& schema,
                                                   ParsedChunks&& parsed);

  size_t num_rows() const { return total_rows_; }
  size_t num_columns() const { return schema_.size(); }
  size_t num_chunks() const { return chunks_.size(); }
  size_t chunk_rows(size_t chunk_idx) const {
    assert(chunk_idx < chunks_.size());
    return chunk_row_counts_[chunk_idx];
  }

  const std::vector<ColumnSchema>& schema() const { return schema_; }

  std::vector<std::string> column_names() const {
    std::vector<std::string> names;
    names.reserve(schema_.size());
    for (const auto& col : schema_) {
      names.push_back(col.name);
    }
    return names;
  }

  const std::vector<std::unique_ptr<ArrowColumnBuilder>>& chunk_columns(size_t chunk_idx) const {
    assert(chunk_idx < chunks_.size());
    return chunks_[chunk_idx];
  }

  /// Export as ArrowArrayStream. Emits one RecordBatch per chunk.
  /// The Table must outlive the stream (ensured via shared_ptr).
  void export_to_stream(ArrowArrayStream* stream);

private:
  std::vector<ColumnSchema> schema_;
  std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>> chunks_;
  std::vector<size_t> chunk_row_counts_;
  size_t total_rows_;
};

} // namespace libvroom
