#pragma once

/**
 * @file arrow_ipc_writer.h
 * @brief Arrow IPC file format writer
 *
 * Implements Apache Arrow IPC (Inter-Process Communication) format output.
 * This format is optimized for zero-copy reads and is compatible with
 * PyArrow, Polars, and other Arrow-compatible tools.
 *
 * Note: This is a stub implementation. Full IPC support requires FlatBuffers
 * for metadata serialization.
 */

#include "arrow_column_builder.h"
#include "options.h"
#include "types.h"

#include <functional>

// Forward declaration (defined in vroom.h)
namespace libvroom {
using ProgressCallback = std::function<bool(size_t, size_t)>;
} // namespace libvroom

#include <memory>
#include <string>
#include <vector>

namespace libvroom {

// Arrow IPC writing options
struct ArrowIpcOptions {
  // Whether to use the streaming format (default) or file format
  // Streaming: schema + record batches (no footer)
  // File: magic + schema + record batches + footer + magic
  bool use_file_format = true;

  // Compression for IPC buffers (not yet implemented)
  // Arrow IPC supports LZ4 and ZSTD compression
  Compression compression = Compression::NONE;

  // Alignment for buffers (Arrow uses 8 bytes by default, 64 for SIMD)
  size_t alignment = 64;

  // Maximum batch size (rows per record batch)
  size_t batch_size = 65536;
};

// Result of Arrow IPC writing
struct ArrowIpcResult {
  std::string error;
  size_t rows_written = 0;
  size_t batches_written = 0;
  size_t bytes_written = 0;

  bool ok() const { return error.empty(); }
};

/**
 * @brief Arrow IPC format writer
 *
 * Writes columnar data to Arrow IPC format (Feather v2).
 * The IPC format uses FlatBuffers for metadata and raw memory
 * for data buffers, enabling zero-copy reads.
 *
 * Usage:
 *   ArrowIpcWriter writer(options);
 *   writer.open("output.arrow");
 *   writer.set_schema(schema);
 *   writer.write_batch(columns);
 *   writer.close();
 */
class ArrowIpcWriter {
public:
  explicit ArrowIpcWriter(const ArrowIpcOptions& options = {});
  ~ArrowIpcWriter();

  // Non-copyable, movable
  ArrowIpcWriter(const ArrowIpcWriter&) = delete;
  ArrowIpcWriter& operator=(const ArrowIpcWriter&) = delete;
  ArrowIpcWriter(ArrowIpcWriter&&) noexcept;
  ArrowIpcWriter& operator=(ArrowIpcWriter&&) noexcept;

  /**
   * @brief Open file for writing
   * @param path Output file path
   * @return Result indicating success or error
   */
  Result<bool> open(const std::string& path);

  /**
   * @brief Set the schema for the output
   * @param schema Column definitions
   *
   * Must be called before write_batch().
   */
  void set_schema(const std::vector<ColumnSchema>& schema);

  /**
   * @brief Write a batch of columns
   * @param columns Column data to write
   * @return Result indicating success or error
   */
  Result<bool> write_batch(const std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns);

  /**
   * @brief Write data in chunks from ParsedChunks
   * @param chunks Parsed CSV chunks
   * @return Result indicating success or error
   */
  Result<bool>
  write_chunks(const std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>>& chunks);

  /**
   * @brief Close and finalize the file
   * @return Result with final statistics
   */
  ArrowIpcResult close();

  /**
   * @brief Check if writer is open
   */
  bool is_open() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * @brief Convert CSV to Arrow IPC format
 *
 * High-level function that reads a CSV file and writes Arrow IPC output.
 *
 * @param csv_path Input CSV file path
 * @param arrow_path Output Arrow IPC file path
 * @param csv_options CSV parsing options
 * @param ipc_options Arrow IPC writing options
 * @param progress Optional progress callback
 * @return Result with conversion statistics
 */
ArrowIpcResult convert_csv_to_arrow_ipc(const std::string& csv_path, const std::string& arrow_path,
                                        const CsvOptions& csv_options = {},
                                        const ArrowIpcOptions& ipc_options = {},
                                        ProgressCallback progress = nullptr);

} // namespace libvroom
