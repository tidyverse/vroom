/**
 * @file test_util.h
 * @brief Shared test utilities for libvroom test files.
 *
 * Provides:
 * - TempCsvFile: RAII helper for creating temporary CSV files from string content
 * - TempOutputFile: RAII helper for temporary output files (e.g., Parquet)
 * - getValue(): Extract a value as string from any ArrowColumnBuilder type
 * - getStringValue(): Extract a value across chunked ParsedChunks by (col, row)
 */

#pragma once

#include "libvroom.h"
#include "libvroom/arrow_column_builder.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <unistd.h>

namespace test_util {

/// Thread-safe counter for unique temp file naming across all test files.
inline std::atomic<uint64_t>& temp_counter() {
  static std::atomic<uint64_t> counter{0};
  return counter;
}

/**
 * RAII helper that writes string content to a temporary CSV file.
 * The file is automatically deleted on destruction.
 * Each instance gets a unique filename using PID + atomic counter.
 */
class TempCsvFile {
public:
  explicit TempCsvFile(const std::string& content, const std::string& extension = ".csv") {
    uint64_t id = temp_counter().fetch_add(1);
    path_ = "/tmp/libvroom_test_" + std::to_string(getpid()) + "_" + std::to_string(id) + extension;
    std::ofstream f(path_, std::ios::binary);
    f.write(content.data(), static_cast<std::streamsize>(content.size()));
    f.close();
  }
  ~TempCsvFile() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }

  TempCsvFile(const TempCsvFile&) = delete;
  TempCsvFile& operator=(const TempCsvFile&) = delete;

private:
  std::string path_;
};

/**
 * RAII helper for temporary output files (e.g., Parquet).
 * Creates a unique path but does not write any content; the test writes to it.
 * The file is automatically deleted on destruction.
 */
class TempOutputFile {
public:
  explicit TempOutputFile(const std::string& extension = ".parquet") {
    uint64_t id = temp_counter().fetch_add(1);
    path_ = "/tmp/libvroom_test_" + std::to_string(getpid()) + "_" + std::to_string(id) + extension;
  }
  ~TempOutputFile() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }

  TempOutputFile(const TempOutputFile&) = delete;
  TempOutputFile& operator=(const TempOutputFile&) = delete;

private:
  std::string path_;
};

/**
 * Get a value as string from any ArrowColumnBuilder type.
 * Handles STRING, INT32, INT64, FLOAT64, BOOL. Other types trigger ADD_FAILURE.
 */
inline std::string getValue(const libvroom::ArrowColumnBuilder* builder, size_t idx) {
  switch (builder->type()) {
  case libvroom::DataType::STRING: {
    auto* col = dynamic_cast<const libvroom::ArrowStringColumnBuilder*>(builder);
    return std::string(col->values().get(idx));
  }
  case libvroom::DataType::INT32: {
    auto* col = dynamic_cast<const libvroom::ArrowInt32ColumnBuilder*>(builder);
    return std::to_string(col->values().get(idx));
  }
  case libvroom::DataType::INT64: {
    auto* col = dynamic_cast<const libvroom::ArrowInt64ColumnBuilder*>(builder);
    return std::to_string(col->values().get(idx));
  }
  case libvroom::DataType::FLOAT64: {
    auto* col = dynamic_cast<const libvroom::ArrowFloat64ColumnBuilder*>(builder);
    std::ostringstream oss;
    oss << col->values().get(idx);
    return oss.str();
  }
  case libvroom::DataType::BOOL: {
    auto* col = dynamic_cast<const libvroom::ArrowBoolColumnBuilder*>(builder);
    return col->values().get(idx) ? "true" : "false";
  }
  default:
    ADD_FAILURE() << "Unsupported column type: " << static_cast<int>(builder->type());
    return "";
  }
}

/**
 * Get a string value from parsed chunks by column and row index.
 * Searches across all chunks to find the correct row, handling multi-chunk results.
 */
inline std::string getStringValue(const libvroom::ParsedChunks& chunks, size_t col, size_t row) {
  size_t row_offset = 0;
  for (const auto& chunk : chunks.chunks) {
    size_t chunk_rows = chunk[col]->size();
    if (row < row_offset + chunk_rows) {
      return getValue(chunk[col].get(), row - row_offset);
    }
    row_offset += chunk_rows;
  }
  ADD_FAILURE() << "Row " << row << " not found in any chunk (total rows: " << row_offset << ")";
  return "";
}

} // namespace test_util
