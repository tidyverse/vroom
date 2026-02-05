#include "libvroom/convert.h"

#include "libvroom/table.h"
#include "libvroom/vroom.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

namespace libvroom {

// Main conversion function
ConversionResult convert_csv_to_parquet(const VroomOptions& options, ProgressCallback progress) {
  ConversionResult result;
  using Clock = std::chrono::high_resolution_clock;

  // Only capture detailed timing when verbose mode is enabled
  // This avoids overhead from 12+ Clock::now() calls in the hot path
  Clock::time_point total_start, reader_create_start, reader_create_end;
  Clock::time_point open_start, open_end, read_start, read_end;
  Clock::time_point writer_create_start, writer_create_end;
  Clock::time_point writer_open_start, writer_open_end;
  Clock::time_point set_schema_start, set_schema_end;
  Clock::time_point write_start, write_end, close_start, close_end, total_end;

  if (options.verbose) {
    total_start = Clock::now();
    reader_create_start = Clock::now();
  }

  // Create CSV reader
  CsvReader reader(options.csv);

  if (options.verbose) {
    reader_create_end = Clock::now();
    open_start = Clock::now();
  }

  auto open_result = reader.open(options.input_path);

  if (options.verbose) {
    open_end = Clock::now();
  }

  if (!open_result) {
    result.error = open_result.error;
    return result;
  }

  // Capture stats early (avoids re-reading file in main.cpp)
  result.cols = reader.schema().size();

  if (options.verbose) {
    std::cerr << "Reading " << options.input_path << "\n";
    std::cerr << "  Columns: " << reader.schema().size() << "\n";
    std::cerr << "  Threads: " << options.csv.num_threads << "\n";

    for (const auto& col : reader.schema()) {
      std::cerr << "    " << col.name << ": " << type_name(col.type) << "\n";
    }

    auto open_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(open_end - open_start).count();
    std::cerr << "  Open time: " << open_ms << "ms\n";

    read_start = Clock::now();
  }

  // Read all data
  auto read_result = reader.read_all();

  if (options.verbose) {
    read_end = Clock::now();
  }

  if (!read_result) {
    result.error = read_result.error;
    // Copy any collected errors even on failure
    result.parse_errors = reader.errors();
    return result;
  }

  // Copy collected errors from reader
  result.parse_errors = reader.errors();

  // Capture row count from parsed data
  result.rows = reader.row_count();

  if (options.verbose) {
    auto read_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_start).count();
    std::cerr << "  Rows: " << reader.row_count() << "\n";
    std::cerr << "  Read time: " << read_ms << "ms\n";
  }

  if (progress) {
    // Report progress at 50%
    if (!progress(50, 100)) {
      result.error = "Cancelled by user";
      return result;
    }
  }

  // Write to Parquet
  if (options.verbose) {
    writer_create_start = Clock::now();
  }

  ParquetWriter writer(options.parquet);

  if (options.verbose) {
    writer_create_end = Clock::now();
    writer_open_start = Clock::now();
  }

  auto write_open_result = writer.open(options.output_path);

  if (options.verbose) {
    writer_open_end = Clock::now();
  }

  if (!write_open_result) {
    result.error = write_open_result.error;
    return result;
  }

  if (options.verbose) {
    set_schema_start = Clock::now();
  }

  writer.set_schema(reader.schema());

  if (options.verbose) {
    set_schema_end = Clock::now();
    write_start = Clock::now();
  }

  // Batch chunks into larger row groups (like Polars)
  // Target: 512 * 512 = 262,144 rows per row group
  constexpr size_t TARGET_ROW_GROUP_SIZE = 512 * 512;

  auto& parsed = read_result.value;
  const auto& schema = reader.schema();

  if (parsed.chunks.empty()) {
    // Nothing to write
  } else if (parsed.chunks.size() == 1) {
    // Single chunk - use direct write (no pipeline overhead)
    auto write_result = writer.write(parsed.chunks[0]);
    if (!write_result) {
      result.error = write_result.error;
      return result;
    }
  } else {
    // Multiple chunks - use pipelined writer for better throughput
    auto pipeline_start = writer.start_pipeline();
    if (!pipeline_start) {
      result.error = pipeline_start.error;
      return result;
    }

    // Check if we have string columns (affects batching strategy)
    bool has_strings = false;
    for (const auto& col_schema : schema) {
      if (col_schema.type == DataType::STRING) {
        has_strings = true;
        break;
      }
    }

    if (has_strings) {
      // Write each chunk directly - avoids expensive string merge
      for (auto& chunk_columns : parsed.chunks) {
        if (chunk_columns.empty())
          continue;
        auto write_result = writer.submit_row_group(std::move(chunk_columns));
        if (!write_result) {
          result.error = write_result.error;
          return result;
        }
      }
    } else {
      // Numeric-only: batch chunks to reduce row group overhead
      std::vector<std::pair<size_t, size_t>> batches;
      size_t batch_start = 0;
      size_t batch_rows = 0;

      for (size_t i = 0; i < parsed.chunks.size(); ++i) {
        if (parsed.chunks[i].empty())
          continue;
        size_t chunk_rows = parsed.chunks[i][0]->size();
        batch_rows += chunk_rows;

        if (batch_rows >= TARGET_ROW_GROUP_SIZE) {
          batches.emplace_back(batch_start, i + 1);
          batch_start = i + 1;
          batch_rows = 0;
        }
      }
      if (batch_start < parsed.chunks.size()) {
        batches.emplace_back(batch_start, parsed.chunks.size());
      }

      for (const auto& [start_idx, end_idx] : batches) {
        size_t total_batch_rows = 0;
        for (size_t i = start_idx; i < end_idx; ++i) {
          if (!parsed.chunks[i].empty()) {
            total_batch_rows += parsed.chunks[i][0]->size();
          }
        }

        std::vector<std::unique_ptr<ArrowColumnBuilder>> accum;
        for (const auto& col_schema : schema) {
          auto col = ArrowColumnBuilder::create(col_schema.type);
          col->reserve(total_batch_rows);
          accum.push_back(std::move(col));
        }

        for (size_t i = start_idx; i < end_idx; ++i) {
          auto& chunk_columns = parsed.chunks[i];
          if (chunk_columns.empty())
            continue;
          for (size_t col_idx = 0; col_idx < schema.size() && col_idx < chunk_columns.size();
               ++col_idx) {
            accum[col_idx]->merge_from(*chunk_columns[col_idx]);
          }
        }

        auto write_result = writer.submit_row_group(std::move(accum));
        if (!write_result) {
          result.error = write_result.error;
          return result;
        }
      }
    }

    auto finish_result = writer.finish_pipeline();
    if (!finish_result) {
      result.error = finish_result.error;
      return result;
    }
  }

  if (options.verbose) {
    write_end = Clock::now();
    close_start = Clock::now();
  }

  auto close_result = writer.close();

  if (options.verbose) {
    close_end = Clock::now();
  }

  if (!close_result) {
    result.error = close_result.error;
    return result;
  }

  if (options.verbose) {
    total_end = Clock::now();

    auto write_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start).count();
    std::cerr << "  Write time: " << write_ms << "ms\n";

    // Detailed timing breakdown (in microseconds for precision)
    auto reader_create_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                reader_create_end - reader_create_start)
                                .count();
    auto open_us =
        std::chrono::duration_cast<std::chrono::microseconds>(open_end - open_start).count();
    auto read_us =
        std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start).count();
    auto writer_create_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                writer_create_end - writer_create_start)
                                .count();
    auto writer_open_us =
        std::chrono::duration_cast<std::chrono::microseconds>(writer_open_end - writer_open_start)
            .count();
    auto set_schema_us =
        std::chrono::duration_cast<std::chrono::microseconds>(set_schema_end - set_schema_start)
            .count();
    auto write_us =
        std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start).count();
    auto close_us =
        std::chrono::duration_cast<std::chrono::microseconds>(close_end - close_start).count();
    auto total_us =
        std::chrono::duration_cast<std::chrono::microseconds>(total_end - total_start).count();

    auto measured_sum = reader_create_us + open_us + read_us + writer_create_us + writer_open_us +
                        set_schema_us + write_us + close_us;
    auto gap_us = total_us - measured_sum;

    std::cerr << "\n  Detailed timing breakdown:\n";
    std::cerr << "    Reader create:  " << (reader_create_us / 1000.0) << "ms\n";
    std::cerr << "    CSV open:       " << (open_us / 1000.0) << "ms\n";
    std::cerr << "    CSV read:       " << (read_us / 1000.0) << "ms\n";
    std::cerr << "    Writer create:  " << (writer_create_us / 1000.0) << "ms\n";
    std::cerr << "    Writer open:    " << (writer_open_us / 1000.0) << "ms\n";
    std::cerr << "    Set schema:     " << (set_schema_us / 1000.0) << "ms\n";
    std::cerr << "    Parquet write:  " << (write_us / 1000.0) << "ms\n";
    std::cerr << "    Writer close:   " << (close_us / 1000.0) << "ms\n";
    std::cerr << "    -------------------------\n";
    std::cerr << "    Measured sum:   " << (measured_sum / 1000.0) << "ms\n";
    std::cerr << "    Total time:     " << (total_us / 1000.0) << "ms\n";
    std::cerr << "    Unaccounted:    " << (gap_us / 1000.0) << "ms ("
              << (100.0 * gap_us / total_us) << "%)\n";
  }

  if (progress) {
    progress(100, 100);
  }

  return result; // Success (error is empty)
}

// =============================================================================
// read_csv_to_table - convenience function
// =============================================================================

std::shared_ptr<Table> read_csv_to_table(const std::string& path, const CsvOptions& opts) {
  CsvReader reader(opts);

  auto open_result = reader.open(path);
  if (!open_result.ok) {
    throw std::runtime_error(open_result.error);
  }

  auto read_result = reader.read_all();
  if (!read_result.ok) {
    throw std::runtime_error(read_result.error);
  }

  const auto& schema = reader.schema();
  return Table::from_parsed_chunks(schema, std::move(read_result.value));
}

} // namespace libvroom
