#pragma once

#include <cpp11.hpp>
#include <libvroom/arrow_column_builder.h>
#include <libvroom/types.h>
#include <libvroom/vroom.h>

#include <cstring>
#include <vector>

// Open a libvroom reader from either a raw vector (RAWSXP) or file path string.
// Template works with CsvReader, FwfReader, etc.
template <typename ReaderT>
void open_input_source(ReaderT& reader, SEXP input) {
  if (TYPEOF(input) == RAWSXP) {
    size_t data_size = Rf_xlength(input);
    auto buffer = libvroom::AlignedBuffer::allocate(data_size);
    std::memcpy(buffer.data(), RAW(input), data_size);
    auto open_result = reader.open_from_buffer(std::move(buffer));
    if (!open_result) {
      cpp11::stop("Failed to open buffer: %s", open_result.error.c_str());
    }
  } else {
    std::string path = cpp11::as_cpp<std::string>(input);
    auto open_result = reader.open(path);
    if (!open_result) {
      cpp11::stop("Failed to open file: %s", open_result.error.c_str());
    }
  }
}

// Apply explicit column type overrides from R col_types to the reader's schema.
// Template works with CsvReader, FwfReader, etc.
template <typename ReaderT>
void apply_schema_overrides(ReaderT& reader,
                            const std::vector<int>& col_types,
                            const cpp11::strings& col_type_names) {
  if (col_types.empty())
    return;

  auto schema_copy = reader.schema();
  if (col_type_names.size() > 0) {
    // Named matching
    for (size_t i = 0; i < schema_copy.size(); ++i) {
      for (R_xlen_t j = 0; j < col_type_names.size(); ++j) {
        if (schema_copy[i].name == std::string(col_type_names[j])) {
          int type_int = col_types[static_cast<size_t>(j)];
          if (type_int > 0) {
            schema_copy[i].type = static_cast<libvroom::DataType>(type_int);
          }
          break;
        }
      }
    }
  } else {
    // Positional matching
    for (size_t i = 0; i < col_types.size() && i < schema_copy.size(); ++i) {
      int type_int = col_types[i];
      if (type_int > 0) {
        schema_copy[i].type = static_cast<libvroom::DataType>(type_int);
      }
      // type_int == 0 means UNKNOWN/guess -> keep inferred type
      // type_int == -1 means skip -> handled in R post-processing
    }
  }
  reader.set_schema(schema_copy);
}

// Create an empty R tibble with correct column types from a schema.
cpp11::sexp empty_tibble_from_schema(
    const std::vector<libvroom::ColumnSchema>& schema);
