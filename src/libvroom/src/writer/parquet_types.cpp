#include "parquet_types.h"

namespace libvroom {
namespace writer {

void Statistics::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: max (optional binary)
  if (max.has_value()) {
    writer.write_field_begin(1, ThriftCompactWriter::TYPE_BINARY);
    writer.write_binary(max.value());
  }

  // Field 2: min (optional binary)
  if (min.has_value()) {
    writer.write_field_begin(2, ThriftCompactWriter::TYPE_BINARY);
    writer.write_binary(min.value());
  }

  // Field 3: null_count (optional i64)
  if (null_count.has_value()) {
    writer.write_field_begin(3, ThriftCompactWriter::TYPE_I64);
    writer.write_i64(null_count.value());
  }

  // Field 4: distinct_count (optional i64)
  if (distinct_count.has_value()) {
    writer.write_field_begin(4, ThriftCompactWriter::TYPE_I64);
    writer.write_i64(distinct_count.value());
  }

  // Field 5: max_value (optional binary)
  if (max_value.has_value()) {
    writer.write_field_begin(5, ThriftCompactWriter::TYPE_BINARY);
    writer.write_binary(max_value.value());
  }

  // Field 6: min_value (optional binary)
  if (min_value.has_value()) {
    writer.write_field_begin(6, ThriftCompactWriter::TYPE_BINARY);
    writer.write_binary(min_value.value());
  }

  writer.write_struct_end();
}

void SchemaElement::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: type (optional)
  if (type.has_value()) {
    writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
    writer.write_i32(static_cast<int32_t>(type.value()));
  }

  // Field 2: type_length (optional)
  if (type_length.has_value()) {
    writer.write_field_begin(2, ThriftCompactWriter::TYPE_I32);
    writer.write_i32(type_length.value());
  }

  // Field 3: repetition_type (optional)
  if (repetition_type.has_value()) {
    writer.write_field_begin(3, ThriftCompactWriter::TYPE_I32);
    writer.write_i32(static_cast<int32_t>(repetition_type.value()));
  }

  // Field 4: name (required)
  writer.write_field_begin(4, ThriftCompactWriter::TYPE_BINARY);
  writer.write_string(name);

  // Field 5: num_children (optional)
  if (num_children.has_value()) {
    writer.write_field_begin(5, ThriftCompactWriter::TYPE_I32);
    writer.write_i32(num_children.value());
  }

  // Field 6: converted_type (optional)
  if (converted_type.has_value()) {
    writer.write_field_begin(6, ThriftCompactWriter::TYPE_I32);
    writer.write_i32(static_cast<int32_t>(converted_type.value()));
  }

  writer.write_struct_end();
}

void DataPageHeader::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: num_values (required i32)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(num_values);

  // Field 2: encoding (required enum -> i32)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(encoding));

  // Field 3: definition_level_encoding (required)
  writer.write_field_begin(3, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(definition_level_encoding));

  // Field 4: repetition_level_encoding (required)
  writer.write_field_begin(4, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(repetition_level_encoding));

  // Field 5: statistics (optional)
  if (statistics.has_value()) {
    writer.write_field_begin(5, ThriftCompactWriter::TYPE_STRUCT);
    statistics.value().write(writer);
  }

  writer.write_struct_end();
}

void DictionaryPageHeader::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: num_values (required)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(num_values);

  // Field 2: encoding (required)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(encoding));

  // Field 3: is_sorted (optional)
  if (is_sorted.has_value()) {
    writer.write_field_begin(3, is_sorted.value() ? ThriftCompactWriter::TYPE_BOOL_TRUE
                                                  : ThriftCompactWriter::TYPE_BOOL_FALSE);
  }

  writer.write_struct_end();
}

void DataPageHeaderV2::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: num_values
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(num_values);

  // Field 2: num_nulls
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(num_nulls);

  // Field 3: num_rows
  writer.write_field_begin(3, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(num_rows);

  // Field 4: encoding
  writer.write_field_begin(4, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(encoding));

  // Field 5: definition_levels_byte_length
  writer.write_field_begin(5, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(definition_levels_byte_length);

  // Field 6: repetition_levels_byte_length
  writer.write_field_begin(6, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(repetition_levels_byte_length);

  // Field 7: is_compressed (optional, default true)
  if (is_compressed.has_value()) {
    writer.write_field_begin(7, is_compressed.value() ? ThriftCompactWriter::TYPE_BOOL_TRUE
                                                      : ThriftCompactWriter::TYPE_BOOL_FALSE);
  }

  // Field 8: statistics (optional)
  if (statistics.has_value()) {
    writer.write_field_begin(8, ThriftCompactWriter::TYPE_STRUCT);
    statistics.value().write(writer);
  }

  writer.write_struct_end();
}

void PageHeader::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: type (required)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(type));

  // Field 2: uncompressed_page_size (required)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(uncompressed_page_size);

  // Field 3: compressed_page_size (required)
  writer.write_field_begin(3, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(compressed_page_size);

  // Field 4: crc (optional)
  if (crc.has_value()) {
    writer.write_field_begin(4, ThriftCompactWriter::TYPE_I32);
    writer.write_i32(crc.value());
  }

  // Field 5: data_page_header (optional)
  if (data_page_header.has_value()) {
    writer.write_field_begin(5, ThriftCompactWriter::TYPE_STRUCT);
    data_page_header.value().write(writer);
  }

  // Field 7: dictionary_page_header (optional)
  if (dictionary_page_header.has_value()) {
    writer.write_field_begin(7, ThriftCompactWriter::TYPE_STRUCT);
    dictionary_page_header.value().write(writer);
  }

  writer.write_struct_end();
}

void KeyValue::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: key (required)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_BINARY);
  writer.write_string(key);

  // Field 2: value (optional)
  if (value.has_value()) {
    writer.write_field_begin(2, ThriftCompactWriter::TYPE_BINARY);
    writer.write_string(value.value());
  }

  writer.write_struct_end();
}

void ColumnMetaData::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: type (required)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(type));

  // Field 2: encodings (required list<i32>)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_LIST);
  writer.write_list_begin(ThriftCompactWriter::TYPE_I32, static_cast<int32_t>(encodings.size()));
  for (const auto& enc : encodings) {
    writer.write_i32(static_cast<int32_t>(enc));
  }
  writer.write_list_end();

  // Field 3: path_in_schema (required list<string>)
  writer.write_field_begin(3, ThriftCompactWriter::TYPE_LIST);
  writer.write_list_begin(ThriftCompactWriter::TYPE_BINARY,
                          static_cast<int32_t>(path_in_schema.size()));
  for (const auto& path : path_in_schema) {
    writer.write_string(path);
  }
  writer.write_list_end();

  // Field 4: codec (required)
  writer.write_field_begin(4, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(static_cast<int32_t>(codec));

  // Field 5: num_values (required)
  writer.write_field_begin(5, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(num_values);

  // Field 6: total_uncompressed_size (required)
  writer.write_field_begin(6, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(total_uncompressed_size);

  // Field 7: total_compressed_size (required)
  writer.write_field_begin(7, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(total_compressed_size);

  // Field 9: data_page_offset (required)
  writer.write_field_begin(9, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(data_page_offset);

  // Field 11: dictionary_page_offset (optional)
  if (dictionary_page_offset.has_value()) {
    writer.write_field_begin(11, ThriftCompactWriter::TYPE_I64);
    writer.write_i64(dictionary_page_offset.value());
  }

  // Field 12: statistics (optional)
  if (statistics.has_value()) {
    writer.write_field_begin(12, ThriftCompactWriter::TYPE_STRUCT);
    statistics.value().write(writer);
  }

  writer.write_struct_end();
}

void ColumnChunk::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: file_path (optional)
  if (file_path.has_value()) {
    writer.write_field_begin(1, ThriftCompactWriter::TYPE_BINARY);
    writer.write_string(file_path.value());
  }

  // Field 2: file_offset (required, default 0)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(file_offset);

  // Field 3: meta_data (optional but usually present)
  if (meta_data.has_value()) {
    writer.write_field_begin(3, ThriftCompactWriter::TYPE_STRUCT);
    meta_data.value().write(writer);
  }

  writer.write_struct_end();
}

void RowGroup::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: columns (required list)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_LIST);
  writer.write_list_begin(ThriftCompactWriter::TYPE_STRUCT, static_cast<int32_t>(columns.size()));
  for (const auto& col : columns) {
    col.write(writer);
  }
  writer.write_list_end();

  // Field 2: total_byte_size (required)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(total_byte_size);

  // Field 3: num_rows (required)
  writer.write_field_begin(3, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(num_rows);

  // Field 5: file_offset (optional)
  if (file_offset.has_value()) {
    writer.write_field_begin(5, ThriftCompactWriter::TYPE_I64);
    writer.write_i64(file_offset.value());
  }

  // Field 6: total_compressed_size (optional)
  if (total_compressed_size.has_value()) {
    writer.write_field_begin(6, ThriftCompactWriter::TYPE_I64);
    writer.write_i64(total_compressed_size.value());
  }

  writer.write_struct_end();
}

void TypeDefinedOrder::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();
  writer.write_struct_end();
}

void ColumnOrder::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();
  // Field 1: TYPE_ORDER (union variant)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_STRUCT);
  type_order.write(writer);
  writer.write_struct_end();
}

void FileMetaData::write(ThriftCompactWriter& writer) const {
  writer.write_struct_begin();

  // Field 1: version (required)
  writer.write_field_begin(1, ThriftCompactWriter::TYPE_I32);
  writer.write_i32(version);

  // Field 2: schema (required list)
  writer.write_field_begin(2, ThriftCompactWriter::TYPE_LIST);
  writer.write_list_begin(ThriftCompactWriter::TYPE_STRUCT, static_cast<int32_t>(schema.size()));
  for (const auto& elem : schema) {
    elem.write(writer);
  }
  writer.write_list_end();

  // Field 3: num_rows (required)
  writer.write_field_begin(3, ThriftCompactWriter::TYPE_I64);
  writer.write_i64(num_rows);

  // Field 4: row_groups (required list)
  writer.write_field_begin(4, ThriftCompactWriter::TYPE_LIST);
  writer.write_list_begin(ThriftCompactWriter::TYPE_STRUCT,
                          static_cast<int32_t>(row_groups.size()));
  for (const auto& rg : row_groups) {
    rg.write(writer);
  }
  writer.write_list_end();

  // Field 5: key_value_metadata (optional list)
  if (key_value_metadata.has_value()) {
    writer.write_field_begin(5, ThriftCompactWriter::TYPE_LIST);
    writer.write_list_begin(ThriftCompactWriter::TYPE_STRUCT,
                            static_cast<int32_t>(key_value_metadata.value().size()));
    for (const auto& kv : key_value_metadata.value()) {
      kv.write(writer);
    }
    writer.write_list_end();
  }

  // Field 6: created_by (optional)
  if (created_by.has_value()) {
    writer.write_field_begin(6, ThriftCompactWriter::TYPE_BINARY);
    writer.write_string(created_by.value());
  }

  // Field 7: column_orders (optional list)
  if (column_orders.has_value()) {
    writer.write_field_begin(7, ThriftCompactWriter::TYPE_LIST);
    writer.write_list_begin(ThriftCompactWriter::TYPE_STRUCT,
                            static_cast<int32_t>(column_orders.value().size()));
    for (const auto& co : column_orders.value()) {
      co.write(writer);
    }
    writer.write_list_end();
  }

  writer.write_struct_end();
}

} // namespace writer
} // namespace libvroom
