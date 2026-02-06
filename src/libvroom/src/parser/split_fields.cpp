// SIMD-accelerated field splitting for CSV parsing using Google Highway.
//
// This file uses Highway's dynamic dispatch to select the optimal
// implementation at runtime based on CPU capabilities.
// Uses CLMUL-based quote parity tracking for 64-byte blocks.

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "parser/split_fields-inl.h"
#include "hwy/foreach_target.h"
#include "parser/split_fields-inl.h"

// Generate dispatch tables and public API (only once)
#if HWY_ONCE

#include "libvroom/vroom.h"

namespace libvroom {

// Export SIMD implementations for dynamic dispatch
HWY_EXPORT(SplitFieldsSimdImpl);
HWY_EXPORT(SplitFieldsSimdIntoImpl);

// Scalar field splitter implementation (original code, renamed)
std::vector<FieldView> split_fields_scalar(const char* data, size_t size, char separator,
                                           char quote) {
  std::vector<FieldView> fields;

  if (size == 0) {
    return fields;
  }

  size_t field_start = 0;
  bool in_quote = false;

  for (size_t i = 0; i < size; ++i) {
    char c = data[i];

    if (c == quote) {
      if (in_quote && i + 1 < size && data[i + 1] == quote) {
        // Escaped quote - skip next char
        ++i;
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote) {
      if (c == separator) {
        // End of field
        FieldView field;
        field.data = data + field_start;
        field.size = i - field_start;
        field.quoted = false;

        // Check if field is quoted
        if (field.size >= 2 && field.data[0] == quote && field.data[field.size - 1] == quote) {
          field.quoted = true;
          field.data++;
          field.size -= 2;
        }

        // Trim whitespace
        while (field.size > 0 && (field.data[0] == ' ' || field.data[0] == '\t')) {
          field.data++;
          field.size--;
        }
        while (field.size > 0 &&
               (field.data[field.size - 1] == ' ' || field.data[field.size - 1] == '\t')) {
          field.size--;
        }

        fields.push_back(field);
        field_start = i + 1;
      } else if (c == '\n' || c == '\r') {
        // End of line
        break;
      }
    }
  }

  // Handle last field
  FieldView field;
  field.data = data + field_start;
  field.size = size - field_start;

  // Remove trailing newline
  while (field.size > 0 &&
         (field.data[field.size - 1] == '\n' || field.data[field.size - 1] == '\r')) {
    field.size--;
  }

  // Check if quoted
  if (field.size >= 2 && field.data[0] == quote && field.data[field.size - 1] == quote) {
    field.quoted = true;
    field.data++;
    field.size -= 2;
  } else {
    field.quoted = false;
  }

  // Trim whitespace
  while (field.size > 0 && (field.data[0] == ' ' || field.data[0] == '\t')) {
    field.data++;
    field.size--;
  }
  while (field.size > 0 &&
         (field.data[field.size - 1] == ' ' || field.data[field.size - 1] == '\t')) {
    field.size--;
  }

  fields.push_back(field);

  return fields;
}

// Public API: SIMD field splitter (for lines >= 64 bytes)
std::vector<FieldView> split_fields_simd(const char* data, size_t size, char separator,
                                         char quote) {
  return HWY_DYNAMIC_DISPATCH(SplitFieldsSimdImpl)(data, size, separator, quote);
}

// Public API: Automatic dispatcher
// Uses SIMD for lines >= 64 bytes, scalar for shorter lines
std::vector<FieldView> split_fields(const char* data, size_t size, char separator, char quote) {
  // Use SIMD for lines >= 64 bytes to amortize setup cost
  if (size >= 64) {
    return split_fields_simd(data, size, separator, quote);
  }
  return split_fields_scalar(data, size, separator, quote);
}

// ============================================================================
// Buffer-reusing versions (avoid allocation per call)
// ============================================================================

// Helper for field post-processing (trim quotes and whitespace)
static inline void PostProcessFieldInline(FieldView& field, char quote) {
  // Remove quotes if present
  if (field.size >= 2 && field.data[0] == quote && field.data[field.size - 1] == quote) {
    field.quoted = true;
    field.data++;
    field.size -= 2;
  }

  // Trim leading whitespace
  while (field.size > 0 && (field.data[0] == ' ' || field.data[0] == '\t')) {
    field.data++;
    field.size--;
  }

  // Trim trailing whitespace
  while (field.size > 0 &&
         (field.data[field.size - 1] == ' ' || field.data[field.size - 1] == '\t')) {
    field.size--;
  }
}

// Scalar field splitting with buffer reuse
void split_fields_scalar_into(const char* data, size_t size, char separator, char quote,
                              std::vector<FieldView>& fields) {
  fields.clear();

  if (size == 0) {
    return;
  }

  size_t field_start = 0;
  bool in_quote = false;

  for (size_t i = 0; i < size; ++i) {
    char c = data[i];

    if (c == quote) {
      if (in_quote && i + 1 < size && data[i + 1] == quote) {
        ++i; // Escaped quote
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote) {
      if (c == separator) {
        FieldView field;
        field.data = data + field_start;
        field.size = i - field_start;
        field.quoted = false;
        PostProcessFieldInline(field, quote);
        fields.push_back(field);
        field_start = i + 1;
      } else if (c == '\n' || c == '\r') {
        break;
      }
    }
  }

  // Handle last field
  FieldView field;
  field.data = data + field_start;
  field.size = size - field_start;

  // Remove trailing newline
  while (field.size > 0 &&
         (field.data[field.size - 1] == '\n' || field.data[field.size - 1] == '\r')) {
    field.size--;
  }

  field.quoted = false;
  PostProcessFieldInline(field, quote);
  fields.push_back(field);
}

// SIMD field splitting with buffer reuse
void split_fields_simd_into(const char* data, size_t size, char separator, char quote,
                            std::vector<FieldView>& fields) {
  HWY_DYNAMIC_DISPATCH(SplitFieldsSimdIntoImpl)(data, size, separator, quote, fields);
}

// Automatic dispatcher with buffer reuse
void split_fields_into(const char* data, size_t size, char separator, char quote,
                       std::vector<FieldView>& fields) {
  if (size >= 64) {
    split_fields_simd_into(data, size, separator, quote, fields);
  } else {
    split_fields_scalar_into(data, size, separator, quote, fields);
  }
}

} // namespace libvroom

#endif // HWY_ONCE
