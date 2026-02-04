#include "arrow_to_r.h"

#include <cpp11.hpp>
#include <libvroom/arrow_buffer.h>
#include <libvroom/arrow_column_builder.h>
#include <libvroom/types.h>

#include <cstring>

using namespace libvroom;

namespace {

SEXP int32_column_to_r(const ArrowInt32ColumnBuilder& col, size_t nrows) {
  cpp11::writable::integers result(nrows);
  const int32_t* src = col.values().data();
  int* dest = INTEGER(result);

  if (!col.null_bitmap().has_nulls()) {
    std::memcpy(dest, src, nrows * sizeof(int32_t));
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? src[i] : NA_INTEGER;
    }
  }
  return result;
}

SEXP int64_column_to_r(const ArrowInt64ColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const int64_t* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<double>(src[i]);
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<double>(src[i]) : NA_REAL;
    }
  }
  return result;
}

SEXP float64_column_to_r(const ArrowFloat64ColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const double* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    std::memcpy(dest, src, nrows * sizeof(double));
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? src[i] : NA_REAL;
    }
  }
  return result;
}

SEXP bool_column_to_r(const ArrowBoolColumnBuilder& col, size_t nrows) {
  cpp11::writable::logicals result(nrows);
  const uint8_t* src = col.values().data();
  int* dest = LOGICAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<int>(src[i]);
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<int>(src[i]) : NA_LOGICAL;
    }
  }
  return result;
}

SEXP string_column_to_r(const ArrowStringColumnBuilder& col, size_t nrows) {
  cpp11::writable::strings result(nrows);
  const StringBuffer& buf = col.values();

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      std::string_view sv = buf.get(i);
      SET_STRING_ELT(result, i,
                     Rf_mkCharLenCE(sv.data(), static_cast<int>(sv.size()), CE_UTF8));
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      if (nulls.is_valid(i)) {
        std::string_view sv = buf.get(i);
        SET_STRING_ELT(result, i,
                       Rf_mkCharLenCE(sv.data(), static_cast<int>(sv.size()), CE_UTF8));
      } else {
        SET_STRING_ELT(result, i, NA_STRING);
      }
    }
  }
  return result;
}

SEXP date_column_to_r(const ArrowDateColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const int32_t* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<double>(src[i]);
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<double>(src[i]) : NA_REAL;
    }
  }

  result.attr("class") = "Date";
  return result;
}

SEXP timestamp_column_to_r(const ArrowTimestampColumnBuilder& col, size_t nrows) {
  cpp11::writable::doubles result(nrows);
  const int64_t* src = col.values().data();
  double* dest = REAL(result);

  if (!col.null_bitmap().has_nulls()) {
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = static_cast<double>(src[i]) / 1e6;
    }
  } else {
    const NullBitmap& nulls = col.null_bitmap();
    for (size_t i = 0; i < nrows; i++) {
      dest[i] = nulls.is_valid(i) ? static_cast<double>(src[i]) / 1e6 : NA_REAL;
    }
  }

  result.attr("class") = cpp11::writable::strings({"POSIXct", "POSIXt"});
  result.attr("tzone") = "UTC";
  return result;
}

} // anonymous namespace

SEXP column_to_r(const ArrowColumnBuilder& column, size_t nrows) {
  switch (column.type()) {
  case DataType::INT32:
    return int32_column_to_r(
        static_cast<const ArrowInt32ColumnBuilder&>(column), nrows);
  case DataType::INT64:
    return int64_column_to_r(
        static_cast<const ArrowInt64ColumnBuilder&>(column), nrows);
  case DataType::FLOAT64:
    return float64_column_to_r(
        static_cast<const ArrowFloat64ColumnBuilder&>(column), nrows);
  case DataType::BOOL:
    return bool_column_to_r(
        static_cast<const ArrowBoolColumnBuilder&>(column), nrows);
  case DataType::STRING:
    return string_column_to_r(
        static_cast<const ArrowStringColumnBuilder&>(column), nrows);
  case DataType::DATE:
    return date_column_to_r(
        static_cast<const ArrowDateColumnBuilder&>(column), nrows);
  case DataType::TIMESTAMP:
    return timestamp_column_to_r(
        static_cast<const ArrowTimestampColumnBuilder&>(column), nrows);
  default:
    // For UNKNOWN/NA types, return character column
    if (auto* str_col =
            dynamic_cast<const ArrowStringColumnBuilder*>(&column)) {
      return string_column_to_r(*str_col, nrows);
    }
    cpp11::stop("Unsupported column type: %d", static_cast<int>(column.type()));
  }
}

cpp11::writable::list columns_to_r(
    const std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns,
    const std::vector<ColumnSchema>& schema,
    size_t nrows) {

  size_t ncols = columns.size();
  cpp11::writable::list result(ncols);
  cpp11::writable::strings names(ncols);

  for (size_t i = 0; i < ncols; i++) {
    result[static_cast<R_xlen_t>(i)] = column_to_r(*columns[i], nrows);
    names[static_cast<R_xlen_t>(i)] = schema[i].name;
  }

  result.attr("names") = names;
  result.attr("class") =
      cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
  result.attr("row.names") =
      cpp11::writable::integers({NA_INTEGER, -static_cast<int>(nrows)});

  return result;
}
