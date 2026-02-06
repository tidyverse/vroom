#include "libvroom_helpers.h"

#include <cpp11.hpp>
#include <libvroom/types.h>

cpp11::sexp empty_tibble_from_schema(
    const std::vector<libvroom::ColumnSchema>& schema) {
  size_t ncols = schema.size();
  cpp11::writable::list result(ncols);
  cpp11::writable::strings names(ncols);

  for (size_t i = 0; i < ncols; i++) {
    switch (schema[i].type) {
    case libvroom::DataType::INT32:
      result[static_cast<R_xlen_t>(i)] = Rf_allocVector(INTSXP, 0);
      break;
    case libvroom::DataType::FLOAT64:
      result[static_cast<R_xlen_t>(i)] = Rf_allocVector(REALSXP, 0);
      break;
    case libvroom::DataType::BOOL:
      result[static_cast<R_xlen_t>(i)] = Rf_allocVector(LGLSXP, 0);
      break;
    default:
      result[static_cast<R_xlen_t>(i)] = Rf_allocVector(STRSXP, 0);
      break;
    }
    names[static_cast<R_xlen_t>(i)] = schema[i].name;
  }

  result.attr("names") = names;
  result.attr("class") =
      cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
  result.attr("row.names") =
      cpp11::writable::integers({NA_INTEGER, 0});
  return result;
}
