#include "libvroom_helpers.h"

#include <cpp11.hpp>
#include <libvroom/types.h>

cpp11::sexp empty_tibble_from_schema(
    const std::vector<libvroom::ColumnSchema>& schema) {
  size_t ncols = schema.size();
  cpp11::writable::list result(ncols);
  cpp11::writable::strings names(ncols);

  for (size_t i = 0; i < ncols; i++) {
    R_xlen_t ri = static_cast<R_xlen_t>(i);
    switch (schema[i].type) {
    case libvroom::DataType::INT32:
      result[ri] = Rf_allocVector(INTSXP, 0);
      break;
    case libvroom::DataType::INT64:
    case libvroom::DataType::FLOAT64:
      result[ri] = Rf_allocVector(REALSXP, 0);
      break;
    case libvroom::DataType::BOOL:
      result[ri] = Rf_allocVector(LGLSXP, 0);
      break;
    case libvroom::DataType::DATE: {
      SEXP v = Rf_allocVector(REALSXP, 0);
      Rf_setAttrib(v, R_ClassSymbol, Rf_mkString("Date"));
      result[ri] = v;
      break;
    }
    case libvroom::DataType::TIMESTAMP: {
      SEXP v = Rf_allocVector(REALSXP, 0);
      cpp11::writable::strings cls({"POSIXct", "POSIXt"});
      Rf_setAttrib(v, R_ClassSymbol, cls);
      Rf_setAttrib(v, Rf_install("tzone"), Rf_mkString("UTC"));
      result[ri] = v;
      break;
    }
    case libvroom::DataType::TIME: {
      SEXP v = Rf_allocVector(REALSXP, 0);
      cpp11::writable::strings cls({"hms", "difftime"});
      Rf_setAttrib(v, R_ClassSymbol, cls);
      Rf_setAttrib(v, Rf_install("units"), Rf_mkString("secs"));
      result[ri] = v;
      break;
    }
    default:
      result[ri] = Rf_allocVector(STRSXP, 0);
      break;
    }
    names[ri] = schema[i].name;
  }

  result.attr("names") = names;
  result.attr("class") =
      cpp11::writable::strings({"tbl_df", "tbl", "data.frame"});
  result.attr("row.names") =
      cpp11::writable::integers({NA_INTEGER, 0});
  return result;
}
