#pragma once

#include "Rcpp.h"

#ifdef VROOM_USE_CONNECTIONS_API

// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define class class_name
#define private private_ptr
#include <R_ext/Connections.h>
#undef class
#undef private
#ifdef __clang__
# pragma clang diagnostic pop
#endif
// clang-format on

#if R_CONNECTIONS_VERSION != 1
#error "Missing or unsupported connection API in R"
#endif

#if R_VERSION < R_Version(3, 3, 0)
/* R before 3.3.0 didn't have R_GetConnection() */
extern "C" {

extern Rconnection getConnection(int n);
static Rconnection R_GetConnection(SEXP sConn) {
  return getConnection(Rf_asInteger(sConn));
}
}

#endif

#else

#pragma once

#include "Rcpp.h"

inline SEXP R_GetConnection(SEXP con) { return con; }

inline size_t R_ReadConnection(SEXP con, void* buf, size_t n) {
  static Rcpp::Function readBin = Rcpp::Environment::base_env()["readBin"];

  Rcpp::RawVector res = readBin(con, Rcpp::RawVector(0), n);
  memcpy(buf, res.begin(), res.size());

  return res.length();
}

inline size_t R_WriteConnection(SEXP con, void* buf, size_t n) {
  static Rcpp::Function writeBin = Rcpp::Environment::base_env()["writeBin"];

  Rcpp::RawVector payload(n);
  memcpy(payload.begin(), buf, n);

  writeBin(payload, con);

  return n;
}

#endif

inline std::string con_description(SEXP con) {
  auto summary_connection = Rcpp::as<Rcpp::Function>(
      Rcpp::Environment::base_env()["summary.connection"]);
  return Rcpp::as<std::string>(
      Rcpp::as<Rcpp::List>(summary_connection(con))[0]);
}

inline bool is_open(SEXP con) {
  auto isOpen =
      Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["isOpen"]);

  Rcpp::LogicalVector res = isOpen(con);

  return res[0];
}
