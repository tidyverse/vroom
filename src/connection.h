#pragma once

#include <cpp11/as.hpp>
#include <cpp11/function.hpp>
#include <cpp11/list.hpp>
#include <cpp11/raws.hpp>

// We need to undefine these here as they may be previously defined in windows headers
#undef TRUE
#undef FALSE
#include <cpp11/logicals.hpp>

inline SEXP R_GetConnection(SEXP con) { return con; }

inline size_t R_ReadConnection(SEXP con, void* buf, size_t n) {
  static auto readBin = cpp11::package("base")["readBin"];

  cpp11::raws res(
      readBin(con, cpp11::writable::raws(static_cast<R_xlen_t>(0)), n));
  memcpy(buf, RAW(res), res.size());

  return res.size();
}

inline size_t R_WriteConnection(SEXP con, void* buf, size_t n) {
  static auto writeBin = cpp11::package("base")["writeBin"];

  cpp11::writable::raws payload(n);
  memcpy(RAW(payload), buf, n);

  writeBin(payload, con);

  return n;
}

inline std::string con_description(SEXP con) {
  static auto summary_connection = cpp11::package("base")["summary.connection"];
  return cpp11::as_cpp<std::string>(cpp11::list(summary_connection(con))[0]);
}

inline bool is_open(SEXP con) {
  static auto isOpen = cpp11::package("base")["isOpen"];

  cpp11::logicals res(isOpen(con));

  return res[0];
}
