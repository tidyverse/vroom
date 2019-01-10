#include "index_connection.h"

#include <fstream>

#include <Rcpp.h>

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wkeyword-macro"
#define class class_name
#define private private_ptr
#include <R_ext/Connections.h>
#undef class
#undef private
# pragma clang diagnostic pop
// clang-format on

#if R_CONNECTIONS_VERSION != 1
#error "Missing or unsupported connection API in R"
#endif

#if R_VERSION < R_Version(3, 3, 0)
/* R before 3.3.0 didn't have R_GetConnection() */
extern Rconnection getConnection(int n);
static Rconnection R_GetConnection(SEXP sConn) {
  return getConnection(asInteger(sConn));
}
#endif

using namespace vroom;

index_connection::index_connection(
    SEXP in,
    const char delim,
    bool has_header,
    size_t skip,
    size_t chunk_size) {

  has_header_ = has_header;

  auto tempfile =
      Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["tempfile"])();
  filename_ = std::string(CHAR(STRING_ELT(tempfile, 0)));

  std::ofstream out(
      filename_.c_str(),
      std::fstream::out | std::fstream::binary | std::fstream::trunc);

  auto con = R_GetConnection(in);

  std::vector<char> buf(chunk_size);

  idx_.reserve(128);
  idx_.push_back(0);

  auto sz = R_ReadConnection(con, buf.data(), chunk_size);

  while (sz > 0) {
    index_region(buf, idx_, delim, 0, sz);
    out.write(buf.data(), sz);

    sz = R_ReadConnection(con, buf.data(), chunk_size);
  }

  out.close();

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename_, error);
  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  rows_ = idx_.size() / columns_;

  if (has_header_) {
    --rows_;
  }

#if DEBUG
  std::ofstream log(
      "index_connection.idx",
      std::fstream::out | std::fstream::binary | std::fstream::trunc);
  for (auto& v : idx_) {
    log << v << '\n';
  }
  log.close();
  Rcpp::Rcout << columns_ << ':' << rows_ << '\n';
#endif
}
