#include "index_connection.h"

#include <fstream>

#include "utils.h"
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
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    const char comment,
    const size_t chunk_size,
    const bool progress) {

  has_header_ = has_header;
  quote_ = quote;
  trim_ws_ = trim_ws;
  escape_double_ = escape_double;
  escape_backslash_ = escape_backslash;
  comment_ = comment;
  skip_ = skip;
  progress_ = progress;

  auto tempfile =
      Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["tempfile"])();
  filename_ = std::string(CHAR(STRING_ELT(tempfile, 0)));

  std::ofstream out(
      filename_.c_str(),
      std::fstream::out | std::fstream::binary | std::fstream::trunc);

  auto con = R_GetConnection(in);

  bool should_open = !con->isopen;
  if (should_open) {
    Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["open"])(in, "rb");
  }

  std::vector<char> buf(chunk_size);

  idx_ = std::vector<idx_t>(2);

  idx_[0].reserve(128);

  auto sz = R_ReadConnection(con, buf.data(), chunk_size);

  // Parse header
  auto start = find_first_line(buf);

  auto first_nl = find_next_newline(buf, start);

  // Check for windows newlines
  windows_newlines_ = first_nl > 0 && buf[first_nl - 1] == '\r';

  std::unique_ptr<RProgress::RProgress> pb = nullptr;
  if (progress_) {
    pb = std::unique_ptr<RProgress::RProgress>(
        new RProgress::RProgress(get_pb_format("connection"), 1e12));
    pb->update(0);
  }

  // Index the first row
  idx_[0].push_back(start - 1);
  index_region(buf, idx_[0], delim, quote, start, first_nl + 1, pb);
  columns_ = idx_[0].size() - 1;

#if DEBUG
  Rcpp::Rcerr << "columns: " << columns_ << '\n';
#endif

  while (sz > 0) {
    index_region(buf, idx_[1], delim, quote, first_nl, sz, pb, sz / 10);
    out.write(buf.data(), sz);

    sz = R_ReadConnection(con, buf.data(), chunk_size);
  }

  out.close();

  if (progress_) {
    pb->update(1);
  }

  /* raw connections are always created as open, but we should close them */
  bool should_close =
      should_open || strcmp("rawConnection", con->class_name) == 0;
  if (should_close) {
    Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["close"])(in);
  }

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename_, error);
  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  auto total_size = std::accumulate(
      idx_.begin(), idx_.end(), 0, [](size_t sum, const idx_t& v) {
        sum += v.size() - 1;
#if DEBUG
        Rcpp::Rcerr << v.size() << '\n';
#endif
        return sum;
      });

  rows_ = total_size / columns_;

  if (has_header_) {
    --rows_;
  }

#if DEBUG
  std::ofstream log(
      "index_connection.idx",
      std::fstream::out | std::fstream::binary | std::fstream::trunc);
  for (auto& v : idx_[0]) {
    log << v << '\n';
  }
  log.close();
  Rcpp::Rcerr << columns_ << ':' << rows_ << '\n';
#endif
}
