#include "fixed_width_index_connection.h"

#include "connection.h"

#include <fstream>
#include <future> // std::async, std::future

#include "r_utils.h"
#include <Rcpp.h>

#ifdef VROOM_LOG
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "spdlog/spdlog.h"
#endif

#include <array>

using namespace vroom;

fixed_width_index_connection::fixed_width_index_connection(
    SEXP in,
    std::vector<int> col_starts,
    std::vector<int> col_ends,
    bool trim_ws,
    const size_t skip,
    const char comment,
    const size_t n_max,
    const bool progress,
    const size_t chunk_size) {

  col_starts_ = col_starts;
  col_ends_ = col_ends;
  trim_ws_ = trim_ws;

  filename_ = Rcpp::as<std::string>(Rcpp::as<Rcpp::Function>(
      Rcpp::Environment::namespace_env("vroom")["vroom_tempfile"])());

  std::FILE* out = std::fopen(filename_.c_str(), "wb");

  auto con = R_GetConnection(in);

  bool should_open = !is_open(in);
  if (should_open) {
    Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["open"])(in, "rb");
  }

  std::array<std::vector<char>, 2> buf = {std::vector<char>(chunk_size),
                                          std::vector<char>(chunk_size)};

  // A buf index that alternates between 0,1
  auto i = 0;

  newlines_.reserve(128);

  auto readBin =
      Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["readBin"]);

  size_t sz = R_ReadConnection(con, buf[i].data(), chunk_size - 1);
  buf[i][sz] = '\0';

  // Parse header
  size_t start = find_first_line(buf[i], skip, comment);

  std::unique_ptr<RProgress::RProgress> pb = nullptr;
  if (progress) {
    pb = std::unique_ptr<RProgress::RProgress>(
        new RProgress::RProgress(get_pb_format("connection"), 1e12));
    pb->tick(start);
  }

  size_t total_read = 0;
  std::future<void> parse_fut;
  std::future<void> write_fut;
  size_t lines_read = 0;
  std::unique_ptr<RProgress::RProgress> empty_pb = nullptr;

  newlines_.push_back(start - 1);

  while (sz > 0) {
    if (parse_fut.valid()) {
      parse_fut.wait();
    }
    parse_fut = std::async([&, i, start, total_read, sz] {
      lines_read = index_region(
          buf[i], newlines_, start, sz, total_read, n_max, empty_pb);
    });

    if (write_fut.valid()) {
      write_fut.wait();
    }
    write_fut = std::async(
        [&, i, sz] { std::fwrite(buf[i].data(), sizeof(char), sz, out); });

    if (progress) {
      pb->tick(sz);
    }

    total_read += sz;

    i = (i + 1) % 2;
    sz = R_ReadConnection(con, buf[i].data(), chunk_size - 1);
    if (sz > 0) {
      buf[i][sz] = '\0';
    }

    start = 0;

    SPDLOG_DEBUG("first_nl_loc: {0} size: {1}", start, sz);
  }
  if (parse_fut.valid()) {
    parse_fut.wait();
  }
  if (write_fut.valid()) {
    write_fut.wait();
  }
  std::fclose(out);

  if (progress) {
    pb->update(1);
  }

  /* raw connections are always created as open, but we should close them */
  bool should_close = should_open || Rf_inherits(in, "rawConnection");
  if (should_close) {
    Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["close"])(in);
  }

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename_, error);
  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

#ifdef VROOM_LOG
#if SPDLOG_ACTIVE_LEVEL <= SPD_LOG_LEVEL_DEBUG
  auto log = spdlog::basic_logger_mt(
      "basic_logger", "logs/fixed_width_index_connection.idx", true);
  for (auto& v : newlines_) {
    SPDLOG_LOGGER_DEBUG(log, "{}", v);
  }
  SPDLOG_LOGGER_DEBUG(log, "end of idx {0:x}", (size_t)&newlines_);
  spdlog::drop("basic_logger");
#endif
#endif
}
