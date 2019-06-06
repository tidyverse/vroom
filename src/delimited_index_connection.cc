#include "delimited_index_connection.h"

#include "connection.h"

#include <fstream>
#include <future> // std::async, std::future

#include "r_utils.h"
#include <Rcpp.h>

#ifdef VROOM_LOG
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "spdlog/spdlog.h"
#endif

using namespace vroom;

delimited_index_connection::delimited_index_connection(
    SEXP in,
    const char* delim,
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    size_t n_max,
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

  filename_ = Rcpp::as<std::string>(Rcpp::as<Rcpp::Function>(
      Rcpp::Environment::namespace_env("vroom")["vroom_tempfile"])());

  std::FILE* out = std::fopen(filename_.c_str(), "wb");

  auto con = R_GetConnection(in);

  bool should_open = !is_open(in);
  if (should_open) {
    Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["open"])(in, "rb");
  }

  /* raw connections are always created as open, but we should close them */
  bool should_close = should_open || Rf_inherits(in, "rawConnection");

  std::array<std::vector<char>, 2> buf = {std::vector<char>(chunk_size),
                                          std::vector<char>(chunk_size)};
  // std::vector<char>(chunk_size)};

  // A buf index that alternates between 0,1
  auto i = 0;

  idx_ = std::vector<idx_t>(2);

  idx_[0].reserve(128);

  size_t sz = R_ReadConnection(con, buf[i].data(), chunk_size - 1);
  buf[i][sz] = '\0';

  if (sz == 0) {
    if (should_close) {
      Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["close"])(in);
    }
    return;
  }

  // Parse header
  size_t start = find_first_line(buf[i], skip_, comment_);

  std::string delim_;
  if (delim == nullptr) {
    delim_ = std::string(1, guess_delim(buf[i], start, 5, sz));
  } else {
    delim_ = delim;
  }

  delim_len_ = delim_.length();

  size_t first_nl = find_next_newline(buf[i], start);

  if (sz > 1 && buf[i][first_nl] != '\n') {
    // This first newline must not have fit in the buffer, throw error
    // suggesting a larger buffer size.

    // Try reading again, if size is 0 we are at the end of the file, so should
    // just go on.
    size_t next_sz = R_ReadConnection(con, buf[i].data(), chunk_size - 1);
    if (!(next_sz == 0)) {
      if (should_close) {
        Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["close"])(in);
      }
      std::stringstream ss;

      ss << "The size of the connection buffer (" << chunk_size
         << ") was not large enough\nto fit a complete line:\n  * Increase it "
            "by "
            "setting `Sys.setenv(\"VROOM_CONNECTION_SIZE\")`";

      throw Rcpp::exception(ss.str().c_str(), false);
    }
  }

  // Check for windows newlines
  windows_newlines_ = first_nl > 0 && buf[i][first_nl - 1] == '\r';

  std::unique_ptr<RProgress::RProgress> pb = nullptr;
  if (progress_) {
    pb = std::unique_ptr<RProgress::RProgress>(
        new RProgress::RProgress(get_pb_format("connection"), 1e12));
    pb->tick(start);
  }

  n_max = n_max != static_cast<size_t>(-1) ? n_max + has_header_ : n_max;

  std::unique_ptr<multi_progress> empty_pb = nullptr;

  // Index the first row
  idx_[0].push_back(start - 1);

  size_t cols = 0;
  bool in_quote = false;
  size_t lines_read = index_region(
      buf[i],
      idx_[0],
      delim_.c_str(),
      quote,
      in_quote,
      start,
      first_nl + 1,
      0,
      n_max,
      cols,
      0,
      empty_pb);

  columns_ = idx_[0].size() - 1;

  SPDLOG_DEBUG(
      "first_line_columns: {0} first_nl_loc: {1} size: {2}",
      columns_,
      first_nl,
      sz);

  size_t total_read = 0;
  std::future<void> parse_fut;
  std::future<void> write_fut;
  // We don't actually want any progress bar, so just pass a dummy one.

  while (sz > 0) {
    if (parse_fut.valid()) {
      parse_fut.wait();
    }
    parse_fut = std::async([&, i, sz, first_nl, total_read] {
      n_max -= lines_read;

      lines_read = index_region(
          buf[i],
          idx_[1],
          delim_.c_str(),
          quote,
          in_quote,
          first_nl,
          sz,
          total_read,
          n_max,
          cols,
          columns_,
          empty_pb);
    });

    if (write_fut.valid()) {
      write_fut.wait();
    }
    write_fut = std::async(
        [&, i, sz] { std::fwrite(buf[i].data(), sizeof(char), sz, out); });

    if (progress_) {
      pb->tick(sz);
    }

    total_read += sz;

    i = (i + 1) % 2;
    sz = R_ReadConnection(con, buf[i].data(), chunk_size - 1);
    if (sz > 0) {
      buf[i][sz] = '\0';
    }

    first_nl = 0;

    // SPDLOG_DEBUG("first_nl_loc: {0} size: {1}", first_nl, sz);
  }
  if (parse_fut.valid()) {
    parse_fut.wait();
  }
  if (write_fut.valid()) {
    write_fut.wait();
  }
  std::fclose(out);

  if (progress_) {
    pb->update(1);
  }

  /* raw connections are always created as open, but we should close them */
  if (should_close) {
    Rcpp::as<Rcpp::Function>(Rcpp::Environment::base_env()["close"])(in);
  }

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename_, error);
  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  size_t file_size = mmap_.size();

  if (mmap_[file_size - 1] != '\n') {
    if (columns_ == 0) {
      ++columns_;
      idx_[0].push_back(file_size);
    } else {
      idx_[1].push_back(file_size);
    }
  }

  size_t total_size = std::accumulate(
      idx_.begin(), idx_.end(), 0, [](size_t sum, const idx_t& v) {
        sum += v.size() > 0 ? v.size() - 1 : 0;
        return sum;
      });

  rows_ = columns_ > 0 ? total_size / columns_ : 0;

  if (rows_ > 0 && has_header_) {
    --rows_;
  }

#ifdef VROOM_LOG
#if SPDLOG_ACTIVE_LEVEL <= SPD_LOG_LEVEL_DEBUG
  auto log = spdlog::basic_logger_mt(
      "basic_logger", "logs/index_connection.idx", true);
  for (auto& i : idx_) {
    for (auto& v : i) {
      SPDLOG_LOGGER_DEBUG(log, "{}", v);
    }
    SPDLOG_LOGGER_DEBUG(log, "end of idx {0:x}", (size_t)&i);
  }
  spdlog::drop("basic_logger");
#endif
#endif

  SPDLOG_DEBUG("columns: {0} rows: {1}", columns_, rows_);
}
