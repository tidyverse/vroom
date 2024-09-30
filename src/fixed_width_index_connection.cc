#include <cpp11/as.hpp>
#include <cpp11/function.hpp>

#include "connection.h"
#include "fixed_width_index_connection.h"
#include "r_utils.h"
#include "unicode_fopen.h"
#include <array>
#include <fstream>
#include <future> // std::async, std::future
#include <utility>

#ifdef VROOM_LOG
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "spdlog/spdlog.h"
#endif

using namespace vroom;

fixed_width_index_connection::fixed_width_index_connection(
    SEXP in,
    std::vector<int> col_starts,
    std::vector<int> col_ends,
    bool trim_ws,
    const size_t skip,
    const char* comment,
    const bool skip_empty_rows,
    const size_t n_max,
    std::shared_ptr<vroom_errors> errors,
    const bool progress,
    const size_t chunk_size) {

  col_starts_ = std::move(col_starts);
  col_ends_ = std::move(col_ends);
  trim_ws_ = trim_ws;

  filename_ =
      cpp11::as_cpp<std::string>(cpp11::package("vroom")["vroom_tempfile"]());

  std::FILE* out = unicode_fopen(filename_.c_str(), "wb");

  auto con = R_GetConnection(in);

  bool should_open = !is_open(in);
  if (should_open) {
    cpp11::package("base")["open"](in, "rb");
  }

  std::array<std::vector<char>, 2> buf = {
      std::vector<char>(chunk_size), std::vector<char>(chunk_size)};

  // A buf index that alternates between 0,1
  auto i = 0;

  newlines_.reserve(128);

  size_t sz = R_ReadConnection(con, buf[i].data(), chunk_size - 1);
  buf[i][sz] = '\0';

  size_t skip_counter = 0;

  // Parse header
  size_t start = find_first_line(
      buf[i],
      skip,
      comment,
      skip_empty_rows,
      /* embedded_nl */ false,
      /* quote */ '\0',
      skip_counter);
  
  if (skip_counter) {
    errors->add_skips_at_start(skip_counter, filename_);
  }

  // Check for windows newlines
  size_t first_nl;
  newline_type nl_type;
  std::tie(first_nl, nl_type) = find_next_newline(
      buf[i],
      start,
      comment,
      skip_empty_rows,
      false,
      /* quote */ '\0');

  bool n_max_set = n_max != static_cast<size_t>(-1);

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
  size_t lines_remaining = n_max;
  std::unique_ptr<RProgress::RProgress> empty_pb = nullptr;

  if (n_max > 0) {
    newlines_.push_back(start - 1);
  }

  while (sz > 0) {
    if (parse_fut.valid()) {
      parse_fut.wait();
    }
    if (lines_read >= lines_remaining) {
      break;
    }
    lines_remaining -= lines_read;

    parse_fut = std::async([&, i, start, total_read, sz] {
      lines_read = index_region(
          buf[i],
          newlines_,
          start,
          sz,
          total_read,
          comment,
          skip_empty_rows,
          lines_remaining,
          empty_pb);
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
    cpp11::package("base")["close"](in);
  }

  std::error_code error;
  if (n_max != 0) {
    mmap_ = make_mmap_source(filename_.c_str(), error);
    if (error) {
      cpp11::stop("%s", error.message().c_str());
    }
  }

  char last_char = mmap_[mmap_.size() - 1];
  bool ends_with_newline = last_char == '\n' || last_char == '\r';
  if (!n_max_set && !ends_with_newline) {
    newlines_.push_back(mmap_.size());
  }

#ifdef VROOM_LOG
  auto log = spdlog::basic_logger_mt(
      "basic_logger", "logs/fixed_width_index_connection.idx", true);
  log->set_level(spdlog::level::debug);
  for (auto&& v : newlines_) {
    SPDLOG_LOGGER_DEBUG(log, "{}", v);
  }
  SPDLOG_LOGGER_DEBUG(log, "end of idx {0:x}", (size_t)&newlines_);
  spdlog::drop("basic_logger");
#endif
}
