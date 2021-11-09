#include "delimited_index.h"

#include "parallel.h"

#include "multi_progress.h"
#include <exception>
#include <fstream>
#include <iostream>
#include <numeric>

#ifdef VROOM_LOG
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "spdlog/spdlog.h"
#endif

#ifndef VROOM_STANDALONE
#include "r_utils.h"
#endif

#include "unicode_fopen.h"

using namespace vroom;

delimited_index::delimited_index(
    const char* filename,
    const char* delim,
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    size_t n_max,
    const char* comment,
    const bool skip_empty_rows,
    std::shared_ptr<vroom_errors> errors,
    size_t num_threads,
    bool progress,
    const bool use_threads)
    : filename_(filename),
      has_header_(has_header),
      quote_(quote),
      trim_ws_(trim_ws),
      escape_double_(escape_double),
      escape_backslash_(escape_backslash),
      skip_(skip),
      comment_(comment),
      rows_(0),
      columns_(0),
      progress_(progress),
      delim_len_(0) {

  std::error_code error;
  mmap_ = make_mmap_source(filename, error);

  if (error) {
    // We cannot actually portably compare error messages due to a bug in
    // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
    // the message on stderr return
#ifndef VROOM_STANDALONE
    REprintf("mapping error: %s\n", error.message().c_str());
#else
    std::cerr << "mapping error: " << error.message() << '\n';
#endif

    return;
  }

  size_t file_size = mmap_.cend() - mmap_.cbegin();

  if (!(mmap_[file_size - 1] == '\n' || mmap_[file_size - 1] == '\r')) {
#ifndef VROOM_STANDALONE
    REprintf("Files must end with a newline\n");
#else
    std::cerr << "Files must end with a newline\n";
#endif
    return;
  }

  bool has_quoted_newlines = quote != '\0';

  size_t start = find_first_line(
      mmap_, skip_, comment_, skip_empty_rows, has_quoted_newlines, quote);

  // If an empty file, or a file with only a newline.
  if (start >= file_size - 1) {
    return;
  }

  if (delim == nullptr) {
#ifndef VROOM_STANDALONE
    delim_ =
        std::string(1, guess_delim(mmap_, start, /* guess_max */ 20, 0, quote));
#else
    throw std::runtime_error("Must specify a delimiter");
#endif
  } else {
    delim_ = delim;
  }

  delim_len_ = delim_.length();

  size_t first_nl, second_nl;
  newline_type nl;
  std::tie(first_nl, nl) = find_next_newline(
      mmap_, start, comment_, skip_empty_rows, has_quoted_newlines, quote);

  std::tie(second_nl, std::ignore) = find_next_newline(
      mmap_,
      first_nl + 1,
      comment,
      skip_empty_rows,
      has_quoted_newlines,
      quote);

  size_t one_row_size = second_nl - first_nl;
  size_t guessed_rows =
      one_row_size > 0 ? (file_size - first_nl) / (one_row_size * 1.1) : 0;

  std::unique_ptr<multi_progress> pb = nullptr;

  if (progress_) {
#ifndef VROOM_STANDALONE
    auto format = get_pb_format("file", filename);
    auto width = get_pb_width(format);
    pb = std::unique_ptr<multi_progress>(
        new multi_progress(format, file_size, width));
    pb->tick(start);
#endif
  }

  bool nmax_set = n_max != static_cast<size_t>(-1);

  if (nmax_set) {
    n_max = n_max + has_header_;
    num_threads = 1;
  }

  // We want at least 10 lines per batch, otherwise threads aren't really
  // useful
  size_t batch_size = file_size / num_threads;
  size_t line_size = second_nl - first_nl;

  if (batch_size < line_size * 10) {
    num_threads = 1;
  }

start_indexing:

  try {

    idx_ = std::vector<idx_t>(num_threads + 1);

    // Index the first row
    size_t cols = 0;
    csv_state state = RECORD_START;
    size_t lines_read = index_region(
        mmap_,
        idx_[0],
        delim_.c_str(),
        nl,
        quote,
        comment_,
        skip_empty_rows,
        state,
        start,
        first_nl + 1,
        0,
        n_max,
        cols,
        0,
        errors,
        pb,
        num_threads,
        -1);
    if (idx_[0].size() > 0) {
      columns_ = idx_[0].size() - 1;
    }

    std::vector<std::future<void>> threads;

    if (nmax_set) {
      threads.emplace_back(std::async(std::launch::async, [&] {
        n_max = n_max > lines_read ? n_max - lines_read : 0;
        index_region(
            mmap_,
            idx_[1],
            delim_.c_str(),
            nl,
            quote,
            comment_,
            skip_empty_rows,
            state,
            first_nl + 1,
            file_size,
            0,
            n_max,
            cols,
            columns_,
            errors,
            pb,
            num_threads,
            file_size / 100);
      }));
    } else {
      threads = parallel_for(
          file_size - first_nl,
          [&](size_t start, size_t end, size_t id) {
            newline_type nl;
            idx_[id + 1].reserve((guessed_rows / num_threads) * columns_);
            std::tie(start, nl) = find_next_newline(
                mmap_,
                first_nl + start,
                comment,
                skip_empty_rows,
                /* has_quote */ false,
                quote);
            ++start;
            std::tie(end, std::ignore) = find_next_newline(
                mmap_,
                first_nl + end,
                comment,
                skip_empty_rows,
                /* has_quote */ false,
                quote);
            ++end;
            size_t cols = 0;
            csv_state state = RECORD_START;
            index_region(
                mmap_,
                idx_[id + 1],
                delim_.c_str(),
                nl,
                quote,
                comment_,
                skip_empty_rows,
                state,
                start,
                end,
                0,
                n_max,
                cols,
                columns_,
                errors,
                pb,
                num_threads,
                file_size / 100);
          },
          num_threads,
          use_threads,
          false);
    }

    if (progress_) {
#ifndef VROOM_STANDALONE
      pb->display_progress();
#endif
    }

    for (auto& t : threads) {
      t.get();
    }

  } catch (newline_error& e) {
    num_threads = 1;
    errors->clear();
    goto start_indexing;
  }
  size_t total_size = std::accumulate(
      idx_.begin(), idx_.end(), std::size_t{0}, [](size_t sum, const idx_t& v) {
        sum += v.size() > 0 ? v.size() : 0;
        return sum;
      });

  rows_ = columns_ > 0 ? total_size / (columns_ + 1) : 0;

  if (rows_ > 0 && has_header_) {
    --rows_;
  }

  // REprintf("columns_: %i rows_: %i\n", columns_, rows_);

#ifdef VROOM_LOG
  auto log = spdlog::basic_logger_mt("basic_logger", "logs/index.idx");
  log->set_level(spdlog::level::debug);
  for (auto& i : idx_) {
    for (auto& v : i) {
      SPDLOG_LOGGER_DEBUG(log, "{}", v);
    }
    SPDLOG_LOGGER_DEBUG(log, "end of idx {0:x}", (size_t)&i);
  }
  spdlog::drop("basic_logger");
#endif

  SPDLOG_DEBUG(
      "columns: {0} rows: {1} total_size: {2}", columns_, rows_, total_size);
}

void delimited_index::trim_quotes(const char*& begin, const char*& end) const {
  if (begin != end && (*begin == quote_)) {
    ++begin;
  }

  if (end != begin && *(end - 1) == quote_) {
    --end;
  }
}

const string delimited_index::get_escaped_string(
    const char* begin, const char* end, bool has_quote) const {

  if (end <= begin) {
    return {begin, begin};
  }

  // If not escaping just return without a copy
  if (!((escape_double_ && has_quote) || escape_backslash_)) {
    return {begin, end};
  }

  std::string out;
  bool needs_escaping = false;
  auto cur = begin;
  auto prev = begin;

  while (cur < end) {
    if ((escape_double_ && has_quote && *cur == quote_) ||
        (escape_backslash_ && *cur == '\\')) {
      if (!needs_escaping) {
        out.reserve(end - begin);
        needs_escaping = true;
      }
      std::copy(prev, cur, std::back_inserter(out));
      ++cur;
      prev = cur;
    }
    ++cur;
  }

  if (needs_escaping) {
    std::copy(prev, cur, std::back_inserter(out));
    return out;
  }

  return {begin, end};
}

inline std::pair<size_t, size_t>
delimited_index::get_cell(size_t i, bool is_first) const {

  auto oi = i;

  auto i_row = i / (columns_);
  auto i_col = i % (columns_);

  auto ni = i_row * (columns_ + 1) + i_col;
  i = ni;

  for (const auto& idx : idx_) {
    auto sz = idx.size();
    if (i + 1 < sz) {

      auto start = idx[i];
      auto end = idx[i + 1];
      if (start == end) {
        return {start, end};
      }
      // By relying on 0 and 1 being true and false we can remove a branch
      // here, which improves performance a bit, as this function is called a
      // lot.
      if (!is_first) {
        start = start + delim_len_;
      }

      // REprintf(
      //"oi: %i ni: %i i: %i start: %i end: %i\n", oi, ni, i, start, end);
      return {start, end};
    }

    i -= sz;
  }

  std::stringstream ss;
  ss.imbue(std::locale(""));
  ss << "Failure to retrieve index " << std::fixed << oi << " / " << rows_;
  throw std::out_of_range(ss.str());
  /* should never get here */
  return {0, 0};
}

const string
delimited_index::get_trimmed_val(size_t i, bool is_first, bool is_last) const {

  size_t begin_p;
  size_t end_p;

  std::tie(begin_p, end_p) = get_cell(i, is_first);
  const char* begin = mmap_.data() + begin_p;
  const char* end = mmap_.data() + end_p;

  // Check for windows newlines if the last column */
  if (is_last) {
    if (begin < end) {
      if (*(end - 1) == '\r') {
        --end;
      }
    }
  }

  if (trim_ws_) {
    trim_whitespace(begin, end);
  }

  bool has_quote = false;
  if (quote_ != '\0') {
    has_quote = *begin == quote_;
    if (has_quote) {
      trim_quotes(begin, end);
    }
    if (trim_ws_) {
      trim_whitespace(begin, end);
    }
  }

  return get_escaped_string(begin, end, has_quote);
}

string delimited_index::get(size_t row, size_t col) const {
  auto i = (row + has_header_) * columns_ + col;

  return get_trimmed_val(i, col == 0, col == (columns_ - 1));
}
