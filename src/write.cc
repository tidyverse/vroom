#include "grisu3.h"
#include <Rcpp.h>
#include <array>
#include <future>

size_t get_buffer_size(
    const Rcpp::List& input,
    const std::vector<SEXPTYPE>& types,
    size_t start,
    size_t end) {

  // First need to determine how big the buffer(s) should be
  // - For characters we need the total nchar() + 2 (for quotes if needed)
  // - For factors we need max(nchar(levels))
  // - For decimal numbers we need 24 + 1 for null terminator
  //   source: https://stackoverflow.com/a/52045523/2055486
  // - For logical we need 5 (FALSE)
  // - We always use ISO8601 for dates, times and datetimes
  // - For dates we need 10 (2019-04-12)
  // - For times we need 8 (01:00:00)
  // - For datetimes we need 20 (2019-04-12T20:46:31Z)

  size_t buf_size = 0;

  size_t num_rows = end - start;

  for (int i = 0; i < input.length(); ++i) {
    switch (types[i]) {
    case STRSXP: {
      for (size_t j = start; j < end; ++j) {
        auto sz = R_nchar(STRING_ELT(input[i], j), Bytes, TRUE, FALSE, "error");
        buf_size += sz;
      }
      break;
    }
    case LGLSXP:
      buf_size += 5 * num_rows;
      break;
    case REALSXP:
      buf_size += 25 * num_rows;
      break;
    }
  }

  // Add size of delimiters + newline
  buf_size += input.length() * num_rows;

  return buf_size;
}

std::vector<char> fill_buf(
    const Rcpp::List& input,
    const std::vector<SEXPTYPE>& types,
    const std::vector<double*>& real_p,
    size_t begin,
    size_t end) {

  auto buf_sz = get_buffer_size(input, types, begin, end);
  // std::cerr << buf_sz << '\n';
  auto buf = std::vector<char>();
  buf.resize(buf_sz);

  size_t pos = 0;
  for (int row = begin; row < end; ++row) {
    for (int col = 0; col < input.length(); ++col) {
      switch (types[col]) {
      case STRSXP: {
        auto str = CHAR(STRING_ELT(input[col], row));
        while (*str != '\0') {
          buf[pos++] = *str++;
        }
        break;
      }
      case LGLSXP:
        break;
      case REALSXP:
        int len = dtoa_grisu3(real_p[col][row], buf.data() + pos);
        pos += len;
        break;
      }
      buf[pos++] = '\t';
    }
    buf[pos - 1] = '\n';
  }

  buf.resize(pos);

  return buf;
}

void write_buf(const std::vector<char>& buf, std::FILE* out) {
  std::fwrite(buf.data(), sizeof buf[0], buf.size(), out);
}

std::vector<SEXPTYPE> get_types(const Rcpp::List& input) {
  std::vector<SEXPTYPE> out;
  for (int col = 0; col < input.length(); ++col) {
    out.push_back(TYPEOF(input[col]));
  }
  return out;
}

std::vector<double*> get_real_p(const Rcpp::List& input) {
  std::vector<double*> out;
  for (int col = 0; col < input.length(); ++col) {
    if (TYPEOF(input[col]) == REALSXP) {
      out.push_back(REAL(input[col]));
    } else {
      out.push_back(nullptr);
    }
  }
  return out;
}

// [[Rcpp::export]]
void vroom_write_(
    Rcpp::List input,
    std::string filename,
    size_t buf_lines,
    size_t num_threads) {

  size_t begin = 0;
  size_t num_rows = Rf_xlength(input[0]);
  std::FILE* out = std::fopen(filename.c_str(), "wb");

  std::array<std::vector<std::future<std::vector<char> > >, 2> futures;
  futures[0].resize(num_threads);
  futures[1].resize(num_threads);

  std::future<void> write_fut;

  int idx = 0;

  auto types = get_types(input);
  auto real_p = get_real_p(input);

  while (begin < num_rows) {
    auto t = 0;
    while (t < num_threads && begin < num_rows) {
      auto num_lines = std::min(buf_lines, num_rows - begin);
      auto end = begin + num_lines;
      futures[idx][t++] =
          std::async(fill_buf, input, types, real_p, begin, end);
      begin += num_lines;
    }

    if (write_fut.valid()) {
      write_fut.wait();
    }

    write_fut = std::async([&, idx, t] {
      for (auto i = 0; i < t; ++i) {
        auto buf = futures[idx][i].get();
        write_buf(buf, out);
      }
    });

    idx = (idx + 1) % 2;
  }

  // Wait for the last writing to finish
  if (write_fut.valid()) {
    write_fut.wait();
  }

  // Close the file
  std::fclose(out);
}
