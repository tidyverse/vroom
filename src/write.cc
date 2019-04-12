#include "grisu3.h"
#include <Rcpp.h>

size_t get_buffer_size(Rcpp::List input, size_t start, size_t end) {

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
    switch (TYPEOF(input[i])) {
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

std::vector<char> fill_buf(Rcpp::List input, size_t begin, size_t end) {

  auto buf_sz = get_buffer_size(input, begin, end);
  auto buf = std::vector<char>();
  buf.resize(buf_sz);

  size_t pos = 0;
  for (int row = begin; row < end; ++row) {
    for (int col = 0; col < input.length(); ++col) {
      switch (TYPEOF(input[col])) {
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
        int len = dtoa_grisu3(REAL_ELT(input[col], row), buf.data() + pos);
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

// [[Rcpp::export]]
void vroom_write_(
    Rcpp::List input, std::string filename, size_t buf_lines = 10) {

  size_t begin = 0;
  size_t num_rows = Rf_xlength(input[0]);
  std::FILE* out = std::fopen(filename.c_str(), "wb");

  while (begin < num_rows) {
    auto num_lines = std::min(begin + buf_lines, num_rows - begin);
    auto end = begin + num_lines;

    auto buf = fill_buf(input, begin, end);
    write_buf(buf, out);
    begin += num_lines;
  }

  std::fclose(out);
}
