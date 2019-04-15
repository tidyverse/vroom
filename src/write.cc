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
  //   (they are converted to UTF-8 in R)
  // - For factors we need max(nchar(levels)) (but currently we just convert to
  //   character in R)
  // - For decimal numbers we need 24
  //   source: https://stackoverflow.com/a/52045523/2055486
  // - For 32 bit integers we need 11 (10 for digits plus the sign)
  // - For logical we need 5 (FALSE)
  //
  // - Currently we convert dates, times and datetimes to character before
  //   output. If we wanted to do it in C it would be
  //   - For dates we need 10 (2019-04-12)
  //   - For times we need 8 (01:00:00)
  //   - For datetimes we need 20 (2019-04-12T20:46:31Z)

  size_t buf_size = 0;

  size_t num_rows = end - start;

  for (int i = 0; i < input.length(); ++i) {
    switch (types[i]) {
    case STRSXP: {
      for (size_t j = start; j < end; ++j) {
        auto sz = Rf_xlength(STRING_ELT(input[i], j));
        buf_size += sz + 2;
      }
      break;
    }
    case LGLSXP:
      buf_size += 5 * num_rows;
      break;
    case REALSXP:
      buf_size += 24 * num_rows;
      break;
    case INTSXP:
      buf_size += 11 * num_rows;
      break;
    }
  }

  // Add size of delimiters + newline
  buf_size += input.length() * num_rows;

  return buf_size;
}

bool needs_quote(const char* str, const char delim, const char* na_str) {
  if (strncmp(str, na_str, 2) == 0) {
    return true;
  }

  for (const char* cur = str; *cur != '\0'; ++cur) {
    if (*cur == '\n' || *cur == '\r' || *cur == '"' || *cur == delim) {
      return true;
    }
  }

  return false;
}

std::vector<char> fill_buf(
    const Rcpp::List& input,
    const char delim,
    const char* na_str,
    const std::vector<SEXPTYPE>& types,
    const std::vector<void*>& ptrs,
    size_t begin,
    size_t end) {

  auto buf_sz = get_buffer_size(input, types, begin, end);
  auto buf = std::vector<char>();
  buf.resize(buf_sz);

  auto na_len = strlen(na_str);

  size_t pos = 0;
  for (int row = begin; row < end; ++row) {
    for (int col = 0; col < input.length(); ++col) {
      switch (types[col]) {
      case STRSXP: {
        auto str = STRING_ELT(input[col], row);
        if (str == NA_STRING) {
          strcpy(buf.data() + pos, na_str);
          pos += na_len;
        } else {
          auto str_p = CHAR(str);
          bool should_quote = needs_quote(str_p, delim, na_str);
          if (should_quote) {
            buf[pos++] = '"';
          }
          while (*str_p != '\0') {
            buf[pos++] = *str_p++;
          }
          if (should_quote) {
            buf[pos++] = '"';
          }
        }

        break;
      }
      case LGLSXP: {
        int value = static_cast<int*>(ptrs[col])[row];
        switch (value) {
        case TRUE:
          strcpy(buf.data() + pos, "TRUE");
          pos += 4;
          break;
        case FALSE:
          strcpy(buf.data() + pos, "FALSE");
          pos += 5;
          break;
        default:
          strcpy(buf.data() + pos, na_str);
          pos += na_len;
          break;
        }
        break;
      }
      case REALSXP: {
        auto value = static_cast<double*>(ptrs[col])[row];
        if (!R_FINITE(value)) {
          if (ISNA(value)) {
            strcpy(buf.data() + pos, na_str);
            pos += na_len;
          } else if (ISNAN(value)) {
            strcpy(buf.data() + pos, "NaN");
            pos += 3;
          } else if (value > 0) {
            strcpy(buf.data() + pos, "Inf");
            pos += 3;
          } else {
            strcpy(buf.data() + pos, "-Inf");
            pos += 4;
          }
        } else {
          int len = dtoa_grisu3(
              static_cast<double*>(ptrs[col])[row], buf.data() + pos);
          pos += len;
        }
        break;
      }
      case INTSXP: {
        auto value = static_cast<int*>(ptrs[col])[row];
        if (value == NA_INTEGER) {
          strcpy(buf.data() + pos, na_str);
          pos += na_len;
        } else {
          // TODO: use something like https://github.com/jeaiii/itoa for
          // faster integer writing
          auto len = sprintf(buf.data() + pos, "%i", value);
          pos += len;
        }
        break;
      }
      }
      buf[pos++] = delim;
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

std::vector<void*> get_ptrs(const Rcpp::List& input) {
  std::vector<void*> out;
  for (int col = 0; col < input.length(); ++col) {
    switch (TYPEOF(input[col])) {
    case REALSXP:
      out.push_back(REAL(input[col]));
      break;
    case INTSXP:
      out.push_back(INTEGER(input[col]));
      break;
    case LGLSXP:
      out.push_back(LOGICAL(input[col]));
      break;
    default:
      out.push_back(nullptr);
    }
  }
  return out;
}

std::vector<char> get_header(const Rcpp::List& input, const char delim) {
  Rcpp::CharacterVector names =
      Rcpp::as<Rcpp::CharacterVector>(input.attr("names"));
  std::vector<char> out;
  for (size_t i = 0; i < names.size(); ++i) {
    auto str = Rf_translateCharUTF8(STRING_ELT(names, i));
    std::copy(str, str + strlen(str), std::back_inserter(out));
    out.push_back(delim);
  }
  out[out.size() - 1] = '\n';
  return out;
}

// [[Rcpp::export]]
void vroom_write_(
    Rcpp::List input,
    std::string filename,
    const char delim,
    const char* na_str,
    bool col_names,
    bool append,
    size_t num_threads,
    size_t buf_lines) {

  size_t begin = 0;
  size_t num_rows = Rf_xlength(input[0]);

  char mode[3] = "wb";
  if (append) {
    strcpy(mode, "ab");
  }

  std::FILE* out = std::fopen(filename.c_str(), mode);
  if (!out) {
    std::string msg("Cannot open file for writing:\n* ");
    msg += '\'' + filename + '\'';
    throw Rcpp::exception(msg.c_str(), false);
  }

  std::array<std::vector<std::future<std::vector<char> > >, 2> futures;
  futures[0].resize(num_threads);
  futures[1].resize(num_threads);

  std::future<void> write_fut;

  int idx = 0;

  auto types = get_types(input);
  auto ptrs = get_ptrs(input);

  if (col_names) {
    auto header = get_header(input, delim);
    write_buf(header, out);
  }

  while (begin < num_rows) {
    auto t = 0;
    while (t < num_threads && begin < num_rows) {
      auto num_lines = std::min(buf_lines, num_rows - begin);
      auto end = begin + num_lines;
      futures[idx][t++] =
          std::async(fill_buf, input, delim, na_str, types, ptrs, begin, end);
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

// [[Rcpp::export]]
Rcpp::CharacterVector vroom_format_(
    Rcpp::List input, const char delim, const char* na_str, bool col_names) {

  size_t num_rows = Rf_xlength(input[0]);
  Rcpp::CharacterVector out(1);

  auto types = get_types(input);
  auto ptrs = get_ptrs(input);

  std::vector<char> data;
  if (col_names) {
    data = get_header(input, delim);
  }

  auto buf = fill_buf(input, delim, na_str, types, ptrs, 0, num_rows);
  std::copy(buf.begin(), buf.end(), std::back_inserter(data));

  out[0] = Rf_mkCharLenCE(data.data(), data.size(), CE_UTF8);

  return out;
}
