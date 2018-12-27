#include "read_normal.h"
#include "parallel.h"

Rcpp::LogicalVector read_lgl(
    std::shared_ptr<std::vector<size_t> > offsets,
    mio::shared_mmap_source mmap,
    R_xlen_t column,
    R_xlen_t num_columns,
    R_xlen_t skip,
    R_xlen_t num_threads) {

  R_xlen_t n = offsets->size() / num_columns - skip;

  Rcpp::LogicalVector out(n);

  auto p = out.begin();

  // Need to copy to a temp buffer since we have no way to tell strtod how
  // long the buffer is.
  char buf[128];

  parallel_for(
      n,
      [&](int start, int end, int id) {
        for (int i = start; i < end; ++i) {
          size_t idx = (i + skip) * num_columns + column;
          size_t cur_loc = (*offsets)[idx];
          size_t next_loc = (*offsets)[idx + 1] - 1;
          size_t len = next_loc - cur_loc;

          std::copy(mmap.data() + cur_loc, mmap.data() + next_loc, buf);
          buf[len] = '\0';

          p[i] = Rf_StringTrue(buf);
        }
      },
      num_threads);

  return out;
}
