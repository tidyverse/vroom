#pragma once

#include "fixed_width_index.h"

namespace vroom {

class fixed_width_index_connection : public fixed_width_index {
  std::string filename_;

public:
  fixed_width_index_connection(
      SEXP in,
      std::vector<int> col_starts,
      std::vector<int> col_ends,
      bool trim_ws,
      const size_t skip,
      const char* comment,
      const bool skip_empty_rows,
      const size_t n_max,
      const bool progress,
      const size_t chunk_size);

  ~fixed_width_index_connection() { remove(filename_.c_str()); }
};

} // namespace vroom
