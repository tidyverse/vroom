#include "delimited_index.h"

namespace vroom {

class delimited_index_connection : public delimited_index {
  std::string filename_;

public:
  delimited_index_connection(
      SEXP in,
      const char* delim,
      const char quote,
      const bool trim_ws,
      const bool escape_double,
      const bool escape_backslash,
      const bool has_header,
      const size_t skip,
      const size_t n_max,
      const char comment,
      const size_t chunk_size,
      const bool progress);

  ~delimited_index_connection() { remove(filename_.c_str()); }
};

} // namespace vroom
