#include "index.h"

namespace vroom {

class index_connection : public index {

public:
  index_connection(
      SEXP in,
      const char delim,
      bool has_header,
      size_t skip,
      size_t chunk_size);

  ~index_connection() { unlink(filename_.c_str()); }
};

} // namespace vroom
