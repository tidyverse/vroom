#include "idx.h"

namespace vroom {

class index_connection : index {

  index_connection(SEXP con, const char delim, size_t num_threads);

  ~index_connection() { unlink(filename_.c_str()); }
};

} // namespace vroom
