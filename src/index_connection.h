#include "index.h"

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wkeyword-macro"
#define class class_name
#define private private_ptr
#include <R_ext/Connections.h>
#undef class
#undef private
# pragma clang diagnostic pop
// clang-format on

#if R_CONNECTIONS_VERSION != 1
#error "Missing or unsupported connection API in R"
#endif

#if R_VERSION < R_Version(3, 3, 0)
/* R before 3.3.0 didn't have R_GetConnection() */
extern "C" {
extern Rconnection getConnection(int n);
static Rconnection R_GetConnection(SEXP sConn) {
  return getConnection(Rf_asInteger(sConn));
}
}
#endif

namespace vroom {

class index_connection : public index {

public:
  index_connection(
      SEXP in,
      const char* delim,
      const char quote,
      const bool trim_ws,
      const bool escape_double,
      const bool escape_backslash,
      const bool has_header,
      const size_t skip,
      const char comment,
      const size_t chunk_size,
      const bool progress);

  ~index_connection() { unlink(filename_.c_str()); }
};

} // namespace vroom
