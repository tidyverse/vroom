#include "vroom_errors.h"
#include <cpp11/external_pointer.hpp>
#include <memory>

[[cpp11::register]] cpp11::data_frame
vroom_errors_(cpp11::external_pointer<std::shared_ptr<vroom_errors>> errors) {
  return (*errors)->error_table();
}
