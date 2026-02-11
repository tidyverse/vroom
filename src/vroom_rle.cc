#include "vroom_rle.h"
#include <cpp11.hpp>

R_altrep_class_t vroom_rle::class_t;

void init_vroom_rle(DllInfo* dll) { vroom_rle::Init(dll); }

[[cpp11::register]] SEXP vroom_rle_make(cpp11::integers input) {
  return vroom_rle::Make(input);
}
