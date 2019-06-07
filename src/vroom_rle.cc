#include "vroom_rle.h"

#ifdef HAS_ALTREP

R_altrep_class_t vroom_rle::class_t;

void init_vroom_rle(DllInfo* dll) { vroom_rle::Init(dll); }

#else
void init_vroom_rle(DllInfo* dll) {}
#endif
