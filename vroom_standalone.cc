#include "src/delimited_index.h"
#include <iostream>

int main(int argc, char** argv) {
  char* end_p;
  int num_threads = strtol(argv[3], &end_p, 10);

  auto idx = vroom::delimited_index(argv[1], argv[2], '"', false, false, false, false, 0, -1, '\0', num_threads, false);

  std::cout << idx.num_rows() * idx.num_columns() << '\n';
}
