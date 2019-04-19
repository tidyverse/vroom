#include "src/delimited_index.h"
#include <iostream>

int main(int argc, char** argv) {
  auto idx = vroom::delimited_index(argv[1], "\t", '"', false, false, false, false, 0, -1, '\0', 8, false);

  std::cout << idx.num_rows() * idx.num_columns() << '\n';
}
