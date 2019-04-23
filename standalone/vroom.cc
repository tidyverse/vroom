#include "CLI11.hpp"
#include "delimited_index.h"
#include <iostream>

int main(int argc, char** argv) {

  CLI::App app{"App description"};

  std::string filename;
  app.add_option("filename", filename, "The file to index");
  std::string delim = "\t";
  app.add_option("-d,--delim", delim, "The delimiter to use (default: tab)");
  int num_threads = 8;
  app.add_option("-t,--num_threads", num_threads, "The number of parallel threads to use");
  bool escape_double = false;
  app.add_flag("--escape_double", escape_double, "Quotes escaped by doubling them");
  bool escape_backslash = false;
  app.add_flag("--escape_backslash", escape_backslash, "Quotes escaped by backslash escaping them");
  std::string comment;
  app.add_option("--comment", comment, "Character to use as a comment");
  size_t skip = 0;
  app.add_option("-s,--skip", skip, "Number of lines to skip");
  CLI11_PARSE(app, argc, argv);

  auto idx = vroom::delimited_index(filename.c_str(), delim.c_str(), '"', false, escape_double, escape_backslash, false, skip, -1, comment.c_str()[0], num_threads, false, false);

  std::cout << idx.num_rows() * idx.num_columns() << '\n';

  return 0;
}
