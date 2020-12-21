#include <cpp11/strings.hpp>

#include <random>
#include <string>

[[cpp11::register]] cpp11::strings gen_character_(
    int n,
    int min,
    int max,
    std::string values,
    uint32_t seed,
    uint32_t seed2) {
  std::mt19937 gen1(seed);
  std::mt19937 gen2(seed2);

  cpp11::writable::strings out(n);

  std::uniform_int_distribution<> char_dis(0, values.length() - 1);

  std::uniform_int_distribution<> len_dis(min, max);

  for (int i = 0; i < n; ++i) {
    std::string str;
    auto str_len = len_dis(gen1);
    for (int j = 0; j < str_len; ++j) {
      auto c = char_dis(gen2);
      str.push_back(values[c]);
    }
    out[i] = str.c_str();
  }

  return out;
}
