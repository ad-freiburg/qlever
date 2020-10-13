//
// Created by johannes on 13.10.20.
//

#ifndef QLEVER_XSDPARSER_H
#define QLEVER_XSDPARSER_H

#include <ctre/ctre.h>

#include <charconv>
#include <cmath>
#include <iostream>
#include <optional>

class XsdParser {
  static constexpr auto floatRegexStr =
      ctll::fixed_string {"(?<sign>\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee]((\\+|-)?([0-9]+)))?|(?<inf>(?<infSign>\\+|-)?INF)|(?<nan>NaN)"};

  static constexpr size_t sign = 1;
  static constexpr size_t float_val = 2;
  static constexpr size_t exp = 5;
  static constexpr size_t is_inf = 8;
  static constexpr size_t inf_sign = 9;
  static constexpr size_t nan = 10;



 public:
  static std::optional<float> parseFloat(std::string_view input) {

    if (auto m = ctre::match<floatRegexStr>(input)) {

      if (m.get<nan>()) {
        return std::numeric_limits<float>::quiet_NaN();
      }
      if (m.get<is_inf>()) {
        return m.get<inf_sign>().to_string() == "-" ? -std::numeric_limits<float>::infinity() : std::numeric_limits<float>::infinity();
      }

      float val = std::stof(m.get<float_val>().to_string());
      if (m.get<sign>().to_string() == "-") {
        val *= -1;
      }

      if (m.get<exp>()) {
        auto exp_val = std::stof(m.get<exp>().to_string());
        val *= std::pow(10.0f, exp_val);
      }

      return val;



    }
    return std::nullopt;
  }
};
#endif  // QLEVER_XSDPARSER_H
