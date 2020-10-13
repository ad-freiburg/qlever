//
// Created by johannes on 13.10.20.
//

#ifndef QLEVER_XSDPARSER_H
#define QLEVER_XSDPARSER_H

#include <ctre/ctre.h>
#include <optional>
#include <charconv>
#include <iostream>

class XsdParser {
  static constexpr auto floatRegexStr =
      ctll::fixed_string {"(?<sign>\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?|(?<inf>(?<infSign>\\+|-)?INF)|(?<nan>NaN)"};

 public:
  static std::optional<float> parseFloat(std::string_view input) {
    if (auto m = ctre::match<floatRegexStr>(input)) {
      std::cout <<  0 << " : "  << m.get<0>() << " " << m.get<0>().to_string() << '\n';
      std::cout <<  1 << " : "  << m.get<1>() << " " << m.get<1>().to_string() << '\n';
      std::cout <<  2 << " : "  << m.get<2>() << " " << m.get<2>().to_string() << '\n';
      std::cout <<  3 << " : "  << m.get<3>() << " " << m.get<3>().to_string() << '\n';
      std::cout <<  4 << " : "  << m.get<4>() << " " << m.get<4>().to_string() << '\n';
      std::cout <<  5 << " : "  << m.get<5>() << " " << m.get<5>().to_string() << '\n';
      std::cout <<  6 << " : "  << m.get<6>() << " " << m.get<6>().to_string() << '\n';
      std::cout <<  7 << " : "  << m.get<7>() << " " << m.get<7>().to_string() << '\n';
      std::cout <<  8 << " : "  << m.get<8>() << " " << m.get<8>().to_string() << '\n';
      return std::stof(m.get<0>().to_string());

    }
    return std::nullopt;
  }
};
#endif  // QLEVER_XSDPARSER_H
