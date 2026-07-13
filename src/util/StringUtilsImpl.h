// Copyright 2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Authors: Andre Schlegel (schlegea@informatik.uni-freiburg.de)
//          Johannes Kalmbach, kalmbach@cs.uni-freiburg.de

#ifndef QLEVER_SRC_UTIL_STRINGUTILSIMPL_H
#define QLEVER_SRC_UTIL_STRINGUTILSIMPL_H

#include <ctre-unicode.hpp>

#include "util/Algorithm.h"
#include "util/CtreHelpers.h"
#include "util/Exception.h"
#include "util/StringUtils.h"

namespace ad_utility {

namespace detail {
// CTRE named capture group identifiers for C++17 compatibility
constexpr ctll::fixed_string digitCaptureGroup = "digit";

// Helper for `insertThousandSeparator` below. This has to be a class
// template (rather than a function-local `static constexpr` variable),
// because a function-local variable never has linkage, but a `const auto&`
// non-type template parameter (used for `ctre::search_all` below) requires
// an object with linkage in C++17 (unlike in C++20, which allows
// literal-class-type template arguments directly). A static data member of
// a class template, unlike a function-local variable, does have linkage.
template <const char floatingPointSignifier>
struct DigitSequencePattern {
  // A `ctll::fixed_string` of `floatingPointSignifier`, that can be used
  // inside regex character classes, without being confused with one of the
  // reserved characters.
  static constexpr auto adjustedSignifier = []() {
    constexpr ctll::fixed_string floatingPointSignifierAsFixedString(
        {floatingPointSignifier, '\0'});

    // Inside a regex character class are fewer reserved characters.
    if constexpr (contains(R"--(^-[]\)--", floatingPointSignifier)) {
      return "\\" + floatingPointSignifierAsFixedString;
    } else {
      return floatingPointSignifierAsFixedString;
    }
  }();

  // The pattern finds any digit sequence, that is long enough for inserting
  // thousand separators and is not the fractual part of a floating point.
  static constexpr ctll::fixed_string value{"(?:^|[^\\d" + adjustedSignifier +
                                            "])(?<digit>\\d{4,})"};
};
}  // namespace detail

// _____________________________________________________________________________
template <const char floatingPointSignifier>
std::string insertThousandSeparator(const std::string_view str,
                                    const char separatorSymbol) {
  static const auto isDigit = [](const char c) {
    // `char` is ASCII. So the number symbols are the codes from 48 to 57.
    return '0' <= c && c <= '9';
  };
  AD_CONTRACT_CHECK(!isDigit(separatorSymbol) &&
                    !isDigit(floatingPointSignifier));

  /*
  As string view doesn't support the option to insert new values between old
  values, so we create a new string in the wanted format.
  */
  std::ostringstream ostream;

  /*
  Insert separator into the given string and add it into the `ostream`.
  Ignores content of the given string, just works based on length.
  */
  auto insertSeparator = [&separatorSymbol,
                          &ostream](const std::string_view s) {
    // Nothing to do, if the string is to short.
    AD_CORRECTNESS_CHECK(s.length() > 3);

    /*
    For walking over the string view.
    Our initialization value skips the leading digits, so that only the digits
    remain, where we have to put the separator in front of every three chars.
    */
    size_t currentIdx{s.length() % 3 == 0 ? 3 : s.length() % 3};
    ostream << s.substr(0, currentIdx);
    for (; currentIdx < s.length(); currentIdx += 3) {
      ostream << separatorSymbol << s.substr(currentIdx, 3);
    }
  };

  const char* parsePointer = str.data();
  ql::ranges::for_each(
      ctre::search_all<
          detail::DigitSequencePattern<floatingPointSignifier>::value>(str),
      [&parsePointer, &ostream, &insertSeparator](const auto& match) {
        auto digitSequence =
            match.template get<detail::digitCaptureGroup>().to_view();

        // Insert the transformed digit sequence, and the string between it
        // and the `parsePointer`, into the stream.
        ostream << std::string_view(parsePointer,
                                    digitSequence.data() - parsePointer);
        insertSeparator(digitSequence);
        parsePointer = digitSequence.data() + digitSequence.size();
      });
  ostream << str.substr(parsePointer - str.data());
  return ostream.str();
}

template std::string insertThousandSeparator<'.'>(const std::string_view str,
                                                  const char separatorSymbol);
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_STRINGUTILSIMPL_H
