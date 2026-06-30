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
  Create a `ctll::fixed_string` of `floatingPointSignifier`, that can be used
  inside regex character classes, without being confused with one of the
  reserved characters.
  */
  static constexpr auto adjustFloatingPointSignifierForRegex = []() {
    constexpr ctll::fixed_string floatingPointSignifierAsFixedString(
        {floatingPointSignifier, '\0'});

    // Inside a regex character class are fewer reserved character.
    if constexpr (contains(R"--(^-[]\)--", floatingPointSignifier)) {
      return "\\" + floatingPointSignifierAsFixedString;
    } else {
      return floatingPointSignifierAsFixedString;
    }
  };

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

  /*
  The pattern finds any digit sequence, that is long enough for inserting
  thousand separators and is not the fractual part of a floating point.
  */
  static constexpr ctll::fixed_string regexPatDigitSequence{
      "(?:^|[^\\d" + adjustFloatingPointSignifierForRegex() +
      "])(?<digit>\\d{4,})"};
  // Indices into `str`. Newer libc++ no longer allows constructing a
  // `string_view` from a wrapped iterator and a length, so we work in
  // offsets and use `substr` rather than carrying iterators around.
  std::size_t parseOffset = 0;
  ql::ranges::for_each(
      ctre::search_all<regexPatDigitSequence>(str),
      [&parseOffset, &ostream, &insertSeparator, &str](const auto& match) {
        /*
        The digit sequence, that must be transformed. Note: The string view
        iterators point to entries in the `str` string.
        */
        const std::string_view& digitSequence{
            match.template get<detail::digitCaptureGroup>()};
        const std::size_t digitOffset = static_cast<std::size_t>(
            std::begin(digitSequence) - std::begin(str));

        // Insert the transformed digit sequence, and the string between it
        // and `parseOffset`, into the stream.
        ostream << str.substr(parseOffset, digitOffset - parseOffset);
        insertSeparator(digitSequence);
        parseOffset = digitOffset + digitSequence.size();
      });
  ostream << str.substr(parseOffset);
  return ostream.str();
}

template std::string insertThousandSeparator<'.'>(const std::string_view str,
                                                  const char separatorSymbol);
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_STRINGUTILSIMPL_H
