// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_replace.h>

#include <string_view>

#include "util/Algorithm.h"
#include "util/Concepts.h"
#include "util/CtreHelpers.h"

using std::string;
using std::string_view;

namespace ad_utility {
//! Utility functions for string. Can possibly be changed to
//! a templated version using std::basic_string<T> instead of
//! std::string. However, it is not required so far.

//! Returns the longest prefix that the two arguments have in common
string_view commonPrefix(string_view a, string_view b);

//! Case transformations. Should be thread safe.
string getLowercase(const string& orig);

string getUppercase(const string& orig);

// Check if the given string `language tag` is `BPC47` conform.
// Use the ICU library (unicode/uloc.h) for this procedure.
bool strIsLangTag(const string& strLangTag);

/*
 * @brief convert a UTF-8 String to lowercase according to the held locale
 * @param s UTF-8 encoded string
 * @return The lowercase version of s, also encoded as UTF-8
 */
string utf8ToLower(std::string_view s);

// Get the uppercase value. For details see `utf8ToLower` above
string utf8ToUpper(std::string_view s);

/**
 * Get the substring from the UTF8-encoded str that starts at the start-th
 * codepoint as has a length of size codepoints. If start >= the number of
 * codepoints in str, an empty string is returned. If start + size >= the number
 * of codepoints in str then the substring will reach until the end of str. This
 * behavior is consistent with std::string::substr, but working on UTF-8
 * characters that might have multiple bytes.
 */
string_view getUTF8Substring(const std::string_view str, size_t start,
                             size_t size);

// Overload for the above function that creates the substring from the
// `start`-th codepoint to the end of the string.
string_view getUTF8Substring(const std::string_view str, size_t start);

/**
 * @brief get a prefix of a utf-8 string of a specified length
 *
 * Returns first min(prefixLength, numCodepointsInInput) codepoints in the UTF-8
 string sv.

 * CAVEAT: The result is often misleading when looking for an answer to the
 * question "is X a prefix of Y" because collation might ignore aspects like
 * punctuation or case.
 * This is currently only used for the text index where all words that
 * share a common prefix of a certain length are stored in the same block.
 * @param sv a UTF-8 encoded string
 * @param prefixLength The number of Unicode codepoints we want to extract.
 * @return the first max(prefixLength, numCodepointsInArgSP) Unicode
 * codepoints of sv, encoded as UTF-8
 */
std::pair<size_t, std::string_view> getUTF8Prefix(std::string_view s,
                                                  size_t prefixLength);

// Overload for the above function that creates the substring from the
// `start`-th codepoint to the end of the string.
string_view getUTF8Substring(const std::string_view str, size_t start);

//! Gets the last part of a string that is somehow split by the given separator.
string getLastPartOfString(const string& text, const char separator);

/**
 * @brief Return the last position where <literalEnd> was found in the <input>
 * without being escaped by backslashes. If it is not found at all, string::npos
 * is returned.
 */
size_t findLiteralEnd(std::string_view input, std::string_view literalEnd);

/*
@brief Add elements of the range to a stream, with the `separator` between the
elements.

@tparam Range An input range, whos dereferenced iterators can be inserted into
streams.

@param separator Will be put between each of the string representations
of the range elements.
*/
template <std::ranges::input_range Range>
requires ad_utility::Streamable<
    std::iter_reference_t<std::ranges::iterator_t<Range>>>
void lazyStrJoin(std::ostream* stream, Range&& r, std::string_view separator);

// Similar to the overload of `lazyStrJoin` above, but the result is returned as
// a string.
template <std::ranges::input_range Range>
requires ad_utility::Streamable<
    std::iter_reference_t<std::ranges::iterator_t<Range>>>
std::string lazyStrJoin(Range&& r, std::string_view separator);

/*
@brief Adds indentation before the given string and directly after new line
characters.

@param indentationSymbol What the indentation should look like..
*/
std::string addIndentation(std::string_view str,
                           std::string_view indentationSymbol);

/*
@brief Insert the given separator symbol between groups of thousands. For
example: `insertThousandDelimiter("The number 48900.", ',', '.')` returns `"The
number 48,900."`.

@tparam floatingPointSignifier The symbol that, if between two ranges of
numbers, signifies, that the two ranges of numbers build one floating point
number.

@param str The input string.
@param separatorSymbol What symbol to put between groups of thousands.
*/
template <const char floatingPointSignifier = '.'>
std::string insertThousandSeparator(const std::string_view str,
                                    const char separatorSymbol = ' ');

// *****************************************************************************
// Definitions:
// *****************************************************************************

// A "constant-time" comparison for strings.
// Implementation based on https://stackoverflow.com/a/25374036
// Basically for 2 strings of equal length this function will always
// take the same time to compute regardless of how many characters are
// matching. This is to prevent analysing the secret comparison string
// by analysing response times to incrementally figure out individual
// characters. An equally safe, but slower method to achieve the same thing
// would be to compute cryptographically secure hashes (like SHA-3 for example)
// and compare the hashes instead of the actual strings.
constexpr bool constantTimeEquals(std::string_view view1,
                                  std::string_view view2) {
  using byte_view = std::basic_string_view<volatile std::byte>;
  auto impl = [](byte_view str1, byte_view str2) {
    if (str1.length() != str2.length()) {
      return false;
    }
    volatile std::byte mismatchFound{0};
    for (size_t i = 0; i < str1.length(); ++i) {
      // In C++20 compound assignment of volatile variables causes a warning,
      // so we can't use 'mismatchFound |=' until compiling with C++23 where it
      // is fine again. mismatchFound can be interpreted as bool and "is false"
      // until the first mismatch in the strings is found.
      mismatchFound = mismatchFound | (str1[i] ^ str2[i]);
    }
    return !static_cast<bool>(mismatchFound);
  };
  auto toVolatile = [](std::string_view view) constexpr -> byte_view {
    // Casting is safe because both types have the same size
    static_assert(sizeof(std::string_view::value_type) ==
                  sizeof(byte_view::value_type));
    return {
        static_cast<const std::byte*>(static_cast<const void*>(view.data())),
        view.size()};
  };
  return impl(toVolatile(view1), toVolatile(view2));
}

// _________________________________________________________________________
template <std::ranges::input_range Range>
requires ad_utility::Streamable<
    std::iter_reference_t<std::ranges::iterator_t<Range>>>
void lazyStrJoin(std::ostream* stream, Range&& r, std::string_view separator) {
  auto begin = std::begin(r);
  auto end = std::end(r);

  // Is the range empty?
  if (begin == end) {
    return;
  }

  // Add the first entry without a seperator.
  *stream << *begin;

  // Add the remaining entries.
  ++begin;
  std::ranges::for_each(begin, end,
                        [&stream, &separator](const auto& listItem) {
                          *stream << separator << listItem;
                        },
                        {});
}

// _________________________________________________________________________
template <std::ranges::input_range Range>
requires ad_utility::Streamable<
    std::iter_reference_t<std::ranges::iterator_t<Range>>>
std::string lazyStrJoin(Range&& r, std::string_view separator) {
  std::ostringstream stream;
  lazyStrJoin(&stream, AD_FWD(r), separator);
  return std::move(stream).str();
}

// ___________________________________________________________________________
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
  Insert separator into the given string and add it into the `ostream`. Ignores
  content of the given string, just works based on length.
  */
  auto insertSeparator = [&separatorSymbol,
                          &ostream](const std::string_view s) {
    // Nothing to do, if the string is to short.
    AD_CORRECTNESS_CHECK(s.length() > 3);

    /*
    For walking over the string view.
    Our initialization value skips the leading digits, so that only the digits
    remain, where we have to put the seperator in front of every three chars.
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
  auto parseIterator = std::begin(str);
  std::ranges::for_each(
      ctre::range<regexPatDigitSequence>(str),
      [&parseIterator, &ostream, &insertSeparator](const auto& match) {
        /*
        The digit sequence, that must be transformed. Note: The string view
        iterators point to entries in the `str` string.
        */
        const std::string_view& digitSequence{match.template get<"digit">()};

        // Insert the transformed digit sequence, and the string between it and
        // the `parseIterator`, into the stream.
        ostream << std::string_view(parseIterator, std::begin(digitSequence));
        insertSeparator(digitSequence);
        parseIterator = std::end(digitSequence);
      });
  ostream << std::string_view(std::move(parseIterator), std::end(str));
  return ostream.str();
}
}  // namespace ad_utility

// these overloads are missing in the STL
// TODO they can be constexpr once the compiler completely supports C++20
template <typename Char>
inline std::basic_string<Char> strCatImpl(const std::basic_string_view<Char>& a,
                                          std::basic_string_view<Char> b) {
  std::basic_string<Char> res;
  res.reserve(a.size() + b.size());
  res += a;
  res += b;
  return res;
}

template <typename Char>
inline std::basic_string<Char> operator+(const std::basic_string<Char>& a,
                                         std::basic_string_view<Char> b) {
  return strCatImpl(std::basic_string_view<Char>{a}, b);
}

template <typename Char>
inline std::basic_string<Char> operator+(const std::basic_string_view<Char>& a,
                                         std::basic_string<Char> b) {
  return strCatImpl(a, std::basic_string_view<Char>{b});
}

inline std::string operator+(char c, std::string_view b) {
  return strCatImpl(std::string_view(&c, 1), b);
}
