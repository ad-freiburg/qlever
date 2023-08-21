// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <unicode/bytestream.h>
#include <unicode/casemap.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <functional>
#include <iterator>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>

#include "util/Concepts.h"
#include "util/Exception.h"
#include "util/Forward.h"

using std::string;
using std::string_view;

namespace ad_utility {
//! Utility functions for string. Can possibly be changed to
//! a templated version using std::basic_string<T> instead of
//! std::string. However, it is not required so far.

//! Returns the longest prefix that the two arguments have in common
inline string_view commonPrefix(string_view a, string_view b);

//! Case transformations. Should be thread safe.
inline string getLowercase(const string& orig);

inline string getUppercase(const string& orig);

inline string utf8ToLower(std::string_view s);

inline std::pair<size_t, std::string_view> getUTF8Prefix(std::string_view s,
                                                         size_t prefixLength);

//! Gets the last part of a string that is somehow split by the given separator.
inline string getLastPartOfString(const string& text, const char separator);

/**
 * @brief Return the last position where <literalEnd> was found in the <input>
 * without being escaped by backslashes. If it is not found at all, string::npos
 * is returned.
 */
inline size_t findLiteralEnd(std::string_view input,
                             std::string_view literalEnd);

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
inline std::string addIndentation(std::string_view str,
                                  std::string_view indentationSymbol);

// *****************************************************************************
// Definitions:
// *****************************************************************************

// ____________________________________________________________________________
string_view commonPrefix(string_view a, const string_view b) {
  size_t maxIdx = std::min(a.size(), b.size());
  size_t i = 0;
  while (i < maxIdx) {
    if (a[i] != b[i]) {
      break;
    }
    ++i;
  }
  return a.substr(0, i);
}

// ____________________________________________________________________________
string getLowercase(const string& orig) {
  string retVal;
  retVal.reserve(orig.size());
  for (size_t i = 0; i < orig.size(); ++i) {
    retVal += tolower(orig[i]);
  }
  return retVal;
}

// ____________________________________________________________________________
string getUppercase(const string& orig) {
  string retVal;
  retVal.reserve(orig.size());
  for (size_t i = 0; i < orig.size(); ++i) {
    retVal += toupper(orig[i]);
  }
  return retVal;
}

namespace detail {
// The common implementation of `utf8ToLower` and `utf8ToUpper` (for
// details see below).
std::string utf8StringTransform(std::string_view s, auto transformation) {
  std::string result;
  icu::StringByteSink<std::string> sink(&result);
  UErrorCode err = U_ZERO_ERROR;
  transformation("", 0,
                 icu::StringPiece{s.data(), static_cast<int32_t>(s.size())},
                 sink, nullptr, err);
  if (U_FAILURE(err)) {
    throw std::runtime_error(u_errorName(err));
  }
  return result;
}
}  // namespace detail

// ____________________________________________________________________________
/*
 * @brief convert a UTF-8 String to lowercase according to the held locale
 * @param s UTF-8 encoded string
 * @return The lowercase version of s, also encoded as UTF-8
 */
std::string utf8ToLower(std::string_view s) {
  return detail::utf8StringTransform(s, [](auto&&... args) {
    return icu::CaseMap::utf8ToLower(AD_FWD(args)...);
  });
}

// Get the uppercase value. For details see `utf8ToLower` above
inline std::string utf8ToUpper(std::string_view s) {
  return detail::utf8StringTransform(s, [](auto&&... args) {
    return icu::CaseMap::utf8ToUpper(AD_FWD(args)...);
  });
}

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
std::pair<size_t, std::string_view> getUTF8Prefix(std::string_view sv,
                                                  size_t prefixLength) {
  const char* s = sv.data();
  int32_t length = sv.length();
  size_t numCodepoints = 0;
  int32_t i = 0;
  for (i = 0; i < length && numCodepoints < prefixLength;) {
    UChar32 c;
    U8_NEXT(s, i, length, c);
    if (c >= 0) {
      ++numCodepoints;
    } else {
      throw std::runtime_error(
          "Illegal UTF sequence in ad_utility::getUTF8Prefix");
    }
  }
  return {numCodepoints, sv.substr(0, i)};
}

/**
 * Get the substring from the UTF8-encoded str that starts at the start-th
 * codepoint as has a length of size codepoints. If start >= the number of
 * codepoints in str, an empty string is returned. If start + size >= the number
 * of codepoints in str then the substring will reach until the end of str. This
 * behavior is consistent with std::string::substr, but working on UTF-8
 * characters that might have multiple bytes.
 */
inline string_view getUTF8Substring(const std::string_view str, size_t start,
                                    size_t size) {
  // To generate a substring we have to "cut off" part of the string at the
  // start and end. The end can be removed with `getUTF8Prefix`.
  auto strWithEndRemoved = getUTF8Prefix(str, start + size).second;
  // Generate the prefix that should be removed from `str`. Actually remove it
  // from `str` by using the size in UTF-8 of the prefix and `string.substr`.
  auto prefixToRemove = getUTF8Prefix(strWithEndRemoved, start).second;
  return strWithEndRemoved.substr(prefixToRemove.size());
}
// Overload for the above function that creates the substring from the
// `start`-th codepoint to the end of the string.
inline string_view getUTF8Substring(const std::string_view str, size_t start) {
  // `str.size()` is >= the number of codepoints because each codepoint has at
  // least one byte in UTF-8
  return getUTF8Substring(str, start, str.size());
}

// ____________________________________________________________________________
string getLastPartOfString(const string& text, const char separator) {
  size_t pos = text.rfind(separator);
  if (pos != text.npos) {
    return text.substr(pos + 1);
  } else {
    return text;
  }
}

// _________________________________________________________________________
inline size_t findLiteralEnd(const std::string_view input,
                             const std::string_view literalEnd) {
  // keep track of the last position where the literalEnd was found unescaped
  auto lastFoundPos = size_t(-1);
  auto endPos = input.find(literalEnd, 0);
  while (endPos != string::npos) {
    if (endPos > 0 && input[endPos - 1] == '\\') {
      size_t numBackslash = 1;
      auto slashPos = endPos - 2;
      // the first condition checks > 0 for unsigned numbers
      while (slashPos < input.size() && input[slashPos] == '\\') {
        slashPos--;
        numBackslash++;
      }
      if (numBackslash % 2 == 0) {
        // even number of backslashes means that the quote we found has not
        // been escaped
        break;
      }
      endPos = input.find(literalEnd, endPos + 1);
    } else {
      // no backslash before the literalEnd, mark this as a candidate position
      lastFoundPos = endPos;
      endPos = input.find(literalEnd, endPos + 1);
    }
  }

  // if we have found any unescaped occurence of literalEnd, return the last
  // of these positions
  if (lastFoundPos != size_t(-1)) {
    return lastFoundPos;
  }
  return endPos;
}

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
std::string addIndentation(std::string_view str,
                           std::string_view indentationSymbol) {
  // An empty indentation makes no sense. Must be an error.
  AD_CONTRACT_CHECK(!indentationSymbol.empty());

  // Add an indentation to the beginning and replace a new line with a new line,
  // directly followed by the indentation.
  return absl::StrCat(
      indentationSymbol,
      absl::StrReplaceAll(str,
                          {{"\n", absl::StrCat("\n", indentationSymbol)}}));
}

}  // namespace ad_utility

// these overloads are missing in the STL
// they can be constexpr once the compiler completely supports C++20
inline std::string operator+(const std::string& a, std::string_view b) {
  std::string res;
  res.reserve(a.size() + b.size());
  res += a;
  res += b;
  return res;
}

inline std::string operator+(char c, std::string_view b) {
  std::string res;
  res.reserve(1 + b.size());
  res += c;
  res += b;
  return res;
}
