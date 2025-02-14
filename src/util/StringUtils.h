// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <string_view>

#include "backports/algorithm.h"
#include "util/Concepts.h"
#include "util/ConstexprSmallString.h"

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

// Implements a case insensitive `language-range` to `language-tag`comparison.
bool isLanguageMatch(string& languageTag, string& languageRange);

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

@tparam Range An input range, whose dereferenced iterators can be inserted into
streams.

@param separator Will be put between each of the string representations
of the range elements.
*/
CPP_template(typename Range)(
    requires ql::ranges::input_range<Range> CPP_and
        ad_utility::Streamable<std::iter_reference_t<ql::ranges::iterator_t<
            Range>>>) void lazyStrJoin(std::ostream* stream, Range&& r,
                                       std::string_view separator);

// Similar to the overload of `lazyStrJoin` above, but the result is returned as
// a string.
CPP_template(typename Range)(
    requires ql::ranges::input_range<Range> CPP_and ad_utility::Streamable<
        std::iter_reference_t<ql::ranges::iterator_t<Range>>>) std::string
    lazyStrJoin(Range&& r, std::string_view separator);

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

Note: To avoid cyclic dependencies, this function is defined in a separate file
`StringUtilsImpl.h`. This file is then included in the `StringUtils.cpp` with an
explicit instantiation for the default template argument `.`. The tests include
the impl file directly to exhaustively test the behavior for other template
arguments.
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
CPP_template_def(typename Range)(
    requires ql::ranges::input_range<Range> CPP_and_def
        ad_utility::Streamable<std::iter_reference_t<ql::ranges::iterator_t<
            Range>>>) void lazyStrJoin(std::ostream* stream, Range&& r,
                                       std::string_view separator) {
  auto begin = std::begin(r);
  auto end = std::end(r);

  // Is the range empty?
  if (begin == end) {
    return;
  }

  // Add the first entry without a separator.
  *stream << *begin;

  // Add the remaining entries.
  ++begin;
  ql::ranges::for_each(begin, end,
                       [&stream, &separator](const auto& listItem) {
                         *stream << separator << listItem;
                       },
                       {});
}

// _________________________________________________________________________
CPP_template_def(typename Range)(
    requires ql::ranges::input_range<Range> CPP_and_def ad_utility::Streamable<
        std::iter_reference_t<ql::ranges::iterator_t<Range>>>) std::string
    lazyStrJoin(Range&& r, std::string_view separator) {
  std::ostringstream stream;
  lazyStrJoin(&stream, AD_FWD(r), separator);
  return std::move(stream).str();
}

// The implementation of `constexprStrCat` below.
namespace detail::constexpr_str_cat_impl {
// We currently have a fixed upper bound of 100 characters on the inputs.
// This can be changed once it becomes a problem. It would also be possible
// to have flexible upper bounds, but this would make the implementation much
// more complicated.
using ConstexprString = ad_utility::ConstexprSmallString<100>;

// Concatenate the elements of `arr` into a single array with an additional
// zero byte at the end. `sz` must be the sum of the sizes in `arr`, else the
// behavior is undefined.
template <size_t sz, size_t numStrings>
constexpr std::array<char, sz + 1> catImpl(
    const std::array<ConstexprString, numStrings>& arr) {
  std::array<char, sz + 1> buf{};
  auto it = buf.begin();
  for (const auto& str : arr) {
    for (size_t i = 0; i < str.size(); ++i) {
      *it = str[i];
      ++it;
    }
  }
  return buf;
};
// Concatenate the `strings` into a single `std::array<char>` with an
// additional zero byte at the end.
template <ConstexprString... strings>
constexpr auto constexprStrCatBufferImpl() {
  constexpr size_t sz = (size_t{0} + ... + strings.size());
  constexpr auto innerResult =
      catImpl<sz>(std::array<ConstexprString, sizeof...(strings)>{strings...});
  return innerResult;
}

// A constexpr variable that stores the concatenation of the `strings`.
// TODO<C++26> This can be a `static constexpr` variable inside the
// `constexprStrCatBufferImpl()` function above.
template <ConstexprString... strings>
constexpr inline auto constexprStrCatBufferVar =
    constexprStrCatBufferImpl<strings...>();
}  // namespace detail::constexpr_str_cat_impl

// Return the concatenation of the `strings` as a `string_view`. Can be
// evaluated at compile time. The buffer that backs the returned `string_view`
// will be zero-terminated, so it is safe to pass pointers into the result
// into legacy C-APIs.
template <detail::constexpr_str_cat_impl::ConstexprString... strings>
constexpr std::string_view constexprStrCat() {
  const auto& b =
      detail::constexpr_str_cat_impl::constexprStrCatBufferVar<strings...>;
  return {b.data(), b.size() - 1};
}
}  // namespace ad_utility

// A helper function for the `operator+` overloads below.
template <typename Char>
std::basic_string<Char> strCatImpl(const std::basic_string_view<Char>& a,
                                   std::basic_string_view<Char> b) {
  std::basic_string<Char> res;
  res.reserve(a.size() + b.size());
  res += a;
  res += b;
  return res;
}

// These overloads of `operator+` between a `string` and a `string_view`  are
// missing in the STL.
// TODO they can be constexpr once the compiler completely supports C++20
template <typename Char>
std::basic_string<Char> operator+(const std::basic_string<Char>& a,
                                  std::basic_string_view<Char> b) {
  return strCatImpl(std::basic_string_view<Char>{a}, b);
}

template <typename Char>
std::basic_string<Char> operator+(const std::basic_string_view<Char>& a,
                                  std::basic_string<Char> b) {
  return strCatImpl(a, std::basic_string_view<Char>{b});
}

template <typename Char>
std::string operator+(Char c, std::basic_string_view<Char> b) {
  return strCatImpl(std::string_view(&c, 1), b);
}
