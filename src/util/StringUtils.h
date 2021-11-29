// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <assert.h>
#include <ctype.h>
#include <grp.h>
#include <stdint.h>
#include <stdlib.h>
#include <unicode/bytestream.h>
#include <unicode/casemap.h>

#include <array>
#include <clocale>
#include <cstring>
#include <cwchar>
#include <iostream>
#include <limits>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "../parser/ParseException.h"

using std::array;
using std::string;
using std::string_view;
using std::vector;

namespace ad_utility {
//! Utility functions for string. Can possibly be changed to
//! a templated version using std::basic_string<T> instead of
//! std::string. However, it is not required so far.

//! Safe startWith function. Returns true iff prefix is a
//! prefix of text. Using a larger pattern than text.size()
//! will return false. Case sensitive.
inline bool startsWith(string_view text, string_view prefix);

//! Safe startWith function. Returns true iff prefix is a
//! prefix of text. Using a larger pattern than text.size()
//! will return false. Case sensitive.
//! if prefixSize < prefix.size() we will only use the first prefisSize chars of
//! prefix.
inline bool startsWith(string_view text, string_view prefix, size_t prefixSize);

//! Safe endsWith function. Returns true iff suffix is a
//! prefix of text. Using a larger pattern than text.size()
//! will return false. Case sensitive.
inline bool endsWith(string_view text, const char* suffix, size_t patternSize);

//! Safe endsWith function. Returns true iff suffix is a
//! prefix of text. Using a larger pattern than text.size()
//! will return false. Case sensitive.
inline bool endsWith(string_view text, string_view suffix);

//! Safe endsWith function. Returns true iff suffix is a
//! prefix of text. Using a larger pattern than text.size()
//! will return false. Case sensitive.
inline bool endsWith(string_view text, const char* suffix);

//! Returns the longest prefix that the two arguments have in common
inline string_view commonPrefix(string_view a, const string_view b);

//! Case transformations. Should be thread safe.
inline string getLowercase(const string& orig);

inline string getUppercase(const string& orig);

inline string getLowercaseUtf8(std::string_view s);

inline string getUppercaseUtf8(const std::string& s);

inline string firstCharToUpperUtf8(const string& s);

inline std::pair<size_t, std::string> getUTF8Prefix(std::string_view s,
                                                    size_t prefixLength);

//! Gets the last part of a string that is somehow split by the given separator.
inline string getLastPartOfString(const string& text, const char separator);

inline string getSecondLastPartOfString(const string& text, const char sep);

inline string removeSpaces(const string& orig);

inline string normalizeSpaces(const string& orig);

//! Converts an optional string to a JSON string value.
//! I.e. it escapes illegal characters and adss quotes around the string.
//! Taking a std::optional<string> it takes normal strings (which are implicitly
//! converted or std::nullopt which it converts to 'null' (without quotes)
inline string toJson(const std::optional<string>& orig);

//! Strips any sequence of characters in s from the left and right of the text.
//! The type T can be anything convertible to a std::string_view<char> so in
//! particular s can be a string literal, std::string, std::string_view.
//! Additionally it can also be a single char as in ' '
template <class T>
inline string strip(const string& text, const T& s);
//! Strips any sequence of characters from s from the left of the text.
//! The type T can be anything convertible to a std::string_view<char> so in
//! particular s can be a string literal, std::string, std::string_view.
//! Additionally it can also be a single char as in ' '
template <class T>
inline string lstrip(const string& text, const T& s);
//! Strips any sequence of characters from s from the right of the text.
//! The type T can be anything convertible to a std::string_view<char> so in
//! particular s can be a string literal, std::string, std::string_view.
//! Additionally it can also be a single char as in ' '
template <class T>
inline string rstrip(const string& text, const T& s);

//! Splits a string at the separator, kinda like python.
inline vector<string> split(const string& orig, const char sep);

//! Splits a string at any maximum length sequence of whitespace
inline vector<string> splitWs(const string& orig);

//! Splits a string at any maximum length sequence of whitespace, ignoring
//! whitespace within an escaped sequence. Left char begins an escaped
//! sequence right ends it. If left == right the char toggles an escaped
//! sequence, otherwise the depth of opening chars is tracked and whitespace
//! is ignored as long as that depth is larger than 0.
inline vector<string> splitWsWithEscape(const string& orig, const char left,
                                        const char right);

//! Splits a string at any character found in the separators string.
//! The type T can be anything convertible to a std::string_view so in
//! particular seps can be a string literal, a std::string or
//! a std::string_view.
//! This is analogous to how std::string::find_first_of() works
template <class T>
inline vector<string> splitAny(const string& orig, const T& separators);

//! Similar to Python's ",".join(somelist) this joins elements of the given
//! vector to_join, separating them by joiner.
//! In order for this to work the type J needs an operator+= implementation that
//! accepts a value of type S as the right hand side
template <typename J, typename S>
J join(const vector<J>& to_join, const S& joiner);

//! This scans the haystack from start onward until it finds a closing bracket
//! at the same bracket depth as the opening bracket.
//! This throws a Parse exception, if haystack[start] != openingBracket, or if
//! the end of teh haystack is reached before a bracket at the right depth was
//! found.
inline size_t findClosingBracket(const string& haystack, size_t start = 0,
                                 char openingBracket = '{',
                                 char closingBracket = '}');

/**
 * @brief Return the last position where <literalEnd> was found in the <input>
 * without being escaped by backslashes. If it is not found at all, string::npos
 * is returned.
 */
inline size_t findLiteralEnd(std::string_view input,
                             std::string_view literalEnd);

// *****************************************************************************
// Definitions:
// *****************************************************************************

// ____________________________________________________________________________
bool startsWith(string_view text, string_view prefix) {
  if (prefix.size() > text.size()) {
    return false;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (text[i] != prefix[i]) {
      return false;
    }
  }
  return true;
}

// _______________________________________________________________________
bool startsWith(string_view text, string_view prefix, size_t prefixSize) {
  return startsWith(text,
                    prefix.substr(0, std::min(prefix.size(), prefixSize)));
}

/* ____________________________________________________________________________
bool startsWith(const string& text, const char* prefix) {
  return startsWith(text, prefix, std::char_traits<char>::length(prefix));
}
*/

// ____________________________________________________________________________
bool endsWith(string_view text, const char* suffix, size_t suffixSize) {
  if (suffixSize > text.size()) {
    return false;
  }
  for (size_t i = 0; i < suffixSize; ++i) {
    if (text[text.size() - (i + 1)] != suffix[suffixSize - (i + 1)]) {
      return false;
    }
  }
  return true;
}

// ____________________________________________________________________________
bool endsWith(string_view text, std::string_view suffix) {
  return endsWith(text, suffix.data(), suffix.size());
}

// ____________________________________________________________________________
bool endsWith(string_view text, const char* suffix) {
  return endsWith(text, suffix, std::char_traits<char>::length(suffix));
}

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

// ____________________________________________________________________________
/*
 * @brief convert a UTF-8 String to lowercase according to the held locale
 * @param s UTF-8 encoded string
 * @return The lowercase version of s, also encoded as UTF-8
 */
std::string getLowercaseUtf8(std::string_view s) {
  std::string result;
  icu::StringByteSink<std::string> sink(&result);
  UErrorCode err = U_ZERO_ERROR;
  icu::CaseMap::utf8ToLower("", 0, s, sink, nullptr, err);
  if (U_FAILURE(err)) {
    throw std::runtime_error(u_errorName(err));
  }
  return result;
}

// ____________________________________________________________________________
string getUppercaseUtf8(const std::string& s) {
  std::string result;
  icu::StringByteSink<std::string> sink(&result);
  UErrorCode err = U_ZERO_ERROR;
  icu::CaseMap::utf8ToUpper("", 0, s, sink, nullptr, err);
  if (U_FAILURE(err)) {
    throw std::runtime_error(u_errorName(err));
  }
  return result;
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
std::pair<size_t, std::string> getUTF8Prefix(std::string_view sv,
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
  return {numCodepoints, std::string(sv.data(), i)};
}

// ____________________________________________________________________________
inline string firstCharToUpperUtf8(const string& s) {
  auto [numCodepoints, prefix] = getUTF8Prefix(s, 1);
  (void)numCodepoints;
  auto prefixSize = prefix.size();
  prefix = getUppercaseUtf8(prefix);
  return prefix + s.substr(prefixSize);
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

// ____________________________________________________________________________
string getSecondLastPartOfString(const string& text, const char separator) {
  size_t pos = text.rfind(separator);
  size_t pos2 = text.rfind(separator, pos - 1);
  if (pos2 != text.npos) {
    return text.substr(pos2 + 1, pos - (pos2 + 1));
  } else {
    return text;
  }
}

// ____________________________________________________________________________
string removeSpaces(const string& orig) {
  string ret;
  ret.reserve(orig.size());
  for (size_t i = 0; i < orig.size(); ++i) {
    if (orig[i] != ' ') {
      ret += orig[i];
    }
  }
  return ret;
}

// ____________________________________________________________________________
string normalizeSpaces(const string& orig) {
  string ret;
  ret.reserve(orig.size());
  bool lastSpace = false;
  for (size_t i = 0; i < orig.size(); ++i) {
    if (orig[i] == ' ' || orig[i] == '\t') {
      if (!lastSpace) {
        ret += ' ';
      }
      lastSpace = true;
    } else {
      ret += orig[i];
      lastSpace = false;
    }
  }
  return ret;
}

// ____________________________________________________________________________
string toJson(const std::optional<string>& input) {
  if (!input) {
    return "null";
  }
  const auto& orig = input.value();
  // First we should determine the size of the result.
  // As a linear scan this should almost always be faster than
  // having to realloc. + 2 for quotes
  size_t rsize = orig.size() + 2;
  for (const auto& c : orig) {
    switch (c) {
      case '\t':
      case '\v':
      case '\0':
      case '\f':
      case '\b':
      case '\n':
      case '\"':
      case '\\':
        rsize++;
        continue;
      default:
        continue;
    }
  }
  string ret;
  ret.reserve(rsize);
  ret += '"';
  for (const auto& c : orig) {
    switch (c) {
      case '\t':
        ret += "\\t";
        continue;
      case '\v':
        ret += "\\v";
        continue;
      case '\0':
        ret += "\\0";
        continue;
      case '\f':
        ret += "\\f";
        continue;
      case '\b':
        ret += "\\b";
        continue;
      case '\n':
        ret += "\\n";
        continue;
      case '\"':
      case '\\':
        ret += '\\';
        [[fallthrough]];
      default:
        ret += c;
    }
  }
  ret += '"';
  return ret;
}

// _____________________________________________________________________________
vector<string> split(const string& orig, const char sep) {
  vector<string> result;
  if (orig.size() > 0) {
    size_t from = 0;
    size_t sepIndex = orig.find(sep);
    while (sepIndex != string::npos) {
      result.emplace_back(orig.substr(from, sepIndex - from));
      from = sepIndex + 1;
      sepIndex = orig.find(sep, from);
    }
    result.emplace_back(orig.substr(from));
  }
  return result;
}

// _____________________________________________________________________________
vector<string> splitWs(const string& orig) {
  vector<string> result;
  if (orig.size() > 0) {
    size_t start = 0;
    size_t pos = 0;
    while (pos < orig.size()) {
      if (::isspace(static_cast<unsigned char>(orig[pos]))) {
        if (start != pos) {
          result.emplace_back(orig.substr(start, pos - start));
        }
        // skip any whitespace
        while (pos < orig.size() &&
               ::isspace(static_cast<unsigned char>(orig[pos]))) {
          pos++;
        }
        start = pos;
      }
      pos++;
    }
    // avoid adding whitespace at the back of the string
    if (start != orig.size()) {
      result.emplace_back(orig.substr(start));
    }
  }
  return result;
}

// _____________________________________________________________________________
inline vector<string> splitWsWithEscape(const string& orig, const char left,
                                        const char right) {
  vector<string> result;
  if (orig.size() > 0) {
    size_t start = 0;
    size_t pos = 0;
    int depth = 0;
    while (pos < orig.size()) {
      if (depth <= 0 && ::isspace(static_cast<unsigned char>(orig[pos]))) {
        if (start != pos) {
          result.emplace_back(orig.substr(start, pos - start));
        }
        // skip any whitespace
        while (pos < orig.size() &&
               ::isspace(static_cast<unsigned char>(orig[pos]))) {
          pos++;
        }
        start = pos;
      }
      if (orig[pos] == left) {
        depth++;
        if (left == right) {
          depth %= 2;
        }
      } else if (orig[pos] == right) {
        depth--;
      }
      pos++;
    }
    // avoid adding whitespace at the back of the string
    if (start != orig.size()) {
      result.emplace_back(orig.substr(start));
    }
  }
  return result;
}

// _____________________________________________________________________________
template <class T>
vector<string> splitAny(const string& orig, const T& separators) {
  // Converting the input into a std::string_view gives us a size() method and
  // iterators. It works with both const char* and std::string without needing
  // a copy
  std::string_view seps(separators);
  // 256 bytes should fit on almost any stack
  // note the {}, it initializes with 0 = false
  array<bool, std::numeric_limits<unsigned char>::max() + 1> chars{};
  for (size_t i = 0; i < seps.size(); ++i) {
    chars[seps[i]] = true;
  }
  vector<string> result;
  if (orig.size() > 0) {
    size_t from = 0;
    size_t i = 0;
    while (i < orig.size()) {
      if (chars[static_cast<unsigned char>(orig[i])]) {
        if (from < i) {
          result.emplace_back(orig.substr(from, i - from));
        }
        from = i + 1;
      }
      ++i;
    }
    if (from < orig.size()) {
      result.emplace_back(orig.substr(from));
    }
  }
  return result;
}

// _____________________________________________________________________________
template <typename J, typename S>
J join(const vector<J>& to_join, const S& joiner) {
  J res{};  // {} does zero initialization
  auto it = to_join.begin();
  if (it == to_join.end()) {
    return res;
  }
  res += *it++;
  for (; it != to_join.end(); it++) {
    res += joiner;
    res += *it;
  }
  return res;
}

// _____________________________________________________________________________
template <class T>
inline string lstrip(const string& text, const T& s) {
  auto pos = text.find_first_not_of(s);
  if (pos == string::npos) {
    return string{};
  }
  return text.substr(pos);
}

// _____________________________________________________________________________
template <class T>
inline string rstrip(const string& text, const T& s) {
  auto pos = text.find_last_not_of(s);
  if (pos == string::npos) {
    return string{};
  }
  auto length = pos + 1;
  return text.substr(0, length);
}

// _____________________________________________________________________________
template <class T>
inline string strip(const string& text, const T& s) {
  auto pos = text.find_first_not_of(s);
  if (pos == string::npos) {
    return string{};
  }
  auto length = text.find_last_not_of(s) - pos + 1;
  return text.substr(pos, length);
}

inline size_t findClosingBracket(const string& haystack, size_t start,
                                 char openingBracket, char closingBracket) {
  if (haystack[start] != openingBracket) {
    throw ParseException(
        "The string " + haystack + " does not have a opening bracket " +
        string(1, openingBracket) + " at position " + std::to_string(start));
  }
  int depth = 0;
  size_t i;
  for (i = start + 1; i < haystack.size(); i++) {
    if (haystack[i] == openingBracket) {
      depth++;
    }
    if (haystack[i] == closingBracket) {
      if (depth == 0) {
        return i;
      } else {
        depth--;
      }
    }
  }
  if (depth == 0) {
    throw ParseException("Need curly braces in string " + haystack + "clause.");
  } else {
    throw ParseException("Unbalanced curly braces.");
  }
  return -1;
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

}  // namespace ad_utility

// these overloads are missing in the STL
inline std::string operator+(const std::string& a, std::string_view b) {
  std::string res;
  res.resize(a.size() + b.size());
  std::memcpy(res.data(), a.data(), a.size());
  std::memcpy(res.data() + a.size(), b.data(), b.size());
  return res;
}

inline std::string operator+(char c, std::string_view b) {
  std::string res;
  res.resize(1 + b.size());
  res[0] = c;
  std::memcpy(res.data() + 1, b.data(), b.size());
  return res;
}
