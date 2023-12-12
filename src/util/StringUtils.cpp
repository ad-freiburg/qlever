// Copyright 2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/StringUtils.h"

namespace ad_utility {
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

// ___________________________________________________________________________
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
std::string utf8ToLower(std::string_view s) {
  return detail::utf8StringTransform(s, [](auto&&... args) {
    return icu::CaseMap::utf8ToLower(AD_FWD(args)...);
  });
}

// ____________________________________________________________________________
std::string utf8ToUpper(std::string_view s) {
  return detail::utf8StringTransform(s, [](auto&&... args) {
    return icu::CaseMap::utf8ToUpper(AD_FWD(args)...);
  });
}

// ____________________________________________________________________________
string_view getUTF8Substring(const std::string_view str, size_t start,
                             size_t size) {
  // To generate a substring we have to "cut off" part of the string at the
  // start and end. The end can be removed with `getUTF8Prefix`.
  auto strWithEndRemoved = getUTF8Prefix(str, start + size).second;
  // Generate the prefix that should be removed from `str`. Actually remove it
  // from `str` by using the size in UTF-8 of the prefix and `string.substr`.
  auto prefixToRemove = getUTF8Prefix(strWithEndRemoved, start).second;
  return strWithEndRemoved.substr(prefixToRemove.size());
}

// ____________________________________________________________________________
string_view getUTF8Substring(const std::string_view str, size_t start) {
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
size_t findLiteralEnd(const std::string_view input,
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
