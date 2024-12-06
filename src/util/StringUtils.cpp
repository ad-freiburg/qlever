// Copyright 2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/StringUtils.h"

#include <absl/strings/str_cat.h>
#include <unicode/bytestream.h>
#include <unicode/casemap.h>

#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"

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

// ____________________________________________________________________________
bool strIsLangTag(const string& input) {
  return ctre::match<"[a-zA-Z]+(-[a-zA-Z0-9]+)*">(input);
}

// ____________________________________________________________________________
bool isLanguageMatch(string& languageTag, string& languageRange) {
  if (languageRange.empty() || languageTag.empty()) {
    return false;
  } else {
    if (languageRange.ends_with("*")) {
      languageRange.pop_back();
    }
    std::ranges::transform(languageTag, std::begin(languageTag),
                           [](unsigned char c) { return std::tolower(c); });
    std::ranges::transform(languageRange, std::begin(languageRange),
                           [](unsigned char c) { return std::tolower(c); });
    return languageTag.compare(0, languageRange.length(), languageRange) == 0;
  }
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

  // if we have found any unescaped occurrence of literalEnd, return the last
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

// TODO<joka921> Define in the .cpp file and explicitly instantiate to reduce
// cyclic header complexities.
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
  auto parseIterator = std::begin(str);
  std::ranges::for_each(
      ctre::range<regexPatDigitSequence>(str),
      [&parseIterator, &ostream, &insertSeparator](const auto& match) {
        /*
        The digit sequence, that must be transformed. Note: The string view
        iterators point to entries in the `str` string.
        */
        const std::string_view& digitSequence{match.template get<"digit">()};

        // Insert the transformed digit sequence, and the string between it
        // and the `parseIterator`, into the stream.
        ostream << std::string_view(parseIterator, std::begin(digitSequence));
        insertSeparator(digitSequence);
        parseIterator = std::end(digitSequence);
      });
  ostream << std::string_view(std::move(parseIterator), std::end(str));
  return ostream.str();
}
template std::string insertThousandSeparator<46>(const std::string_view str,
                                                 const char separatorSymbol);

}  // namespace ad_utility
