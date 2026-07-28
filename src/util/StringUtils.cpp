// Copyright 2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/StringUtils.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#ifndef QLEVER_NO_UNICODE
#include <unicode/bytestream.h>
#include <unicode/casemap.h>
#endif

#include <cctype>
#include <iterator>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/StringUtilsImpl.h"

namespace ad_utility {

namespace detail {
// CTRE regex pattern for C++17 compatibility
constexpr ctll::fixed_string langTagRegex = "[a-zA-Z]+(-[a-zA-Z0-9]+)*";
}  // namespace detail
// ____________________________________________________________________________
std::string_view commonPrefix(std::string_view a, const std::string_view b) {
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
std::string getLowercase(const std::string& orig) {
  std::string retVal;
  retVal.reserve(orig.size());
  for (size_t i = 0; i < orig.size(); ++i) {
    retVal += tolower(orig[i]);
  }
  return retVal;
}

// ____________________________________________________________________________
std::string getUppercase(const std::string& orig) {
  std::string retVal;
  retVal.reserve(orig.size());
  for (size_t i = 0; i < orig.size(); ++i) {
    retVal += toupper(orig[i]);
  }
  return retVal;
}

// ____________________________________________________________________________
bool strIsLangTag(const std::string& input) {
  return ctre::match<detail::langTagRegex>(input);
}

// ____________________________________________________________________________
bool isLanguageMatch(std::string& languageTag, std::string& languageRange) {
  if (languageRange.empty() || languageTag.empty()) {
    return false;
  } else {
    if (ql::ends_with(languageRange, "*")) {
      languageRange.pop_back();
    }
    ql::ranges::transform(languageTag, std::begin(languageTag),
                          [](unsigned char c) { return std::tolower(c); });
    ql::ranges::transform(languageRange, std::begin(languageRange),
                          [](unsigned char c) { return std::tolower(c); });
    return languageTag.compare(0, languageRange.length(), languageRange) == 0;
  }
}

// ___________________________________________________________________________
void utf8EncodeCodepoint(uint32_t codepoint, std::string& output) {
  // Encode `codepoint` according to the UTF-8 standard. Codepoints that are
  // not valid Unicode scalar values (larger than U+10FFFF, or in the surrogate
  // range U+D800..U+DFFF, which is reserved for UTF-16 and must never appear
  // in valid UTF-8) are replaced by U+FFFD (the replacement character).
  static constexpr uint32_t maxValidCodepoint = 0x10FFFF;
  static constexpr uint32_t firstSurrogate = 0xD800;
  static constexpr uint32_t lastSurrogate = 0xDFFF;
  if (codepoint > maxValidCodepoint ||
      (codepoint >= firstSurrogate && codepoint <= lastSurrogate)) {
    codepoint = 0xFFFD;
  }
  // A UTF-8 continuation byte has the two-bit header `10` followed by the next
  // six bits of the codepoint.
  static constexpr uint32_t continuationHeader = 0b1000'0000;
  static constexpr uint32_t lowestSixBits = 0b0011'1111;
  auto continuationByte = [](uint32_t bits) {
    return static_cast<char>(continuationHeader | (bits & lowestSixBits));
  };
  if (codepoint <= 0x7F) {
    // Single byte: `0xxxxxxx`.
    output += static_cast<char>(codepoint);
  } else if (codepoint <= 0x7FF) {
    // Two bytes: `110xxxxx 10xxxxxx`.
    output += static_cast<char>(0b1100'0000 | (codepoint >> 6));
    output += continuationByte(codepoint);
  } else if (codepoint <= 0xFFFF) {
    // Three bytes: `1110xxxx 10xxxxxx 10xxxxxx`.
    output += static_cast<char>(0b1110'0000 | (codepoint >> 12));
    output += continuationByte(codepoint >> 6);
    output += continuationByte(codepoint);
  } else {
    // Four bytes: `11110xxx 10xxxxxx 10xxxxxx 10xxxxxx`.
    output += static_cast<char>(0b1111'0000 | (codepoint >> 18));
    output += continuationByte(codepoint >> 12);
    output += continuationByte(codepoint >> 6);
    output += continuationByte(codepoint);
  }
}

// ___________________________________________________________________________
template <bool useICU>
std::pair<size_t, std::string_view> getUTF8Prefix(std::string_view sv,
                                                  size_t prefixLength) {
  // Counting codepoints only requires the byte structure of UTF-8 and no
  // Unicode tables, so both instantiations share this ICU-free
  // implementation. Malformed UTF-8 (invalid lead or continuation bytes,
  // overlong encodings, surrogates, values beyond U+10FFFF) is rejected
  // exactly like by ICU's `U8_NEXT`.
  size_t numCodepoints = 0;
  size_t i = 0;
  while (i < sv.size() && numCodepoints < prefixLength) {
    auto lead = static_cast<unsigned char>(sv[i]);
    // The length of the sequence and the payload bits of the lead byte.
    size_t sequenceLength = lead < 0x80             ? 1
                            : (lead & 0xE0) == 0xC0 ? 2
                            : (lead & 0xF0) == 0xE0 ? 3
                            : (lead & 0xF8) == 0xF0 ? 4
                                                    : 0;
    static constexpr uint32_t leadMask[] = {0, 0x7F, 0x1F, 0x0F, 0x07};
    bool valid = sequenceLength > 0 && i + sequenceLength <= sv.size();
    uint32_t codepoint = valid ? lead & leadMask[sequenceLength] : 0;
    for (size_t j = 1; valid && j < sequenceLength; ++j) {
      auto continuation = static_cast<unsigned char>(sv[i + j]);
      valid = (continuation & 0xC0) == 0x80;
      codepoint = (codepoint << 6) | (continuation & 0x3F);
    }
    static constexpr uint32_t minimumBySequenceLength[] = {0, 0, 0x80, 0x800,
                                                           0x10000};
    valid = valid && codepoint >= minimumBySequenceLength[sequenceLength] &&
            codepoint <= 0x10FFFF &&
            !(codepoint >= 0xD800 && codepoint <= 0xDFFF);
    if (!valid) {
      throw std::runtime_error(
          "Illegal UTF sequence in ad_utility::getUTF8Prefix");
    }
    i += sequenceLength;
    ++numCodepoints;
  }
  return {numCodepoints, sv.substr(0, i)};
}
// Explicit instantiations for both configurations.
template std::pair<size_t, std::string_view> getUTF8Prefix<true>(
    std::string_view, size_t);
template std::pair<size_t, std::string_view> getUTF8Prefix<false>(
    std::string_view, size_t);

#ifndef QLEVER_NO_UNICODE
namespace detail {
// The common implementation of `utf8ToLower` and `utf8ToUpper` (for
// details see below).
template <typename F>
std::string utf8StringTransform(std::string_view s, const char* localeName,
                                F transformation) {
  std::string result;
  icu::StringByteSink<std::string> sink(&result);
  UErrorCode err = U_ZERO_ERROR;
  transformation(localeName, 0,
                 icu::StringPiece{s.data(), static_cast<int32_t>(s.size())},
                 sink, nullptr, err);
  if (U_FAILURE(err)) {
    throw std::runtime_error(u_errorName(err));
  }
  return result;
}
}  // namespace detail
#endif  // QLEVER_NO_UNICODE

namespace {
// The common ICU-free implementation of `utf8ToLower` and `utf8ToUpper` (for
// details see below). Apply `transformation` to each byte of `s` separately,
// which only affects the ASCII characters. `localeName` is deliberately
// ignored, as locale-specific case folding requires ICU.
template <typename F>
std::string asciiStringTransform(std::string_view s,
                                 [[maybe_unused]] const char* localeName,
                                 F transformation) {
  return ::ranges::to<std::string>(
      s | ql::views::transform([&transformation](char c) {
        return static_cast<char>(transformation(static_cast<unsigned char>(c)));
      }));
}
}  // namespace

// ____________________________________________________________________________
template <bool useICU>
std::string utf8ToLower(std::string_view s, const char* localeName) {
  if constexpr (useICU) {
    QLEVER_UNICODE_ONLY("utf8ToLower", {
      return detail::utf8StringTransform(s, localeName, [](auto&&... args) {
        return icu::CaseMap::utf8ToLower(AD_FWD(args)...);
      });
    });
  } else {
    return asciiStringTransform(
        s, localeName, [](unsigned char c) { return std::tolower(c); });
  }
}
// Explicit instantiations for both configurations.
template std::string utf8ToLower<true>(std::string_view, const char*);
template std::string utf8ToLower<false>(std::string_view, const char*);

// ____________________________________________________________________________
template <bool useICU>
std::string utf8ToUpper(std::string_view s, const char* localeName) {
  if constexpr (useICU) {
    QLEVER_UNICODE_ONLY("utf8ToUpper", {
      return detail::utf8StringTransform(s, localeName, [](auto&&... args) {
        return icu::CaseMap::utf8ToUpper(AD_FWD(args)...);
      });
    });
  } else {
    return asciiStringTransform(
        s, localeName, [](unsigned char c) { return std::toupper(c); });
  }
}
// Explicit instantiations for both configurations.
template std::string utf8ToUpper<true>(std::string_view, const char*);
template std::string utf8ToUpper<false>(std::string_view, const char*);

// ____________________________________________________________________________
std::string_view getUTF8Substring(const std::string_view str, size_t start,
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
std::string_view getUTF8Substring(const std::string_view str, size_t start) {
  // `str.size()` is >= the number of codepoints because each codepoint has at
  // least one byte in UTF-8
  return getUTF8Substring(str, start, str.size());
}

// ____________________________________________________________________________
std::string getLastPartOfString(const std::string& text, const char separator) {
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
  while (endPos != std::string::npos) {
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

// ___________________________________________________________________________
std::string truncateOperationString(std::string_view operation) {
  auto prefix = getUTF8Prefix(operation, MAX_LENGTH_OPERATION_ECHO).second;
  if (prefix.length() == operation.length()) {
    return std::string{operation};
  }
  return absl::StrCat(prefix, "...");
}
}  // namespace ad_utility
