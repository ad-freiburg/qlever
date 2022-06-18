

//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "UrlParser.h"

#include "../Exception.h"
using namespace ad_utility;

using std::string;
// _____________________________________________________________________________
string UrlParser::applyPercentDecoding(std::string_view url) {
  string decoded;
  for (size_t i = 0; i < url.size(); ++i) {
    if (url[i] == '+') {
      decoded += ' ';
    } else if (url[i] == '%' && i + 2 < url.size()) {
      char h1 = tolower(url[i + 1]);
      if (h1 >= '0' && h1 <= '9') {
        h1 = h1 - '0';
      } else if (h1 >= 'a' && h1 <= 'f') {
        h1 = h1 - 'a' + 10;
      } else {
        decoded += '%';
        continue;
      }
      char h2 = tolower(url[i + 2]);
      if (h2 >= '0' && h2 <= '9') {
        h2 = h2 - '0';
      } else if (h2 >= 'a' && h2 <= 'f') {
        h2 = h2 - 'a' + 10;
      } else {
        decoded += '%';
        continue;
      }
      decoded += static_cast<char>(h1 * 16 + h2);
      i += 2;
    } else {
      decoded += url[i];
    }
  }
  return decoded;
}

// ___________________________________________________________________________
UrlParser::UrlTarget UrlParser::parseTarget(std::string_view target) {
  static constexpr auto npos = std::string_view::npos;
  UrlTarget result;

  target = target.substr(0, target.find('#'));
  size_t index = target.find('?');
  result._target = target.substr(0, index);
  if (index == npos) {
    return result;
  }
  target.remove_prefix(index + 1);
  while (true) {
    auto next = target.find('&');
    auto paramAndValue = parseSingleKeyValuePair(target.substr(0, next));
    auto [iterator, isNewElement] =
        result._parameters.insert(std::move(paramAndValue));
    if (!isNewElement) {
      AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
               "Duplicate HTTP parameter: " + iterator->first);
    }
    if (next == npos) {
      break;
    }
    target.remove_prefix(next + 1);
  }
  return result;
}

// ____________________________________________________________________________
std::pair<std::string, std::string> UrlParser::parseSingleKeyValuePair(
    std::string_view input) {
  size_t posOfEq = input.find('=');
  if (posOfEq == std::string_view::npos) {
    AD_THROW(ad_semsearch::Exception::BAD_REQUEST,
             "Parameter without \"=\" in HTTP Request. " + std::string{input});
  }
  std::string param{applyPercentDecoding(input.substr(0, posOfEq))};
  std::string value{applyPercentDecoding(input.substr(posOfEq + 1))};
  return {std::move(param), std::move(value)};
}

// _________________________________________________________________________
std::optional<std::string> UrlParser::getDecodedPathAndCheck(
    std::string_view target) noexcept {
  try {
    auto filename = parseTarget(target)._target;
    AD_CHECK(filename.starts_with('/'));
    AD_CHECK(filename.find("..") == string::npos);
    return filename;
  } catch (...) {
    return std::nullopt;
  }
}
