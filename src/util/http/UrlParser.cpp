// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "UrlParser.h"

#include "../Exception.h"

using namespace ad_utility;

// _____________________________________________________________________________
std::string UrlParser::applyPercentDecoding(std::string_view url,
                                            bool urlDecode) {
  // If not decoding wanted, just convert to `std::string`.
  if (urlDecode == false) {
    return std::string{url};
  }
  // Otherwise resolve all %XX.
  std::string decoded;
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
UrlParser::UrlPathAndParameters UrlParser::parseGetRequestTarget(
    std::string_view target, bool urlDecode) {
  UrlPathAndParameters result;

  // Remove everything after # (including it). Does nothing if there is no #.
  // Don't do this is `urlDecode == false` because in that case, the given
  // string contains an unencode SPARQL query, which frequently contains a # as
  // a regular character.
  if (urlDecode == true) {
    target = target.substr(0, target.find('#'));
  }

  // Set `_path` and remove it from `target`. If there is no query string (part
  // starting with "?"), we are done at this point.
  size_t index = target.find('?');
  result._path = target.substr(0, index);
  if (index == std::string::npos) {
    return result;
  }
  target.remove_prefix(index + 1);

  // Parse the query string and store the result in a hash map. Throw an error
  // if the same key appears twice in the query string. Note that this excludes
  // having two "cmd=..." parameters, although that would be meaningful (though
  // not necessary) to support.
  while (true) {
    auto next = target.find('&');
    auto paramAndValue =
        parseSingleKeyValuePair(target.substr(0, next), urlDecode);
    auto [iterator, isNewElement] =
        result._parameters.insert(std::move(paramAndValue));
    if (!isNewElement) {
      AD_THROW("Duplicate HTTP parameter: " + iterator->first);
    }
    if (next == std::string::npos) {
      break;
    }
    target.remove_prefix(next + 1);
  }
  return result;
}

// ____________________________________________________________________________
std::pair<std::string, std::string> UrlParser::parseSingleKeyValuePair(
    std::string_view input, bool urlDecode) {
  size_t posOfEq = input.find('=');
  if (posOfEq == std::string_view::npos) {
    AD_THROW("Parameter without \"=\" in HTTP Request. " + std::string{input});
  }
  std::string param{applyPercentDecoding(input.substr(0, posOfEq), urlDecode)};
  std::string value{applyPercentDecoding(input.substr(posOfEq + 1), urlDecode)};
  return {std::move(param), std::move(value)};
}

// _________________________________________________________________________
std::optional<std::string> UrlParser::getDecodedPathAndCheck(
    std::string_view target) noexcept {
  try {
    auto filename = parseGetRequestTarget(target)._path;
    AD_CONTRACT_CHECK(filename.starts_with('/'));
    AD_CONTRACT_CHECK(filename.find("..") == string::npos);
    return filename;
  } catch (...) {
    return std::nullopt;
  }
}
