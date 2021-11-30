//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_URLPARSER_H
#define QLEVER_URLPARSER_H

#include <string>
#include <string_view>

#include "../HashMap.h"

namespace ad_utility {
/**
 * /brief A simple parser for urls/uris. It does NOT provide the full
 * URI specification, but only a small subset of it.
 *
 * TODO<joka921> Replace by a suitable third-party library when we have
 * chosen one. My current favourite is Boost::Url by Vinnie Falco, but there
 * exists no stable release yet.
 */
class UrlParser {
 public:
  struct UrlTarget {
    std::string _target;
    ad_utility::HashMap<std::string, std::string> _parameters;
  };

  // ___________________________________________________________________________
  static std::string applyPercentDecoding(std::string_view url);

  /// Parse the `target` part of an HTTP GET Request,
  /// for example, `/api.html?someKey=some+val%0Fue`.
  static UrlTarget parseTarget(std::string_view target);

  ///  From the `target` part of an HTTP GET request, only extract the path,
  ///  with percent decoding applied. E.g. `/target.html?key=value` will become
  ///  `/target.html`. Additionally the following checks are applied:
  ///  - The path must not contain `..` to escape from the document root.
  ///  - The path must be absolute (start with a slash `/`).
  ///  If the parsing or one of the checks fails, std::nullopt is returned.
  static std::optional<std::string> getDecodedPathAndCheck(
      std::string_view target) noexcept;

 private:
  static std::pair<std::string, std::string> parseSingleKeyValuePair(
      std::string_view input);
};
}  // namespace ad_utility

#endif  // QLEVER_URLPARSER_H
