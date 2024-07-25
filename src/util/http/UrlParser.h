// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_URLPARSER_H
#define QLEVER_URLPARSER_H

#include <optional>
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
  /// Representation of the "path" and "query" of a URL. For a GET request, the
  /// "path" is the part before the "?" (or everything if there is no "?"), and
  /// the "query" is the part after the "?" (empty if there is no "?").
  struct UrlPathAndQuery {
    std::string path_;
    std::string query_;
  };

  /// The difference to `UrlPathAndQuery` is that the key-value pairs of the
  /// "query" are parsed and stored in a hash map.
  struct UrlPathAndParameters {
    std::string _path;
    ad_utility::HashMap<std::string, std::string> _parameters;
  };

  // URL-decode the given (part of a) URL. If the second argument is false, do
  // nothing except converting the given `std::string_view` to `std::string`.
  static std::string applyPercentDecoding(std::string_view url,
                                          bool urlDecode = true);

  /// Split the `target` of an HTTP Request (e.g.
  /// `/api.html?someKey=some+val%0Fue`) into the path (`/api.html`) and query
  /// (`someKey=some+val%0Fue`).
  static UrlPathAndQuery splitPathAndQuery(std::string_view target);

  /// Parse the `target` part of an HTTP Request, for example,
  /// `/api.html?someKey=some+val%0Fue`. The second argument specifies whether
  /// the key-value pairs of the query string should be URL-decoded (default:
  /// yes).
  static UrlPathAndParameters parseGetRequestTarget(std::string_view target,
                                                    bool urlDecode = true);

  ///  From the `target` part of an HTTP request, only extract the path,
  ///  with percent decoding applied. E.g. `/target.html?key=value` will become
  ///  `/target.html`. Additionally the following checks are applied:
  ///  - The path must not contain `..` to escape from the document root.
  ///  - The path must be absolute (start with a slash `/`).
  ///  If the parsing or one of the checks fails, std::nullopt is returned.
  static std::optional<std::string> getDecodedPathAndCheck(
      std::string_view target) noexcept;

 private:
  // Helper function that parses a single key-value pair from a URL query
  // string. The second argument specifies whether the key and value should be
  // URL-decoded (default: yes).
  static std::pair<std::string, std::string> parseSingleKeyValuePair(
      std::string_view input, bool urlDecode = true);
};
}  // namespace ad_utility

#endif  // QLEVER_URLPARSER_H
