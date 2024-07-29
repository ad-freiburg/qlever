// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_URLPARSER_H
#define QLEVER_URLPARSER_H

#include <boost/url.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "../HashMap.h"

namespace ad_utility {
/**
 * /brief Some helpers to parse request URLs in QLever.
 */
class UrlParser {
 public:
  // Representation of parsed request URL.
  // - `path_` is the URL path.
  // - `query_` and `update_` contain the Query or Update respectively. Only one
  // of the two will be non-empty.
  // - `parameters_` is a hashmap of the URL query parameters.
  struct ParsedUrl {
    std::string path_;
    ad_utility::HashMap<std::string, std::string> parameters_;
    std::optional<std::string> query_;
    std::optional<std::string> update_;
  };

  // Parse a request HTTP request target into its URL path, URL query
  // parameters. The contained query or update are not extracted.
  static UrlParser::ParsedUrl parseRequestTarget(std::string_view target);

  static ad_utility::HashMap<std::string, std::string> paramsToMap(
      boost::urls::params_view params);
};
}  // namespace ad_utility

#endif  // QLEVER_URLPARSER_H
