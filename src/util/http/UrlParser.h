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
  // A parsed URL.
  // - `path_` is the URL path
  // - `parameters_` is a hashmap of the HTTP Query parameters
  struct ParsedUrl {
    std::string path_;
    ad_utility::HashMap<std::string, std::string> parameters_;
  };

  // Representation of parsed HTTP request.
  // - `path_` is the URL path
  // - `parameters_` is a hashmap of the parameters. These can originate from
  // the HTTP Query parameters or the request body
  // - `query_` contains the Query
  struct ParsedRequest {
    std::string path_;
    ad_utility::HashMap<std::string, std::string> parameters_;
    std::optional<std::string> query_;
  };

  // Parse the URL path, URL query parameters of a HTTP Request target.
  static ParsedUrl parseRequestTarget(std::string_view target);

  // Converts the HTTP Query parameters `params` to a hashmap. Throws an error
  // if a key is included twice.
  static ad_utility::HashMap<std::string, std::string> paramsToMap(
      boost::urls::params_view params);
};
}  // namespace ad_utility

#endif  // QLEVER_URLPARSER_H
