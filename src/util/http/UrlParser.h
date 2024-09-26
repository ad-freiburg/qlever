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

/**
 * /brief Some helpers to parse request URLs in QLever.
 */
namespace ad_utility::url_parser {
// TODO: There can be multiple values for a HTTP query parameter. Some SPARQL
// features require setting a parameter multiple times. Change the interface
// s.t. this is supported.

// A parsed URL.
// - `path_` is the URL path
// - `parameters_` is a hashmap of the HTTP Query parameters
struct ParsedUrl {
  std::string path_;
  ad_utility::HashMap<std::string, std::string> parameters_;
};

// Representation of parsed HTTP request.
// - `path_` is the URL path
// - `parameters_` is a hashmap of the parameters
// - `query_` contains the Query
struct ParsedRequest {
  std::string path_;
  ad_utility::HashMap<std::string, std::string> parameters_;
  std::optional<std::string> query_;
};

// Parse the URL path and the URL query parameters of a HTTP Request target.
ParsedUrl parseRequestTarget(std::string_view target);

// Convert the HTTP Query parameters `params` to a hashmap. Throw an error
// if a key is included twice.
ad_utility::HashMap<std::string, std::string> paramsToMap(
    boost::urls::params_view params);
}  // namespace ad_utility::url_parser

#endif  // QLEVER_URLPARSER_H
