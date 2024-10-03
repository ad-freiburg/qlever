// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_URLPARSER_H
#define QLEVER_URLPARSER_H

#include <boost/url.hpp>
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

// The different SPARQL operations that a `ParsedRequest` can represent.
namespace Operation {
// A SPARQL 1.1 Query
struct Query {
  std::string query_;

  bool operator==(const Query& rhs) const = default;
};

// A SPARQL 1.1 Update
struct Update {
  std::string update_;

  bool operator==(const Update& rhs) const = default;
};

// No operation. This can happen for QLever's custom operations (e.g.
// `cache-stats`). These requests have no operation but are still valid.
struct None {
  bool operator==(const None& rhs) const = default;
};
}  // namespace Operation

// Representation of parsed HTTP request.
// - `path_` is the URL path
// - `parameters_` is a hashmap of the parameters
// - `operation_` the operation that should be performed
struct ParsedRequest {
  std::string path_;
  ad_utility::HashMap<std::string, std::string> parameters_;
  std::variant<Operation::Query, Operation::Update, Operation::None> operation_;
};

// Parse the URL path and the URL query parameters of a HTTP Request target.
ParsedUrl parseRequestTarget(std::string_view target);

// Convert the HTTP Query parameters `params` to a hashmap. Throw an error
// if a key is included twice.
ad_utility::HashMap<std::string, std::string> paramsToMap(
    boost::urls::params_view params);
}  // namespace ad_utility::url_parser

#endif  // QLEVER_URLPARSER_H
