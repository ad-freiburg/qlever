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

#include "parser/sparqlParser/DatasetClause.h"
#include "util/HashMap.h"

/**
 * /brief Some helpers to parse request URLs in QLever.
 */
namespace ad_utility::url_parser {

// A map that stores the values for parameters. Parameters can be specified
// multiple times with different values.
using ParamValueMap = ad_utility::HashMap<string, std::vector<string>>;

// Extracts a parameter that must be present exactly once. If the parameter is
// not present std::nullopt is returned. If the parameter is present multiple
// times an exception is thrown.
std::optional<std::string> getParameterCheckAtMostOnce(const ParamValueMap& map,
                                                       string_view key);

// A parsed URL.
// - `path_` is the URL path
// - `parameters_` is a map of the HTTP Query parameters
struct ParsedUrl {
  std::string path_;
  ParamValueMap parameters_;
};

// The different SPARQL operations that a `ParsedRequest` can represent.
namespace sparqlOperation {
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
}  // namespace sparqlOperation

// Representation of parsed HTTP request.
// - `path_` is the URL path
// - `parameters_` is a hashmap of the parameters
// - `operation_` the operation that should be performed
struct ParsedRequest {
  std::string path_;
  ParamValueMap parameters_;
  std::variant<sparqlOperation::Query, sparqlOperation::Update,
               sparqlOperation::None>
      operation_;
};

// Parse the URL path and the URL query parameters of an HTTP Request target.
ParsedUrl parseRequestTarget(std::string_view target);

// Convert the HTTP Query parameters `params` to a ParamValueMap (a map from
// string to vectors of strings).
ParamValueMap paramsToMap(boost::urls::params_view params);

// Parse default and named graphs URIs from the parameters.
std::vector<DatasetClause> parseDatasetClauses(const ParamValueMap& params);
}  // namespace ad_utility::url_parser

#endif  // QLEVER_URLPARSER_H
