// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_HTTPAPIHELPERS_H
#define QLEVER_SRC_ENGINE_HTTPAPIHELPERS_H

#include <cstdint>
#include <optional>
#include <string>

#include "backports/three_way_comparison.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

// Helpers to parse QLever's HTTP api, in particular HTTP query parameters in
// the `?key=value` form.
namespace qlever::http_api_helpers {
// Parse the `pin-geo-index-simplification` parameter (the maximum error in
// meters for the simplification of geometries before indexing) from its
// string representation, e.g. `?pin-geo-index-simplification=0.01` requests
// a maximum simplification error of 0.01 meters. Return `std::nullopt` if
// `simplificationStr` is `std::nullopt`. Throw if `simplificationStr` is
// set, but is not a valid floating-point number.
std::optional<double> parsePinGeoIndexSimplification(
    const std::optional<std::string>& simplificationStr);

// Determine media type candidates to be used for the result of query. Media
// types are determined (in this order) by the current action ((historical)
// `?action=[some-export-specification]` HTTP query parameter, e.g.
// `?action=csv_export` requests CSV) and by the "Accept" header of the request.
// The latter option can produce multiple candidates. The explicit
// `action=..._export` parameter have precedence over the `Accept:...` header
// field.
std::vector<ad_utility::MediaType> determineMediaTypes(
    const ad_utility::url_parser::ParamValueMap& params,
    std::string_view acceptHeader);

// Helper struct defining the pinning of subtrees and the final result of a
// query into the cache.
struct ResultPinning {
  bool pinSubtrees_ = false;
  bool pinResult_ = false;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ResultPinning, pinSubtrees,
                                              pinResult)
};

// Determine `ResultPinning` from the `pin-subresults` and `pin-result` URL
// parameters, e.g. `?pin-subresults=true&pin-result=true` pins both. Either
// parameter defaults to `false` if not present.
ResultPinning determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params);

// Parse the `send` parameter (historical name): the maximum number of
// bindings to export, e.g. `?send=100` limits the response to (at most)
// 100 bindings. Return `std::nullopt` if the parameter is not set. Throw
// if the parameter is set, but is not a valid non-negative integer.
std::optional<uint64_t> parseSendLimit(
    const ad_utility::url_parser::ParamValueMap& params);
}  // namespace qlever::http_api_helpers

#endif  // QLEVER_SRC_ENGINE_HTTPAPIHELPERS_H
