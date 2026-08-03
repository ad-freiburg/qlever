// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tomas Damek <tomas.damek@email.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_REQUESTPARAMETERS_H
#define QLEVER_SRC_ENGINE_REQUESTPARAMETERS_H

#include <optional>
#include <string>

#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

// Helpers to parse QLever's HTTP api, in particular http query parameters in
// the ?key=value form.
namespace qlever::http_api_helpers {
// Parse the `pin-geo-index-simplification` parameter (the maximum error in
// meters for the simplification of geometries before indexing) from its
// string representation, e.g. `?pin-geo-index-simplification=0.01` requests
// a maximum simplification error of 0.01 meters. Return `std::nullopt` if
// `simplificationStr` is `std::nullopt`. Throw if `simplificationStr` is
// set, but is not a valid floating-point number.
std::optional<double> parsePinGeoIndexSimplification(
    const std::optional<std::string>& simplificationStr);

// Determine the media type to be used for the result of a query from the
// (historical) `?action=[some-export-specification]` HTTP query parameter, e.g.
// `?action=csv_export` requests CSV. Return `nullopt` if no such query
// parameter is set.
std::optional<ad_utility::MediaType> determineMediaTypesFromParam(
    const ad_utility::url_parser::ParamValueMap& params);

// Help struct defining the pinning of subtrees and the final result of a query
// into the cache.
struct ResultPinning {
  bool pinSubtrees = false;
  bool pinResult = false;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ResultPinning, pinSubresults,
                                              pinResult)
};

// Determine `ResultPinning` from the `pin-subresults` and `pin-result` URL
// parameters, e.g. `?pin-subresults=true&pin-result=true` pins both. Either
// parameter defaults to `false` if not present.
ResultPinning determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params);

// Parse the `send` parameter (historical name): the maximum number of
// bindings to export, e.g. `?send=100` limits the response to (at most)
// 100 bindings. Returns `std::nullopt` if the parameter is not set.
std::optional<uint64_t> parseSendLimit(
    const ad_utility::url_parser::ParamValueMap& params);
}  // namespace qlever::http_api_helpers

#endif  // QLEVER_SRC_ENGINE_REQUESTPARAMETERS_H
