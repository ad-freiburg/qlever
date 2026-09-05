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
#include <vector>

#include "backports/three_way_comparison.h"
#include "engine/QueryExecutionContext.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

// Helpers to parse QLever's HTTP API, in particular HTTP query parameters in
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
// `action=..._export` parameter has precedence over the `Accept:...` header
// field.
std::vector<ad_utility::MediaType> determineMediaTypes(
    const ad_utility::url_parser::ParamValueMap& params,
    std::string_view acceptHeader);

// Helper struct describing all pinning requested for an operation's result:
// of subresults, of the whole result, and (optionally) of the whole result
// under an explicit name, with an optional geo index on one of its columns.
struct ResultPinning {
  bool pinSubtrees_ = false;
  bool pinResult_ = false;
  std::optional<QueryExecutionContext::PinResultWithName> pinResultWithName_;

  // Describe all requested pinning for the request log line, e.g. `" [pin
  // result] [pin subresults] [pin result with name \"myPin\" with geo index
  // on ?geom, simplification=5m]"`. Empty parts are omitted.
  std::string describeForLog() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ResultPinning, pinSubtrees_,
                                              pinResult_, pinResultWithName_)
};

// Determine `ResultPinning` from the URL parameters `pin-subresults`,
// `pin-result`, `pin-result-with-name`, `pin-geo-index-on-var`, and
// `pin-geo-index-simplification`, e.g. `?pin-result-with-name=myPin` pins the
// result under the name "myPin". `pin-subresults` and `pin-result` default to
// `false`. Throw if `pin-geo-index-on-var` or `pin-geo-index-simplification`
// is given without `pin-result-with-name`, or if
// `pin-geo-index-simplification` is given without `pin-geo-index-on-var`.
ResultPinning determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params);

// Determine the export limit for a response of the given `mediaType` from
// the `send` parameter (see `parseSendLimit`). Always validates `send`
// (throws on an invalid or duplicated value) regardless of `mediaType`, but
// only returns a limit for media types where `considerSendParameter` holds;
// returns `std::nullopt` otherwise.
std::optional<uint64_t> determineSendLimit(
    const ad_utility::url_parser::ParamValueMap& params,
    ad_utility::MediaType mediaType);

// Extract the required `view-name` parameter for a materialized-view
// command; `actionName` (e.g. "Writing", "Loading", "Deleting") names the
// action being performed, for the error message if the parameter is missing.
std::string getViewNameParameter(
    const ad_utility::url_parser::ParamValueMap& params,
    std::string_view actionName);

// Implementation details for the function `determineSendLimit`.
namespace detail {
// Parse the `send` parameter (historical name): the maximum number of
// bindings to export, e.g. `?send=100` limits the response to (at most)
// 100 bindings. Return `std::nullopt` if the parameter is not set. Throw
// if the parameter is set, but is not a valid non-negative integer.
std::optional<uint64_t> parseSendLimit(
    const ad_utility::url_parser::ParamValueMap& params);

// Determine whether the `send` parameter (see `parseSendLimit`) should be
// applied to a response of the given `mediaType`. This always holds for
// `qleverJson`; for `sparqlJson` it only holds if the runtime parameter
// `sparql-results-json-with-time` is enabled (the default).
bool considerSendParameter(ad_utility::MediaType mediaType);

}  // namespace detail
}  // namespace qlever::http_api_helpers

#endif  // QLEVER_SRC_ENGINE_HTTPAPIHELPERS_H
