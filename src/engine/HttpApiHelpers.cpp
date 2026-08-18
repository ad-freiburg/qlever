// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/HttpApiHelpers.h"

#include <absl/strings/str_cat.h>

#include <array>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "engine/HttpError.h"
#include "engine/QueryExecutionContext.h"
#include "global/RuntimeParameters.h"
#include "rdfTypes/Variable.h"
#include "util/Exception.h"
#include "util/http/HttpServer.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

namespace qlever::http_api_helpers {
using namespace ad_utility::url_parser;

// _____________________________________________________________________________
std::optional<double> parsePinGeoIndexSimplification(
    const std::optional<std::string>& simplificationStr) {
  if (!simplificationStr.has_value()) {
    return std::nullopt;
  }
  try {
    return std::stod(simplificationStr.value());
  } catch (...) {
    throw std::runtime_error(
        "Invalid value for `pin-geo-index-simplification`: must be a "
        "floating-point number of meters.");
  }
}

// _____________________________________________________________________________
std::vector<ad_utility::MediaType> determineMediaTypes(
    const ad_utility::url_parser::ParamValueMap& params,
    std::string_view acceptHeader) {
  using enum ad_utility::MediaType;

  static const std::array<std::pair<std::string, ad_utility::MediaType>, 6>
      actionValueToMediaType = {{{"csv_export", csv},
                                 {"tsv_export", tsv},
                                 {"qlever_json_export", qleverJson},
                                 {"sparql_json_export", sparqlJson},
                                 {"turtle_export", turtle},
                                 {"binary_export", octetStream}}};

  for (const auto& [actionValue, mediaType] : actionValueToMediaType) {
    if (checkParameter(params, "action", actionValue).has_value()) {
      return {mediaType};
    }
  }

  try {
    return ad_utility::getMediaTypesFromAcceptHeader(acceptHeader);
  } catch (const std::exception& e) {
    throw HttpError(http::status::not_acceptable, e.what());
  }
}

// _____________________________________________________________________________
ResultPinning determineResultPinning(const ParamValueMap& params) {
  const bool pinSubresults =
      checkParameter(params, "pin-subresults", "true").has_value();
  const bool pinResult =
      checkParameter(params, "pin-result", "true").has_value();

  std::optional<std::string> pinResultWithName =
      ad_utility::url_parser::checkParameter(params, "pin-result-with-name",
                                             {});
  std::optional<std::string> pinNamedGeoIndex =
      ad_utility::url_parser::checkParameter(params, "pin-geo-index-on-var",
                                             {});
  std::optional<double> geoIndexSimplificationInMeters =
      parsePinGeoIndexSimplification(ad_utility::url_parser::checkParameter(
          params, "pin-geo-index-simplification", {}));

  if (!pinResultWithName.has_value()) {
    if (pinNamedGeoIndex.has_value() ||
        geoIndexSimplificationInMeters.has_value()) {
      throw std::runtime_error(
          "`pin-geo-index-on-var` and `pin-geo-index-simplification` "
          "require `pin-result-with-name` to be set");
    }
    return ResultPinning{pinSubresults, pinResult, std::nullopt};
  }
  if (!pinNamedGeoIndex.has_value() &&
      geoIndexSimplificationInMeters.has_value()) {
    throw std::runtime_error(
        "`pin-geo-index-simplification` requires `pin-geo-index-on-var` to "
        "be set");
  }

  std::optional<Variable> geoIndexVar =
      pinNamedGeoIndex.has_value()
          ? std::optional{Variable{absl::StrCat("?", pinNamedGeoIndex.value())}}
          : std::nullopt;
  return ResultPinning{
      pinSubresults, pinResult,
      QueryExecutionContext::PinResultWithName{
          std::move(pinResultWithName).value(), std::move(geoIndexVar),
          geoIndexSimplificationInMeters}};
}

// _____________________________________________________________________________
std::string ResultPinning::describeForLog() const {
  std::string namePart;
  if (pinResultWithName_.has_value()) {
    const auto& pin = pinResultWithName_.value();
    // Describe the "with geo index on ?<var>" part (empty if `geoIndexVar_`
    // is not set).
    std::string geoIndexDescription;
    if (pin.geoIndexVar_.has_value()) {
      std::string simplification =
          pin.geoIndexSimplificationInMeters_.has_value()
              ? absl::StrCat(", simplification=",
                             pin.geoIndexSimplificationInMeters_.value(), "m")
              : "";
      geoIndexDescription = absl::StrCat(
          " with geo index on ", pin.geoIndexVar_->name(), simplification);
    }
    namePart = absl::StrCat(" [pin result with name \"", pin.name_, "\"",
                            geoIndexDescription, "]");
  }
  return absl::StrCat(pinResult_ ? " [pin result]" : "",
                      pinSubtrees_ ? " [pin subresults]" : "", namePart);
}

namespace detail {
// _____________________________________________________________________________
std::optional<uint64_t> parseSendLimit(const ParamValueMap& params) {
  auto sendParameter = getParameterCheckAtMostOnce(params, "send");
  if (!sendParameter.has_value()) {
    return std::nullopt;
  }
  try {
    // The parameter cannot be negative. `std::stoull` would otherwise accept
    // a leading '-' and convert it to an unsigned value.
    AD_CONTRACT_CHECK(!sendParameter->starts_with('-'));
    return std::stoull(sendParameter.value());
  } catch (...) {
    throw std::runtime_error(
        "Invalid value for `send`: must be a "
        "positive integer specifying the number of bindings to be exported.");
  }
}

// _____________________________________________________________________________
bool considerSendParameter(ad_utility::MediaType mediaType) {
  using enum ad_utility::MediaType;
  return mediaType == qleverJson ||
         (getRuntimeParameter<
              &RuntimeParameters::sparqlResultsJsonWithTime_>() &&
          mediaType == sparqlJson);
}
}  // namespace detail

// _____________________________________________________________________________
std::optional<uint64_t> determineSendLimit(const ParamValueMap& params,
                                           ad_utility::MediaType mediaType) {
  auto sendLimit = detail::parseSendLimit(params);
  return detail::considerSendParameter(mediaType) ? sendLimit : std::nullopt;
}

}  // namespace qlever::http_api_helpers
