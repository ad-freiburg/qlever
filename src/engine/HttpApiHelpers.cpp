// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/HttpApiHelpers.h"

#include <array>
#include <optional>
#include <stdexcept>
#include <string>

#include "engine/HttpError.h"
#include "util/http/HttpServer.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

namespace qlever::http_api_helpers {
using namespace ad_utility::url_parser;

// __________________________________________________________________________
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

// ____________________________________________________________________________
std::vector<ad_utility::MediaType> determineMediaTypes(
    const ad_utility::url_parser::ParamValueMap& params,
    std::string_view acceptHeader) {
  using namespace ad_utility::url_parser;
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

// ____________________________________________________________________________
ResultPinning determineResultPinning(const ParamValueMap& params) {
  const bool pinSubresults =
      checkParameter(params, "pin-subresults", "true").has_value();
  const bool pinResult =
      checkParameter(params, "pin-result", "true").has_value();
  return ResultPinning{pinSubresults, pinResult};
}

// ____________________________________________________________________________
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

}  // namespace qlever::http_api_helpers
