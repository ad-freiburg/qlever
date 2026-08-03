// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tomas Damek <tomas.damek@email.uni-freiburg.de>

#include "engine/RequestParameters.h"

#include <array>
#include <boost/url/param.hpp>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

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

// __________________________________________________________________________
std::optional<ad_utility::MediaType> determineMediaTypesFromParam(
    const ParamValueMap& params) {
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
      return mediaType;
      break;
    }
  }

  return std::nullopt;
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
