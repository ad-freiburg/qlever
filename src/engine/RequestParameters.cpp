#include "engine/RequestParameters.h"

#include <optional>
#include <stdexcept>
#include <string>

#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

namespace qlever::http_api_helpers {

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
    const ad_utility::url_parser::ParamValueMap& params) {
  using namespace ad_utility::url_parser;
  using enum ad_utility::MediaType;

  std::optional<MediaType> mediaType = std::nullopt;

  if (checkParameter(params, "action", "csv_export")) {
    mediaType = csv;
  } else if (checkParameter(params, "action", "tsv_export")) {
    mediaType = tsv;
  } else if (checkParameter(params, "action", "qlever_json_export")) {
    mediaType = qleverJson;
  } else if (checkParameter(params, "action", "sparql_json_export")) {
    mediaType = sparqlJson;
  } else if (checkParameter(params, "action", "turtle_export")) {
    mediaType = turtle;
  } else if (checkParameter(params, "action", "binary_export")) {
    mediaType = octetStream;
  }

  return mediaType;
}

// ____________________________________________________________________________
ResultPinning determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params) {
  const bool pinSubresults =
      ad_utility::url_parser::checkParameter(params, "pin-subresults", "true")
          .has_value();
  const bool pinResult =
      ad_utility::url_parser::checkParameter(params, "pin-result", "true")
          .has_value();
  return ResultPinning{pinSubresults, pinResult};
}

// ____________________________________________________________________________
std::optional<uint64_t> parseSendLimit(
    const ad_utility::url_parser::ParamValueMap& params) {
  auto sendParameter =
      ad_utility::url_parser::getParameterCheckAtMostOnce(params, "send");
  if (sendParameter.has_value()) {
    return std::stoul(sendParameter.value());
  } else {
    return std::nullopt;
  }
}

}  // namespace qlever::http_api_helpers
