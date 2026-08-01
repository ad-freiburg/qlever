#include "engine/RequestParameters.h"

#include <optional>
#include <stdexcept>
#include <string>

#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

// __________________________________________________________________________
std::optional<double>
ad_utility::request_parameters::parsePinGeoIndexSimplification(
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
std::optional<ad_utility::MediaType>
ad_utility::request_parameters::determineMediaTypesFromParam(
    const ad_utility::url_parser::ParamValueMap& params) {
  using namespace ad_utility::url_parser;
  using ad_utility::MediaType;

  std::optional<MediaType> mediaType = std::nullopt;

  if (checkParameter(params, "action", "csv_export")) {
    mediaType = MediaType::csv;
  } else if (checkParameter(params, "action", "tsv_export")) {
    mediaType = MediaType::tsv;
  } else if (checkParameter(params, "action", "qlever_json_export")) {
    mediaType = MediaType::qleverJson;
  } else if (checkParameter(params, "action", "sparql_json_export")) {
    mediaType = MediaType::sparqlJson;
  } else if (checkParameter(params, "action", "turtle_export")) {
    mediaType = MediaType::turtle;
  } else if (checkParameter(params, "action", "binary_export")) {
    mediaType = MediaType::octetStream;
  }

  return mediaType;
}

// ____________________________________________________________________________
std::pair<bool, bool> ad_utility::request_parameters::determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params) {
  const bool pinSubresults =
      ad_utility::url_parser::checkParameter(params, "pin-subresults", "true")
          .has_value();
  const bool pinResult =
      ad_utility::url_parser::checkParameter(params, "pin-result", "true")
          .has_value();
  return {pinSubresults, pinResult};
}
