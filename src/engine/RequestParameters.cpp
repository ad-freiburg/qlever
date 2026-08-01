#include "engine/RequestParameters.h"

#include <optional>
#include <stdexcept>
#include <string>

#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

namespace ad_utility::request_parameters {

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
std::pair<bool, bool> determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params) {
  const bool pinSubresults =
      ad_utility::url_parser::checkParameter(params, "pin-subresults", "true")
          .has_value();
  const bool pinResult =
      ad_utility::url_parser::checkParameter(params, "pin-result", "true")
          .has_value();
  return {pinSubresults, pinResult};
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

}  // namespace ad_utility::request_parameters
