#ifndef QLEVER_SRC_ENGINE_REQUESTPARAMETERS_H
#define QLEVER_SRC_ENGINE_REQUESTPARAMETERS_H

#include <optional>
#include <string>

#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

/**
 * /brief Helpers that interpret already-parsed URL parameters into typed,
 * validated request settings for QLever's SPARQL endpoint.
 */
namespace ad_utility::request_parameters {
// Parse the `pin-geo-index-simplification` parameter (the maximum error in
// meters for the simplification of geometries before indexing) from its
// string representation. Return `std::nullopt` if `simplificationStr` is
// `std::nullopt`. Throw if `simplificationStr` is set, but is not a valid
// floating-point number.
std::optional<double> parsePinGeoIndexSimplification(
    const std::optional<std::string>& simplificationStr);

// Determines the media type to be used for the result of a query. It resolves
// the media type depending on the URL parameter "action= ..." (for TSV and CSV
// export, for QLever-historical reasons).
std::optional<ad_utility::MediaType> determineMediaTypesFromParam(
    const ad_utility::url_parser::ParamValueMap& params);

// Determine whether the subtrees and the result should be pinned.
std::pair<bool, bool> determineResultPinning(
    const ad_utility::url_parser::ParamValueMap& params);
}  // namespace ad_utility::request_parameters

#endif  // QLEVER_SRC_ENGINE_REQUESTPARAMETERS_H
