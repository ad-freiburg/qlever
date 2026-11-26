// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "rdfTypes/GeoSparqlHelpers.h"

#include <absl/strings/charconv.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>

#include <cmath>
#include <ctre-unicode.hpp>
#include <string_view>

#include "backports/algorithm.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/Exception.h"

namespace {
constexpr ctll::fixed_string de9imFilterRegex = "[0-2TtFf*]{9}";
}  // namespace

// ____________________________________________________________________________
std::optional<De9imFilterString> parseDe9imFilterString(
    std::string_view filter) {
  if (!ctre::match<de9imFilterRegex>(filter)) {
    return std::nullopt;
  }
  // The regex above already enforces that `filter` has exactly 9 characters.
  AD_CORRECTNESS_CHECK(filter.size() == De9imFilterString{}.size());
  De9imFilterString result{};
  ql::ranges::copy(filter, result.begin());
  return result;
}

// ____________________________________________________________________________
bool de9imFilterCanMatchDisjoint(const De9imFilterString& filter) {
  auto admitsF = [](char c) { return c == '*' || c == 'F' || c == 'f'; };
  return admitsF(filter[0]) && admitsF(filter[1]) && admitsF(filter[3]) &&
         admitsF(filter[4]);
}

namespace ad_utility {

namespace detail {

// The latitude in degrees at which `pb_util` clamps a coordinate when
// projecting it to Web Mercator.
static constexpr double webMercatorMaxLatitude = 85.05112878;

static constexpr auto wktPointRegex = ctll::fixed_string(
    "^\\s*[Pp][Oo][Ii][Nn][Tt]\\s*\\(\\s*"
    "(-?[0-9]+|-?[0-9]+\\.[0-9]+)"
    "\\s+"
    "(-?[0-9]+|-?[0-9]+\\.[0-9]+)"
    "\\s*\\)\\s*$");

// Parse a single WKT point and returns a pair of longitude and latitude. If
// the given string does not parse as a WKT point, a pair of `invalidCoordinate`
// is returned.
std::pair<double, double> parseWktPoint(const std::string_view point) {
  double lng = invalidCoordinate, lat = invalidCoordinate;
  if (auto match = ctre::search<wktPointRegex>(point)) {
    std::string_view lng_sv = match.get<1>();
    std::string_view lat_sv = match.get<2>();
    absl::from_chars(lng_sv.data(), lng_sv.data() + lng_sv.size(), lng);
    absl::from_chars(lat_sv.data(), lat_sv.data() + lat_sv.size(), lat);
    // This should never happen: if the regex matches, then each of the two
    // coordinate strings should also parse to a double.
    AD_CORRECTNESS_CHECK(!std::isnan(lng));
    AD_CORRECTNESS_CHECK(!std::isnan(lat));
  }
  return std::pair(lng, lat);
}

// Compute distance (in km) between two WKT points.
double wktDistImpl(GeoPoint point1, GeoPoint point2) {
  auto p1 = S2Point{S2LatLng::FromDegrees(point1.getLat(), point1.getLng())};
  auto p2 = S2Point{S2LatLng::FromDegrees(point2.getLat(), point2.getLng())};
  return S2Earth::ToKm(S1Angle(p1, p2));
}

// _____________________________________________________________________________
std::optional<std::string> geometryNAsWkt(GeoPointOrWkt wkt, int64_t n) {
  auto parseResult = parseGeoPointOrWkt(wkt);
  return utilGeomToWkt(getGeometryN(parseResult.parsedWkt_, n),
                       parseResult.sourceCrs_);
}

// _____________________________________________________________________________
std::optional<std::string> transformAsWkt(
    const GeoPointOrWkt& wkt, const ::util::geo::CRSType& targetCrs) {
  if (targetCrs == ::util::geo::CRSType::UNSUPPORTED) {
    return std::nullopt;
  }
  // Return the input unchanged if it already is in the target CRS (deliberately
  // without validating the WKT, to save the parsing).
  if (sourceCrsFromGeoPointOrWkt(wkt) == targetCrs) {
    return wktFromGeoPointOrWkt(wkt);
  }
  // Parsing projects the coordinates to `targetCrs`.
  auto parseResult = parseGeoPointOrWkt(wkt, targetCrs);
  // Return `std::nullopt` if the geometry reaches beyond the latitude range of
  // Web Mercator (`pb_util` clamps such a latitude to the limit of the range,
  // which would silently move the geometry).
  if (targetCrs == ::util::geo::CRSType::WEB_MERCATOR &&
      parseResult.parsedWkt_.has_value()) {
    static const double maxY =
        latLngToWebMerc<CoordType>(webMercatorMaxLatitude, 0).getY();
    auto boundingBox =
        std::visit([](const auto& val) { return getBoundingBox(val); },
                   parseResult.parsedWkt_.value());
    if (boundingBox.getLowerLeft().getY() <= -maxY ||
        boundingBox.getUpperRight().getY() >= maxY) {
      return std::nullopt;
    }
  }
  // TODO<yarox-1> This can be shortened to
  // `utilGeomToWkt(parseResult.parsedWkt_, targetCrs)` once #3787 is merged.
  auto wktResult = utilGeomToWkt(parseResult.parsedWkt_);
  if (!wktResult.has_value()) {
    return std::nullopt;
  }
  // `getCrsIri` returns an empty string for the default CRS.
  return absl::StrCat(::util::geo::getCrsIri(targetCrs), wktResult.value());
}

// _____________________________________________________________________________
std::optional<std::string> simplifyWkt(GeoPointOrWkt wkt, double tolerance) {
  auto parseResult = parseGeoPointOrWkt(wkt);
  return utilGeomToWkt(simplifyGeometry(parseResult.parsedWkt_, tolerance));
}

// _____________________________________________________________________________
std::optional<std::string> wktToGeoJson(const GeoPointOrWkt& wkt) {
  // A `GeoPoint` is stored with limited precision. Round its coordinates to
  // the six decimals of its WKT export, so that the decoding artifacts (a
  // stored `POINT(1 2)` decodes to `1.0000001089647412 1.9999999664723873`)
  // do not end up in the GeoJSON.
  if (const auto* point = std::get_if<GeoPoint>(&wkt)) {
    auto round = [](double coordinate) {
      return std::round(coordinate * 1e6) / 1e6;
    };
    return utilGeomToGeoJson(std::optional<ParsedWkt>{
        Point<CoordType>{round(point->getLng()), round(point->getLat())}});
  }
  return utilGeomToGeoJson(parseGeoPointOrWkt(wkt).parsedWkt_);
}

// _____________________________________________________________________________
std::optional<double> wktDistLibSpatialJoinImpl(const GeoPointOrWkt& a,
                                                const GeoPointOrWkt& b) {
  return computeMetricDistance(a, b);
}

}  // namespace detail

}  // namespace ad_utility
