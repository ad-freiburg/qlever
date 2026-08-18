// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Patrick Brosi <brosi@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/SpatialJoinGeoUtils.h"

#include <s2/s2polyline.h>

#include "index/ExportIds.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GeoConverters.h"

using namespace geometryConverters;

namespace ad_utility::detail::spatialjoin {

// ____________________________________________________________________________
bool prefilterGeoByBoundingBox(
    const std::optional<::util::geo::DBox>& prefilterLatLngBox,
    const Index& index, VocabIndex vocabIndex,
    const std::optional<ad_utility::BoundingBox>& precomputedBoundingBox) {
  if (prefilterLatLngBox.has_value()) {
    auto hasNoIntersection =
        [&prefilterLatLngBox](const ad_utility::BoundingBox& geomBoundingBox) {
          return !::util::geo::intersects(
              prefilterLatLngBox.value(),
              ad_utility::detail::boundingBoxToUtilBox(geomBoundingBox));
        };

    // Use the `precomputedBoundingBox` for filtering if available.
    if (precomputedBoundingBox.has_value()) {
      return hasNoIntersection(precomputedBoundingBox.value());
    }

    // Otherwise, use the `GeoVocabulary` for filtering.
    auto geoInfo = index.getVocab().getGeoInfo(vocabIndex);
    if (geoInfo.has_value()) {
      // We have a bounding box: Check intersection with prefilter box.
      return hasNoIntersection(geoInfo.value().getBoundingBox());
    } else {
      // Since we know that this function is only called if we have a
      // `GeoVocabulary`, we know that a geometry without precomputed bounding
      // box must be invalid and can thus be skipped.
      return true;
    }
  }
  // If we don't have the required information, we cannot discard the geometry.
  return false;
}

// ____________________________________________________________________________
std::optional<GeoPoint> getPoint(const IdTableView<0>* restable, size_t row,
                                 ColumnIndex col) {
  auto id = restable->at(row, col);
  return id.getDatatype() == Datatype::GeoPoint
             ? std::optional{id.getGeoPoint()}
             : std::nullopt;
}

// ____________________________________________________________________________
std::optional<S2Polyline> getPolyline(const IdTableView<0>& restable,
                                      size_t row, ColumnIndex col,
                                      const Index& index) {
  using namespace ::util::geo;
  auto id = restable.at(row, col);
  auto str = ql::exportIds::idToStringAndType(index, id, {});
  if (!str.has_value()) {
    return std::nullopt;
  }
  // The `lineFromWKT` function skips the part of the string before the first
  // opening bracket. The geometry type needs to be checked separately.
  if (getWKTType(str.value().first) != WKTType::LINESTRING) {
    return std::nullopt;
  }
  auto line = lineFromWKT<double>(str.value().first);
  return line.empty() ? std::nullopt : std::optional{toS2Polyline(line)};
}

}  // namespace ad_utility::detail::spatialjoin
