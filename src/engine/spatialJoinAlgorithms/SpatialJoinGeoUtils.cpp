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

#include <thread>

#include "global/RuntimeParameters.h"
#include "global/ValueId.h"
#include "index/ExportIds.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/Exception.h"
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
std::optional<ad_utility::BoundingBox> getBoundingBoxFromIdTable(
    const IdTableView<0>* idTable,
    const SpatialJoinBoundingBoxColumns& boundingBoxes, size_t row) {
  if (!boundingBoxes.has_value()) {
    return std::nullopt;
  }
  auto idLowerLeft = idTable->at(row, boundingBoxes.value().first);
  auto idUpperRight = idTable->at(row, boundingBoxes.value().second);
  if (idLowerLeft.getDatatype() != Datatype::GeoPoint ||
      idUpperRight.getDatatype() != Datatype::GeoPoint) {
    return std::nullopt;
  }
  return ad_utility::BoundingBox{idLowerLeft.getGeoPoint(),
                                 idUpperRight.getGeoPoint()};
}

// ____________________________________________________________________________
size_t getNumThreads() {
  size_t maxHwConcurrency = std::thread::hardware_concurrency();
  size_t userPreference =
      getRuntimeParameter<&RuntimeParameters::spatialJoinMaxNumThreads_>();
  if (userPreference == 0 || maxHwConcurrency < userPreference) {
    return maxHwConcurrency;
  }
  return userPreference;
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

// ____________________________________________________________________________
sj::SweeperCfg libspatialjoinSweeperConfig(
    size_t threads, ad_utility::MemorySize totalAllowedMemory) {
  using enum SpatialJoinType::Enum;
  // `libspatialjoin` reports a match for one of these relations by invoking
  // `writeRelCb` (see below) with a `pred` argument equal to the
  // corresponding `sep...` string set below. These strings are otherwise
  // opaque to `libspatialjoin`, so any distinct single byte per relation
  // works; we simply (ab)use the (small) numeric value of the enum, which is
  // not meant to be human-readable.
  auto sep = [](SpatialJoinType type) {
    return std::string{static_cast<char>(type.value())};
  };
  AD_CORRECTNESS_CHECK(threads > 0);

  sj::SweeperCfg cfg;
  cfg.numThreads = threads;
  cfg.numCacheThreads = threads;
  // Cache memory per thread, in bytes
  cfg.geomCacheMaxSize = totalAllowedMemory.getBytes() / threads;
  cfg.geomCacheMaxNumElements = 10'000;
  cfg.sepIsect = sep(INTERSECTS);
  cfg.sepContains = sep(CONTAINS);
  cfg.sepCovers = sep(COVERS);
  cfg.sepTouches = sep(TOUCHES);
  cfg.sepEquals = sep(EQUALS);
  cfg.sepOverlaps = sep(OVERLAPS);
  cfg.sepCrosses = sep(CROSSES);
  cfg.useBoxIds = true;
  cfg.useArea = true;
  cfg.useOBB = false;
  cfg.useDiagBox = true;
  cfg.useFastSweepSkip = true;
  cfg.noGeometryChecks = false;
  cfg.euclideanDist = false;
  cfg.haversineApprox = false;
  cfg.computeDE9IM = false;
  cfg.de9imFilter = ::util::geo::FANY;
  // Never let `libspatialjoin` fall back to a self-join when it considers one
  // side to be empty; QLever's callbacks rely on the first geometry of each
  // result pair coming from the left side and the second one from the right
  // side (see #3068).
  cfg.forceTwoSided = true;
  cfg.writeRelCb = {};
  cfg.logCb = {};
  cfg.statsCb = {};
  cfg.sweepProgressCb = {};
  cfg.sweepCancellationCb = {};
  return cfg;
}

}  // namespace ad_utility::detail::spatialjoin
