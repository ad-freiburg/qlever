// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/S2PointPolylineAlgorithm.h"

#include <s2/s2closest_edge_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/util/units/length-units.h>

#include "engine/NamedResultCache.h"
#include "util/GeoConverters.h"
#include "util/HashMap.h"
#include "util/Timer.h"

using namespace geometryConverters;

namespace {
using S2Queries = std::vector<std::unique_ptr<S2ClosestEdgeQuery>>;

// Return one query object for each segment of the `geoIndex` (in the same
// order), which finds all edges within `maxDistInMeters`. The query objects can
// be reused for many targets.
S2Queries makeQueriesForAllSegments(const SpatialJoinCachedIndex& geoIndex,
                                    double maxDistInMeters) {
  S2Queries queries;
  queries.reserve(geoIndex.numSegments());
  for (const auto& segment : geoIndex.segments()) {
    auto& query = *queries.emplace_back(
        std::make_unique<S2ClosestEdgeQuery>(segment.get()));
    query.mutable_options()->set_inclusive_max_distance(S2Earth::ToAngle(
        util::units::Meters(static_cast<float>(maxDistInMeters))));
  }
  return queries;
}

// Return all rows of the `geoIndex` whose shape is within the maximal distance
// of the `queries` (see `makeQueriesForAllSegments`) from the `target`,
// together with that distance in km. If several edges of the shape of a row are
// within the maximal distance, the smallest distance is returned. Shapes that
// no row refers to anymore are skipped.
ad_utility::HashMap<size_t, double> findRowsWithinDistance(
    const SpatialJoinCachedIndex& geoIndex, const S2Queries& queries,
    S2ClosestEdgeQuery::PointTarget& target) {
  ad_utility::HashMap<size_t, double> result;
  auto addNeighbor = [&geoIndex, &result](
                         size_t segment,
                         const S2ClosestEdgeQuery::Result& neighbor) {
    auto row = geoIndex.getRow(segment, neighbor.shape_id());
    if (!row.has_value()) {
      return;
    }
    auto dist = S2Earth::ToKm(neighbor.distance());
    auto [it, isNew] = result.try_emplace(row.value(), dist);
    if (!isNew) {
      it->second = std::min(it->second, dist);
    }
  };
  for (size_t segment = 0; segment < queries.size(); ++segment) {
    // We only receive edges that already satisfy the given criteria.
    for (const auto& neighbor : queries[segment]->FindClosestEdges(&target)) {
      addNeighbor(segment, neighbor);
    }
  }
  return result;
}
}  // namespace

// ____________________________________________________________________________
Result S2PointPolylineAlgorithm::run() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, leftSelectedCols, rightSelectedCols, numColumns] =
      params_;
  IdTable result{numColumns, qec_->getAllocator()};

  AD_CORRECTNESS_CHECK(config_.rightCacheName_.has_value());
  // The `cacheEntry` is a `shared_ptr` that keeps the `s2index` below alive.
  // Binding the `s2index` by reference avoids copying the geo index.
  auto cacheEntry =
      qec_->namedResultCache().get(config_.rightCacheName_.value());
  const auto& s2index = cacheEntry->cachedGeoIndex_;
  AD_CORRECTNESS_CHECK(s2index.has_value());
  AD_CORRECTNESS_CHECK(!config_.getMaxResults().has_value() &&
                       maxDist_.has_value());

  // Construct one query object per segment of the index with the given
  // constraints. The query objects are reused for all points.
  const auto& geoIndex = s2index.value();
  auto s2queries = makeQueriesForAllSegments(geoIndex, maxDist_.value());

  ad_utility::Timer timerAll{ad_utility::Timer::Started};
  ad_utility::Timer timerS2{ad_utility::Timer::Stopped};
  ad_utility::Timer timerWrite{ad_utility::Timer::Stopped};

  // Use the index to lookup the points of the other table
  for (size_t rowLeft = 0; rowLeft < idTableLeft->size(); rowLeft++) {
    auto p = getPoint(idTableLeft, rowLeft, leftJoinCol);
    if (!p.has_value()) {
      continue;
    }
    auto s2target = S2ClosestEdgeQuery::PointTarget{toS2Point(p.value())};

    timerS2.cont();
    auto deduplicatedSet =
        findRowsWithinDistance(geoIndex, s2queries, s2target);
    timerS2.stop();
    timerWrite.cont();
    for (auto [indexRow, dist] : deduplicatedSet) {
      auto rowRight = indexRow;
      addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft, rowRight,
                          Id::makeFromDouble(dist));
    }
    timerWrite.stop();
  }
  spatialJoin_.value()->runtimeInfo().addDetail("time for s2 queries",
                                                timerS2.msecs().count());
  spatialJoin_.value()->runtimeInfo().addDetail("time for result writing",
                                                timerWrite.msecs().count());
  spatialJoin_.value()->runtimeInfo().addDetail("time total",
                                                timerAll.msecs().count());

  return Result{std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight)};
}
