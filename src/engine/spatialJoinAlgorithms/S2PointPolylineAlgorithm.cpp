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

// ____________________________________________________________________________
Result S2PointPolylineAlgorithm::run() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, leftSelectedCols, rightSelectedCols, numColumns] =
      params_;
  IdTable result{numColumns, qec_->getAllocator()};

  AD_CORRECTNESS_CHECK(config_.rightCacheName_.has_value());
  auto s2index = qec_->namedResultCache()
                     .get(config_.rightCacheName_.value())
                     ->cachedGeoIndex_;
  AD_CORRECTNESS_CHECK(s2index.has_value());
  AD_CORRECTNESS_CHECK(!config_.getMaxResults().has_value() &&
                       maxDist_.has_value());

  // Construct one query object per segment of the index with the given
  // constraints. The query objects are reused for all points.
  const auto& geoIndex = s2index.value();
  std::vector<std::unique_ptr<S2ClosestEdgeQuery>> s2queries;
  s2queries.reserve(geoIndex.segments().size());
  for (const auto& segment : geoIndex.segments()) {
    auto& query = *s2queries.emplace_back(
        std::make_unique<S2ClosestEdgeQuery>(segment.get()));
    query.mutable_options()->set_inclusive_max_distance(S2Earth::ToAngle(
        util::units::Meters(static_cast<float>(maxDist_.value()))));
  }

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

    ad_utility::HashMap<size_t, double> deduplicatedSet{};
    timerS2.cont();
    for (size_t segment = 0; segment < s2queries.size(); ++segment) {
      auto res = s2queries[segment]->FindClosestEdges(&s2target);
      for (const auto& neighbor : res) {
        // In this loop we only receive points that already satisfy the given
        // criteria. Shapes that no row refers to anymore are skipped.
        auto indexRow = geoIndex.getRow(segment, neighbor.shape_id());
        if (!indexRow.has_value()) {
          continue;
        }
        auto dist = S2Earth::ToKm(neighbor.distance());
        deduplicatedSet[indexRow.value()] = dist;
      }
    }
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
