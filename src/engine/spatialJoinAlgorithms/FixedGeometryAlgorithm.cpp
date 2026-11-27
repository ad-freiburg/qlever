// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/FixedGeometryAlgorithm.h"

#include <algorithm>
#include <cmath>
#include <limits>

#include "global/RuntimeParameters.h"
#include "rdfTypes/Literal.h"
#include "util/ContainersWithAllocator.h"

using ::util::geo::DE9IMatrix;
using ::util::geo::DPoint;
using ::util::geo::FPoint;
using ::util::geo::I32Point;
using ::util::geo::I32XSortedPolygon;

// ____________________________________________________________________________
I32Point FixedGeometryAlgorithm::toWebMercator(const GeoPoint& point) {
  auto merc =
      ::util::geo::latLngToWebMerc(DPoint{point.getLng(), point.getLat()});
  return {static_cast<int32_t>(merc.getX() * PREC),
          static_cast<int32_t>(merc.getY() * PREC)};
}

// ____________________________________________________________________________
std::optional<FixedGeometryAlgorithm::FixedGeometry>
FixedGeometryAlgorithm::parseFixedGeometry(ValueId id, const Index& index) {
  using enum Datatype;
  if (id.getDatatype() == GeoPoint) {
    return toWebMercator(id.getGeoPoint());
  }

  // The WKT literal as a string, without the quotes and the datatype.
  std::string wkt;
  if (id.getDatatype() == VocabIndex) {
    std::string representation{index.indexToString(id.getVocabIndex())};
    if (!representation.starts_with('"')) {
      return std::nullopt;
    }
    auto literal =
        ad_utility::triple_component::Literal::fromStringRepresentation(
            std::move(representation));
    wkt = std::string{literal.getContent()};
  } else if (id.getDatatype() == LocalVocabIndex) {
    const auto& entry = *id.getLocalVocabIndex();
    if (!entry.isLiteral()) {
      return std::nullopt;
    }
    wkt = std::string{entry.getLiteral().getContent()};
  } else {
    return std::nullopt;
  }

  // Parse the literal and project it to the integer Web Mercator coordinates
  // of `libspatialjoin`, exactly as its own WKT parser does (see
  // `sj::WKTParserBase::projFunc`).
  auto projFunc = [](const DPoint& p,
                     ::util::geo::CRSType sourceCrs) -> I32Point {
    auto merc = ::util::geo::projectToWebMerc(p, sourceCrs);
    return {static_cast<int>(merc.getX() * PREC),
            static_cast<int>(merc.getY() * PREC)};
  };
  try {
    const char* c = wkt.c_str();
    auto crs = ::util::geo::getCRSType(c, &c);
    auto type = ::util::geo::getWKTType(c, &c);
    if (type == ::util::geo::WKTType::POINT) {
      return ::util::geo::pointFromWKTProj<int32_t>(c, &c, projFunc, crs, true);
    }
    if (type == ::util::geo::WKTType::POLYGON) {
      auto polygon =
          ::util::geo::polygonFromWKTProj<int32_t>(c, &c, projFunc, crs, true);
      I32XSortedPolygon sorted{polygon};
      if (sorted.empty()) {
        return std::nullopt;
      }
      auto boundingBox = sorted.boundingBox();
      auto boxIds = sj::boxids::packBoxIds(sj::boxids::getBoxIds(
          sorted, boundingBox, ::util::geo::outerArea(polygon)));
      return PreparedPolygon{std::move(sorted), boundingBox, std::move(boxIds)};
    }
  } catch (const std::exception&) {
    // Invalid WKT: not applicable, the `LibspatialjoinAlgorithm` handles it.
  }
  return std::nullopt;
}

// ____________________________________________________________________________
DE9IMatrix FixedGeometryAlgorithm::relate(const I32Point& point,
                                          const PreparedPolygon& polygon) {
  // Outside the bounding box: disjoint.
  if (!::util::geo::intersects(polygon.boundingBox_, point)) {
    return ::util::geo::MFF0FFF212;
  }
  // The box ids: if the grid cell of the point lies fully inside the polygon,
  // so does the point; if the polygon does not touch the cell, they are
  // disjoint. Only otherwise the polygon is looked at (see
  // `sj::Sweeper::DE9IMCheck` for a point and an area).
  auto [fullyContained, partlyContained] = sj::boxids::boxIdIsect(
      {{1, 0}, {sj::boxids::getBoxId(point), 0}}, polygon.boxIds_);
  if (fullyContained > 0) {
    return ::util::geo::M0FFFFF212;
  }
  if (fullyContained + partlyContained == 0) {
    return ::util::geo::MFF0FFF212;
  }
  return ::util::geo::DE9IM(point, polygon.polygon_);
}

// ____________________________________________________________________________
double FixedGeometryAlgorithm::meterDist(const I32Point& a, const I32Point& b,
                                         double maxDist) {
  // This is `sj::Sweeper::meterDist`, which is private there.
  auto fa = FPoint{static_cast<float>((a.getX() * 1.0) / (PREC * 1.0)),
                   static_cast<float>((a.getY() * 1.0) / (PREC * 1.0))};
  auto fb = FPoint{static_cast<float>((b.getX() * 1.0) / (PREC * 1.0)),
                   static_cast<float>((b.getY() * 1.0) / (PREC * 1.0))};
  double dX = fb.getX() - fa.getX();
  double dY = fb.getY() - fa.getY();
  // The distortion of Web Mercator at the latitude of the points.
  double distFactor = std::min(::util::geo::webMercDistFactor(fa),
                               ::util::geo::webMercDistFactor(fb));
  if (std::abs(dY) > maxDist / distFactor ||
      std::abs(dX) > maxDist / distFactor ||
      (dX * dX + dY * dY) > (maxDist / distFactor) * (maxDist / distFactor)) {
    return std::numeric_limits<double>::max();
  }
  return ::util::geo::webMercMeterDist(fa, fb);
}

// ____________________________________________________________________________
std::optional<Result> FixedGeometryAlgorithm::runIfApplicable() {
  if (!getRuntimeParameter<
          &RuntimeParameters::spatialJoinFixedGeometryAlgorithm_>()) {
    return std::nullopt;
  }
  auto joinType = config_.getJoinType();
  if (!joinType.has_value()) {
    return std::nullopt;
  }
  bool withinDist = joinType.value() == SpatialJoinType::WITHIN_DIST;
  if (!withinDist && joinType.value() != SpatialJoinType::INTERSECTS &&
      joinType.value() != SpatialJoinType::CONTAINS &&
      joinType.value() != SpatialJoinType::COVERS &&
      joinType.value() != SpatialJoinType::WITHIN) {
    return std::nullopt;
  }

  // The fixed side is the side with exactly one row.
  const auto* idTableLeft = params_.idTableLeft_;
  const auto* idTableRight = params_.idTableRight_;
  bool fixedIsLeft = false;
  if (idTableLeft->size() == 1) {
    fixedIsLeft = true;
  } else if (idTableRight->size() != 1) {
    return std::nullopt;
  }
  const auto* fixedTable = fixedIsLeft ? idTableLeft : idTableRight;
  auto fixedCol = fixedIsLeft ? params_.leftJoinCol_ : params_.rightJoinCol_;
  auto fixed =
      parseFixedGeometry(fixedTable->at(0, fixedCol), qec_->getIndex());
  if (!fixed.has_value()) {
    return std::nullopt;
  }
  if (withinDist && !std::holds_alternative<I32Point>(fixed.value())) {
    return std::nullopt;
  }

  // The other side must consist of points. Values that are no geometries
  // (undefined values, numbers, ...) never match and are skipped, but a WKT
  // literal needs the `LibspatialjoinAlgorithm`.
  const auto* pointTable = fixedIsLeft ? idTableRight : idTableLeft;
  auto pointCol = fixedIsLeft ? params_.rightJoinCol_ : params_.leftJoinCol_;
  for (ValueId id : pointTable->getColumn(pointCol)) {
    auto datatype = id.getDatatype();
    if (datatype == Datatype::VocabIndex ||
        datatype == Datatype::LocalVocabIndex) {
      return std::nullopt;
    }
  }
  return runWith(fixedIsLeft, fixed.value());
}

// ____________________________________________________________________________
Result FixedGeometryAlgorithm::run() {
  auto result = runIfApplicable();
  AD_CONTRACT_CHECK(result.has_value(),
                    "The fixed geometry algorithm does not apply to these "
                    "inputs, see `FixedGeometryAlgorithm::runIfApplicable`");
  return std::move(result.value());
}

// ____________________________________________________________________________
Result FixedGeometryAlgorithm::runWith(bool fixedIsLeft,
                                       const FixedGeometry& fixed) {
  const auto* idTableLeft = params_.idTableLeft_;
  const auto* idTableRight = params_.idTableRight_;
  const auto* pointTable = fixedIsLeft ? idTableRight : idTableLeft;
  auto pointCol = fixedIsLeft ? params_.rightJoinCol_ : params_.leftJoinCol_;
  auto joinType = config_.getJoinType().value();
  bool withinDist = joinType == SpatialJoinType::WITHIN_DIST;
  double maxDist = maxDist_.value_or(0.0);
  AD_CORRECTNESS_CHECK(!withinDist || maxDist_.has_value());
  // For a `WITHIN` join, the sides in `params_` are already swapped (see
  // `SpatialJoin::prepareJoin`), so it is a `CONTAINS` join on them, and the
  // result rows are swapped back.
  bool swapBack = joinType == SpatialJoinType::WITHIN;
  if (swapBack) {
    joinType = SpatialJoinType::CONTAINS;
  }

  // Does the pair (left, right) of the point and the fixed geometry satisfy
  // the join type? For `WITHIN_DIST`, also return the distance in meters.
  auto check = [&](const I32Point& point) -> std::pair<bool, double> {
    if (const auto* fixedPoint = std::get_if<I32Point>(&fixed)) {
      if (withinDist) {
        double distance = meterDist(point, *fixedPoint, maxDist);
        return {distance <= maxDist, distance};
      }
      // Two equal points intersect, contain and cover each other.
      return {point == *fixedPoint, 0.0};
    }
    const auto& polygon = std::get<PreparedPolygon>(fixed);
    auto relation = relate(point, polygon);
    // `relation` describes the point relative to the polygon: `coveredBy` if
    // the point lies inside or on the boundary, `within` if it lies inside.
    // A point never contains or covers a polygon.
    bool matches = false;
    if (joinType == SpatialJoinType::INTERSECTS) {
      matches = relation.coveredBy();
    } else if (joinType == SpatialJoinType::CONTAINS) {
      matches = fixedIsLeft && relation.within();
    } else {
      AD_CORRECTNESS_CHECK(joinType == SpatialJoinType::COVERS);
      matches = fixedIsLeft && relation.coveredBy();
    }
    return {matches, 0.0};
  };

  IdTable result{params_.numColumns_, qec_->getAllocator()};
  const auto& ids = pointTable->getColumn(pointCol);
  size_t numPointsChecked = 0;
  for (size_t row = 0; row < ids.size(); ++row) {
    if (row % 10'000 == 0) {
      throwIfCancelled();
    }
    ValueId id = ids[row];
    if (id.getDatatype() != Datatype::GeoPoint) {
      continue;
    }
    ++numPointsChecked;
    auto [matches, distance] = check(toWebMercator(id.getGeoPoint()));
    if (matches) {
      addResultTableEntry(&result, idTableLeft, idTableRight,
                          fixedIsLeft ? 0 : row, fixedIsLeft ? row : 0,
                          kilometersToDistanceId(distance / 1000), swapBack);
    }
  }

  if (spatialJoin_.has_value()) {
    auto& runtimeInfo = spatialJoin_.value()->runtimeInfo();
    runtimeInfo.addDetail("algorithm", "fixed-geometry");
    runtimeInfo.addDetail("num-points-checked", numPointsChecked);
  }
  return Result(
      std::move(result), qlm::vector<ColumnIndex>{qec_->getAllocator()},
      Result::getMergedLocalVocab(*params_.resultLeft_, *params_.resultRight_));
}
