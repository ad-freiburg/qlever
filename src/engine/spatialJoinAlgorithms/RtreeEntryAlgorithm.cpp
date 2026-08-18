// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/RtreeEntryAlgorithm.h"

#include "engine/spatialJoinAlgorithms/SpatialJoinGeoUtils.h"
#include "index/ExportIds.h"
#include "util/Exception.h"

using namespace BoostGeometryNamespace;

// ____________________________________________________________________________
std::string_view RtreeEntryAlgorithm::betweenQuotes(
    std::string_view extractFrom) const {
  size_t pos1 = extractFrom.find("\"", 0);
  size_t pos2 = extractFrom.find("\"", pos1 + 1);
  if (pos1 != std::string::npos && pos2 != std::string::npos) {
    return extractFrom.substr(pos1 + 1, pos2 - pos1 - 1);
  } else {
    return extractFrom;
  }
}

// ____________________________________________________________________________
std::optional<size_t> RtreeEntryAlgorithm::getAnyGeometry(
    const IdTableView<0>* idtable, size_t row, size_t col) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  throw std::runtime_error("not supported in C++17 mode currently");
#else
  auto printWarning = [this, &spatialJoin = spatialJoin_]() {
    if (this->numFailedParsedGeometries_ == 0) {
      std::string warning =
          "The input to a spatial join contained at least one element, "
          "that is not a Point, Linestring, Polygon, MultiPoint, "
          "MultiLinestring or MultiPolygon geometry and is thus skipped. Note "
          "that QLever currently only accepts those geometries for "
          "the spatial joins";
      AD_LOG_WARN << warning << std::endl;
      this->numFailedParsedGeometries_ += 1;
      if (spatialJoin.has_value()) {
        AD_CORRECTNESS_CHECK(spatialJoin.value() != nullptr);
        spatialJoin.value()->addWarning(warning);
      }
    }
  };

  // unfortunately, the current implementation requires the fully materialized
  // string. In the future this might get changed. When only the bounding box
  // is needed, one could store it in an ID similar to GeoPoint (but with less
  // precision), and then the full geometry would only need to be read, when
  // the exact distance is wanted
  std::string str(betweenQuotes(ql::exportIds::idToStringAndType(
                                    qec_->getIndex(), idtable->at(row, col), {})
                                    .value()
                                    .first));
  AnyGeometry geometry;
  try {
    bg::read_wkt(str, geometry);
    geometries_.push_back(std::move(geometry));
  } catch (...) {
    printWarning();
    return std::nullopt;
  }
  return geometries_.size() - 1;  // index of the last element
#endif
}

// ____________________________________________________________________________
double RtreeEntryAlgorithm::computeDist(const size_t geometryIndex1,
                                        const size_t geometryIndex2) const {
  return boost::apply_visitor(ClosestPointVisitor(),
                              geometries_.at(geometryIndex1),
                              geometries_.at(geometryIndex2));
}

// ____________________________________________________________________________
size_t RtreeEntryAlgorithm::convertGeoPointToPoint(GeoPoint point) {
  geometries_.emplace_back(Point(point.getLng(), point.getLat()));
  return geometries_.size() - 1;  // index of the last element
}

// ____________________________________________________________________________
Id RtreeEntryAlgorithm::computeDist(RtreeEntry& geo1, RtreeEntry& geo2) {
  auto convertPoint = [&](RtreeEntry& entry) {
    if (entry.geoPoint_) {
      return entry.geoPoint_.value();
    }
    if (!entry.boundingBox_.has_value()) {
      entry.boundingBox_ = boost::apply_visitor(
          BoundingBoxVisitor(), geometries_.at(entry.geometryIndex_.value()));
    }
    Point p = calculateMidpointOfBox(entry.boundingBox_.value());
    return GeoPoint(p.get<1>(), p.get<0>());
  };

  auto getIndex = [&](RtreeEntry& entry) {
    if (!entry.geometryIndex_) {
      entry.geometryIndex_ = convertGeoPointToPoint(entry.geoPoint_.value());
    }
    return entry.geometryIndex_.value();
  };

  // use the already parsed geometries to calculate the distance
  if (useMidpointForAreas_ ||
      (geo1.geoPoint_.has_value() && geo2.geoPoint_.has_value())) {
    return Id::makeFromDouble(ad_utility::detail::wktDistImpl(
        convertPoint(geo1), convertPoint(geo2)));
  } else {
    // at least one area
    return Id::makeFromDouble(computeDist(getIndex(geo1), getIndex(geo2)));
  }
}

// ____________________________________________________________________________
Point RtreeEntryAlgorithm::calculateMidpointOfBox(const Box& box) const {
  double lng = (box.min_corner().get<0>() + box.max_corner().get<0>()) / 2.0;
  double lat = (box.min_corner().get<1>() + box.max_corner().get<1>()) / 2.0;
  return Point(lng, lat);
}

// ____________________________________________________________________________
std::optional<RtreeEntry> RtreeEntryAlgorithm::getRtreeEntry(
    const IdTableView<0>* idTable, const size_t row, const ColumnIndex col) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  throw std::runtime_error("getRtreeEntry is not supported in this build");
#else
  RtreeEntry entry{row, std::nullopt, std::nullopt, std::nullopt};
  entry.geoPoint_ =
      ad_utility::detail::spatialjoin::getPoint(idTable, row, col);

  if (!entry.geoPoint_) {
    entry.geometryIndex_ = getAnyGeometry(idTable, row, col);
    if (!entry.geometryIndex_) {
      return std::nullopt;
    }
    entry.boundingBox_ = boost::apply_visitor(
        BoundingBoxVisitor(), geometries_.at(entry.geometryIndex_.value()));
  } else {
    entry.boundingBox_ =
        Box(Point(entry.geoPoint_.value().getLng(),
                  entry.geoPoint_.value().getLat()),
            Point(entry.geoPoint_.value().getLng() + 0.00000001,
                  entry.geoPoint_.value().getLat() + 0.00000001));
  }
  return entry;
#endif
}
