// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RTREEENTRYALGORITHM_H
#define QLEVER_SRC_ENGINE_RTREEENTRYALGORITHM_H

#include <variant>

#include "engine/spatialJoinAlgorithms/BoostGeometryTypes.h"
#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithmBase.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "util/VectorWithMemoryLimit.h"

// Intermediate base for the two algorithms that support arbitrary geometries
// (points and areas) by parsing each of them into an `RtreeEntry` and
// approximating the distance between two such entries: `BaselineAlgorithm`
// (checks every pair) and `BoundingBoxAlgorithm` (prunes pairs using an
// r-tree over these entries). The S2-based algorithms only support points and
// `LibspatialjoinAlgorithm` parses geometries completely differently, so none
// of them need (or get) this shared state.
class RtreeEntryAlgorithm : public SpatialJoinAlgorithmBase {
 protected:
  using Point = BoostGeometryNamespace::Point;
  using Box = BoostGeometryNamespace::Box;
  using AnyGeometry = BoostGeometryNamespace::AnyGeometry;
  using RtreeEntry = BoostGeometryNamespace::RtreeEntry;

 public:
  using SpatialJoinAlgorithmBase::SpatialJoinAlgorithmBase;

  // calculates the midpoint of the given Box
  Point calculateMidpointOfBox(const Box& box) const;

  void setUseMidpointForAreas_(bool useMidpointForAreas) {
    useMidpointForAreas_ = useMidpointForAreas;
  }

  // Helper function, which computes the distance of two geometries, where each
  // geometry has already been parsed and is available as an RtreeEntry
  Id computeDist(RtreeEntry& geo1, RtreeEntry& geo2);

  // wrapper to access non const private function for testing
  std::optional<RtreeEntry> onlyForTestingGetRtreeEntry(
      const IdTableView<0>* idTable, const size_t row, const ColumnIndex col) {
    return getRtreeEntry(idTable, row, col);
  }

 protected:
  // this helper function takes an idtable, a row and a column. It then tries
  // to parse a geometry or a geoPoint of that cell in the idtable. If it
  // succeeds, it returns an rtree entry of that geometry/geopoint
  std::optional<RtreeEntry> getRtreeEntry(const IdTableView<0>* idTable,
                                          const size_t row,
                                          const ColumnIndex col);

  // if the distance calculation should be approximated, by the midpoint of
  // the area
  bool useMidpointForAreas_ = true;

  // this vector stores the geometries, which have already been parsed
  ad_utility::VectorWithMemoryLimit<AnyGeometry> geometries_{
      qec_->getAllocator()};

 private:
  // returns everything between the first two quotes. If the string does not
  // contain two quotes, the string is returned as a whole
  std::string_view betweenQuotes(std::string_view extractFrom) const;

  // this helper function gets the string which represents the area from the
  // idtable.
  std::optional<size_t> getAnyGeometry(const IdTableView<0>* idtable,
                                       size_t row, size_t col);

  // this helper function approximates a conversion of the distance between two
  // objects from degrees to meters. Here we assume, that the conversion from
  // degrees to meters is constant, which is however only true for the latitude
  // values. For the longitude values this is not true. Therefore a value which
  // works very good for almost all longitudes and latitudes has been chosen.
  // Only for the poles, the conversion will be way to large (for the longitude
  // difference). Note, that this function is expensive and should only be
  // called when needed
  double computeDist(const size_t geometryIndex1,
                     const size_t geometryIndex2) const;

  // this helper function converts a GeoPoint into a boost geometry Point
  size_t convertGeoPointToPoint(GeoPoint point);

  // number of times the parsing of a geometry failed. For now this is only used
  // to print the warning once, but it could also be used to print how many
  // geometries failed. It is mutable to let parsing function which are const
  // still modify the the nr of failed parsings.
  size_t numFailedParsedGeometries_ = 0;
};

#endif  // QLEVER_SRC_ENGINE_RTREEENTRYALGORITHM_H
