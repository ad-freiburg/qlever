// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_PRINTERS_GEOMETRYINFOPRINTERS_H
#define QLEVER_TEST_PRINTERS_GEOMETRYINFOPRINTERS_H

#include <ostream>

#include "rdfTypes/GeometryInfo.h"

// Note that the printers must be defined inside the `ad_utility` or `testing`
// namespace such that argument-dependent lookup can find them within `gtest`.
namespace ad_utility {

// _____________________________________________________________________________
inline void PrintTo(const Centroid& centroid, std::ostream* os) {
  auto& s = *os;
  s << "Centroid(" << centroid.centroid().toStringRepresentation() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const BoundingBox& bb, std::ostream* os) {
  auto& s = *os;
  s << "BoundingBox(Lower Left = " << bb.lowerLeft().toStringRepresentation()
    << ", Upper Right = " << bb.upperRight().toStringRepresentation() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const GeometryType& gt, std::ostream* os) {
  auto& s = *os;
  s << "GeometryType(" << std::to_string(gt.type())
    << ", IRI: " << gt.asIri().value_or("-") << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const NumGeometries& gt, std::ostream* os) {
  auto& s = *os;
  s << "NumGeometries(" << gt.numGeometries() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const MetricLength& gt, std::ostream* os) {
  auto& s = *os;
  s << "MetricLength(" << gt.length() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const MetricArea& gt, std::ostream* os) {
  auto& s = *os;
  s << "MetricArea(" << gt.area() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const GeometryInfo& gi, std::ostream* os) {
  auto& s = *os;
  s << "GeometryInfo(";
  PrintTo(gi.getWktType(), os);
  s << ",";
  PrintTo(gi.getCentroid(), os);
  s << ",";
  PrintTo(gi.getBoundingBox(), os);
  s << ",";
  PrintTo(gi.getNumGeometries(), os);
  s << ",";
  PrintTo(gi.getMetricLength(), os);
  s << ",";
  PrintTo(gi.getMetricArea(), os);
  s << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const GeoPointOrWkt& g, std::ostream* os) {
  auto& s = *os;
  s << "GeoPointOrWkt(";
  std::visit(
      [&](auto& contained) {
        using T = std::decay_t<decltype(contained)>;
        if constexpr (std::is_same_v<T, GeoPoint>) {
          s << contained.toStringRepresentation();
        } else {
          static_assert(std::is_same_v<T, std::string>);
          s << contained;
        }
      },
      g);
  s << ")";
}

}  // namespace ad_utility

#endif  // QLEVER_TEST_PRINTERS_GEOMETRYINFOPRINTERS_H
