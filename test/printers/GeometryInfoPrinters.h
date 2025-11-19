// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_PRINTERS_GEOMETRYINFOPRINTERS_H
#define QLEVER_TEST_PRINTERS_GEOMETRYINFOPRINTERS_H

#include <ostream>

#include "rdfTypes/GeometryInfo.h"

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::Centroid& centroid, std::ostream* os) {
  auto& s = *os;
  s << "Centroid(" << centroid.centroid().toStringRepresentation() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::BoundingBox& bb, std::ostream* os) {
  auto& s = *os;
  s << "BoundingBox(Lower Left = " << bb.lowerLeft().toStringRepresentation()
    << ", Upper Right = " << bb.upperRight().toStringRepresentation() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::GeometryType& gt, std::ostream* os) {
  auto& s = *os;
  s << "GeometryType(" << gt.type() << ", IRI: " << gt.asIri().value_or("-")
    << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::NumGeometries& gt, std::ostream* os) {
  auto& s = *os;
  s << "NumGeometries(" << gt.numGeometries() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::MetricLength& gt, std::ostream* os) {
  auto& s = *os;
  s << "MetricLength(" << gt.length() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::MetricArea& gt, std::ostream* os) {
  auto& s = *os;
  s << "MetricArea(" << gt.area() << ")";
}

// _____________________________________________________________________________
inline void PrintTo(const ad_utility::GeometryInfo& gi, std::ostream* os) {
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

#endif  // QLEVER_TEST_PRINTERS_GEOMETRYINFOPRINTERS_H
