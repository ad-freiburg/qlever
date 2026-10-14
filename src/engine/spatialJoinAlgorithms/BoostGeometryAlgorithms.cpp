// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// NOTE: This file is compiled without the precompiled header of the `engine`
// library and must not (transitively) include `<boost/sort/...>`, see the note
// at the top of `BoostGeometryTypes.h`.
#include <boost/geometry/algorithms/closest_points.hpp>
#include <boost/geometry/algorithms/covered_by.hpp>
#include <stdexcept>

#include "engine/spatialJoinAlgorithms/BoostGeometryTypes.h"

namespace BoostGeometryNamespace {

// ____________________________________________________________________________
Segment closestPoints(const AnyGeometry& geo1, const AnyGeometry& geo2) {
  return boost::apply_visitor(
      [](const auto& g1, const auto& g2) {
        Segment seg;
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
        (void)g1;
        (void)g2;
        throw std::runtime_error(
            "`closestPoints` is not implemented for C++17, please use a "
            "different spatial join implementation");
#else
        bg::closest_points(g1, g2, seg);
#endif
        return seg;
      },
      geo1, geo2);
}

// ____________________________________________________________________________
bool coveredBy(const Point& point, const Box& box) {
  return bg::covered_by(point, box);
}

}  // namespace BoostGeometryNamespace
