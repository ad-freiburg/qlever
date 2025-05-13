// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
#define QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H

#include <gtest/gtest.h>

#include "util/GeometryInfo.h"

// Helper that asserts (approx.) equality of two GeometryInfo objects
inline void checkGeoInfo(std::optional<ad_utility::GeometryInfo> actual,
                         std::optional<ad_utility::GeometryInfo> expected) {
  ASSERT_EQ(actual.has_value(), expected.has_value());
  if (!actual.has_value() || !expected.has_value()) {
    return;
  }

  auto a = actual.value();
  auto b = expected.value();

  ASSERT_EQ(a.getWktType(), b.getWktType());
  auto aCentroid = a.getCentroid();
  auto bCentroid = b.getCentroid();
  ASSERT_NEAR(aCentroid.getLat(), bCentroid.getLat(), 0.001);
  ASSERT_NEAR(aCentroid.getLng(), bCentroid.getLng(), 0.001);
  auto [all, aur] = a.getBoundingBox();
  auto [bll, bur] = a.getBoundingBox();
  ASSERT_NEAR(all.getLat(), bll.getLat(), 0.001);
  ASSERT_NEAR(all.getLng(), bll.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
}

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
