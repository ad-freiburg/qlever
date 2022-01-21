//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>


#include <gtest/gtest.h>
#include "../src/global/Id.h"
#include "../src/util/Log.h"

#include <ranges>

namespace views = std::views;

TEST(Id, toInternalId) {
  ad_utility::InternalExternalIdManager<1, 1> m;
  for (int i : views::iota(0, 256)) {
    auto id = m.getNextInternalId();
    ASSERT_EQ(i * 256, id);
  }

}

TEST(Id, FirstTest) {
  ad_utility::InternalExternalIdManager<1, 1> m;
  ASSERT_EQ(m.maxInternalId, 255);
  ASSERT_EQ(m.maxExternalIdPerBlock, 255);
  ASSERT_EQ(m.maxId, 256 * 256 - 1);
  for (int i : views::iota(0, 256 * 256)) {
    auto id = m.getNextExternalId();
    ASSERT_EQ(i, id);
  }
  ASSERT_THROW(m.getNextExternalId(), ad_semsearch::Exception);

}

