//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/Id.h"
#include "../src/util/Log.h"

TEST(Id, RoundedIds) {
  ad_utility::RoundedIdManager<1> m;
  for (size_t i = 0; i < 256; ++i) {
    auto id = m.getNextRoundedId();
    ASSERT_EQ(i * 256, id);
    ASSERT_TRUE(m.isRoundedId(id));
    ASSERT_EQ(i, m.toRoundedId(id));
  }
}

TEST(Id, UnroundedIds) {
  ad_utility::RoundedIdManager<1> m;
  for (size_t i = 0; i < 256; ++i) {
    auto id = m.getNextUnroundedId();
    ASSERT_EQ(i, id);
  }
}

TEST(Id, MixedIds) {
  // TODO<joka921> implement this and extend all others.
}
