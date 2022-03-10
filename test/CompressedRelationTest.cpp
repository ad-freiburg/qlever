//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/CompressedRelation.h"

TEST(CompressedRelation, Writer) {
  std::string filename = "compressedRelation.tmp";

  {
    CompressedRelationWriter writer(ad_utility::File{filename, "w"});
    auto pusher = writer.makeTriplePusher();
    std::vector<std::array<Id, 3>> v;

    v.push_back({1, 10, 20});
    v.push_back({1, 10, 21});
    v.push_back({1, 11, 20});
    v.push_back({2, 10, 22});
    pusher.push(v);
    pusher.finish();

    auto metaData = writer.getFinishedMetaData();
    auto blocks = writer.getFinishedBlocks();

    ASSERT_EQ(metaData.size(), 2);
    ASSERT_EQ(blocks.size(), 1);
  }

  ad_utility::deleteFile(filename);
}
