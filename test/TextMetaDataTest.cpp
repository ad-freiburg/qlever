// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "index/TextMetaData.h"

// Test that `ContextListMetaData::sizeOnDisk` is the size of its five fields.
TEST(ContextListMetaDataTest, sizeOnDisk) {
  EXPECT_EQ(ContextListMetaData::sizeOnDisk(),
            sizeof(size_t) + 4 * sizeof(off_t));
}

// Test that `TextBlockMetaData::sizeOnDisk` is the size of the two `uint64_t`
// word indices plus two `ContextListMetaData`.
TEST(TextBlockMetaDataTest, sizeOnDisk) {
  EXPECT_EQ(TextBlockMetaData::sizeOnDisk(),
            2 * sizeof(uint64_t) + 2 * ContextListMetaData::sizeOnDisk());
}
