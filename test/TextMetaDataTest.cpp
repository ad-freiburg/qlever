//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "index/TextMetaData.h"

// Test that sizeOnDisk returns the size of 2 Ids + the metadata sizeOnDisk().
TEST(TextBlockMetaDataTest, sizeOnDisk) {
  EXPECT_EQ(TextBlockMetaData::sizeOnDisk(),
            2 * sizeof(uint64_t) + 2 * ContextListMetaData::sizeOnDisk());
}
