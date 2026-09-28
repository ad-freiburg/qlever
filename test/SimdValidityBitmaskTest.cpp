// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "engine/SimdValidityBitmask.h"
#include "global/Id.h"
#include "global/ValueId.h"

using namespace ad_utility::simd;

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, ValidityBitmask64Basics) {
  // Default is all unbound (mask = 0)
  ValidityBitmask64 defaultMask;
  EXPECT_TRUE(defaultMask.allUnbound());
  EXPECT_FALSE(defaultMask.allValid());
  EXPECT_TRUE(defaultMask.hasUnbound());
  EXPECT_FALSE(defaultMask.hasValid());
  EXPECT_EQ(defaultMask.rawMask(), 0ULL);
  EXPECT_EQ(defaultMask.countValid(), 0u);
  EXPECT_EQ(defaultMask.countUnbound(), 64u);
  EXPECT_EQ(defaultMask.firstUnboundIndex(), 0u);
  EXPECT_EQ(defaultMask.firstValidIndex(), 64u);

  // All valid mask
  auto allValid = ValidityBitmask64::allValidMask();
  EXPECT_TRUE(allValid.allValid());
  EXPECT_FALSE(allValid.allUnbound());
  EXPECT_FALSE(allValid.hasUnbound());
  EXPECT_TRUE(allValid.hasValid());
  EXPECT_EQ(allValid.rawMask(), ~0ULL);
  EXPECT_EQ(allValid.countValid(), 64u);
  EXPECT_EQ(allValid.countUnbound(), 0u);
  EXPECT_EQ(allValid.firstUnboundIndex(), 64u);
  EXPECT_EQ(allValid.firstValidIndex(), 0u);

  // Row inspection and mutation
  ValidityBitmask64 mask;
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_TRUE(mask.isRowUnbound(i));
    EXPECT_FALSE(mask.isRowValid(i));
    mask.setRowValid(i);
    EXPECT_TRUE(mask.isRowValid(i));
    EXPECT_FALSE(mask.isRowUnbound(i));
    EXPECT_EQ(mask.countValid(), i + 1);
    EXPECT_EQ(mask.countUnbound(), 64 - (i + 1));
  }
  EXPECT_TRUE(mask.allValid());

  for (size_t i = 0; i < 64; ++i) {
    mask.setRowUnbound(i);
    EXPECT_TRUE(mask.isRowUnbound(i));
    EXPECT_FALSE(mask.isRowValid(i));
  }
  EXPECT_TRUE(mask.allUnbound());

  // setRow helper
  mask.setRow(7, true);
  EXPECT_TRUE(mask.isRowValid(7));
  EXPECT_EQ(mask.firstValidIndex(), 7u);
  EXPECT_EQ(mask.firstUnboundIndex(), 0u);
  mask.setRow(7, false);
  EXPECT_TRUE(mask.isRowUnbound(7));
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, BitwiseOperators) {
  ValidityBitmask64 maskA{0x0F0F0F0F0F0F0F0FULL};
  ValidityBitmask64 maskB{0x00FF00FF00FF00FFULL};

  EXPECT_EQ((maskA & maskB).rawMask(), 0x000F000F000F000FULL);
  EXPECT_EQ((maskA | maskB).rawMask(), 0x0FFF0FFF0FFF0FFFULL);
  EXPECT_EQ((maskA ^ maskB).rawMask(), 0x0FF00FF00FF00FF0ULL);
  EXPECT_EQ((~maskA).rawMask(), ~0x0F0F0F0F0F0F0F0FULL);

  ValidityBitmask64 m = maskA;
  m &= maskB;
  EXPECT_EQ(m.rawMask(), 0x000F000F000F000FULL);
  m = maskA;
  m |= maskB;
  EXPECT_EQ(m.rawMask(), 0x0FFF0FFF0FFF0FFFULL);
  m = maskA;
  m ^= maskB;
  EXPECT_EQ(m.rawMask(), 0x0FF00FF00FF00FF0ULL);

  EXPECT_TRUE(maskA == ValidityBitmask64{0x0F0F0F0F0F0F0F0FULL});
  EXPECT_TRUE(maskA != maskB);
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, ForEachValidAndUnbound) {
  ValidityBitmask64 mask{0x1000000000000001ULL};  // Bits 0 and 60 set

  std::vector<size_t> validIndices;
  mask.forEachValid([&](size_t idx) { validIndices.push_back(idx); });
  EXPECT_EQ(validIndices, (std::vector<size_t>{0, 60}));

  std::vector<size_t> unboundIndices;
  mask.forEachUnbound([&](size_t idx) { unboundIndices.push_back(idx); });
  EXPECT_EQ(unboundIndices.size(), 62u);
  EXPECT_EQ(unboundIndices.front(), 1u);
  EXPECT_EQ(unboundIndices.back(), 63u);
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, SimdScanBatch64AllCases) {
  std::vector<ValueId> batch(64, ValueId::makeUndefined());

  // Case 1: All unbound
  ValidityBitmask64 maskUnbound =
      SimdValidityScanner::scanBatch64(batch.data());
  EXPECT_TRUE(maskUnbound.allUnbound());
  EXPECT_EQ(maskUnbound.rawMask(), 0ULL);
  EXPECT_TRUE(SimdValidityScanner::isAllUnbound64(batch.data()));
  EXPECT_FALSE(SimdValidityScanner::isAllValid64(batch.data()));

  // Case 2: All valid
  for (size_t i = 0; i < 64; ++i) {
    if (i % 4 == 0) {
      batch[i] = ValueId::makeFromInt(static_cast<int64_t>(i + 1));
    } else if (i % 4 == 1) {
      batch[i] = ValueId::makeFromDouble(static_cast<double>(i) + 0.5);
    } else if (i % 4 == 2) {
      batch[i] = ValueId::makeFromVocabIndex(VocabIndex::make(i + 100));
    } else {
      batch[i] = ValueId::makeFromBool(i % 2 == 0);
    }
  }
  ValidityBitmask64 maskValid = SimdValidityScanner::scanBatch64(batch.data());
  EXPECT_TRUE(maskValid.allValid());
  EXPECT_EQ(maskValid.rawMask(), ~0ULL);
  EXPECT_FALSE(SimdValidityScanner::isAllUnbound64(batch.data()));
  EXPECT_TRUE(SimdValidityScanner::isAllValid64(batch.data()));

  // Case 3: Exactly 1 valid row at each position 0..63
  for (size_t target = 0; target < 64; ++target) {
    std::fill(batch.begin(), batch.end(), ValueId::makeUndefined());
    batch[target] = ValueId::makeFromInt(42);

    ValidityBitmask64 m = SimdValidityScanner::scanBatch64(batch.data());
    EXPECT_EQ(m.rawMask(), (1ULL << target));
    EXPECT_EQ(m.countValid(), 1u);
    EXPECT_EQ(m.countUnbound(), 63u);
    EXPECT_EQ(m.firstValidIndex(), target);
    EXPECT_FALSE(SimdValidityScanner::isAllUnbound64(batch.data()));
  }

  // Case 4: Exactly 1 unbound row at each position 0..63
  for (size_t target = 0; target < 64; ++target) {
    for (size_t i = 0; i < 64; ++i) {
      batch[i] = ValueId::makeFromInt(static_cast<int64_t>(i + 10));
    }
    batch[target] = ValueId::makeUndefined();

    ValidityBitmask64 m = SimdValidityScanner::scanBatch64(batch.data());
    EXPECT_EQ(m.rawMask(), ~(1ULL << target));
    EXPECT_EQ(m.countValid(), 63u);
    EXPECT_EQ(m.countUnbound(), 1u);
    EXPECT_EQ(m.firstUnboundIndex(), target);
    EXPECT_FALSE(SimdValidityScanner::isAllValid64(batch.data()));
  }

  // Case 5: Alternating pattern (even = valid, odd = unbound)
  for (size_t i = 0; i < 64; ++i) {
    if (i % 2 == 0) {
      batch[i] = ValueId::makeFromInt(1);
    } else {
      batch[i] = ValueId::makeUndefined();
    }
  }
  ValidityBitmask64 altMask = SimdValidityScanner::scanBatch64(batch.data());
  EXPECT_EQ(altMask.rawMask(), 0x5555555555555555ULL);
  EXPECT_EQ(altMask.countValid(), 32u);
  EXPECT_EQ(altMask.countUnbound(), 32u);
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, RandomizedSimdScanning) {
  std::mt19937_64 rng(1337);
  std::bernoulli_distribution dist(0.5);

  std::vector<ValueId> batch(64);
  for (size_t trial = 0; trial < 1000; ++trial) {
    uint64_t expectedMask = 0;
    for (size_t i = 0; i < 64; ++i) {
      bool isValid = dist(rng);
      if (isValid) {
        batch[i] = ValueId::makeFromInt(static_cast<int64_t>(i + 1));
        expectedMask |= (1ULL << i);
      } else {
        batch[i] = ValueId::makeUndefined();
      }
    }

    ValidityBitmask64 scanned = SimdValidityScanner::scanBatch64(batch.data());
    EXPECT_EQ(scanned.rawMask(), expectedMask)
        << "Failed on trial " << trial << " with expected mask 0x" << std::hex
        << expectedMask << " vs scanned 0x" << scanned.rawMask();
  }
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, ScanBatchSub64) {
  for (size_t size = 0; size <= 64; ++size) {
    std::vector<ValueId> subBatch(size);
    uint64_t expectedMask = 0;
    for (size_t i = 0; i < size; ++i) {
      if (i % 3 == 0) {
        subBatch[i] = ValueId::makeFromInt(100);
        expectedMask |= (1ULL << i);
      } else {
        subBatch[i] = ValueId::makeUndefined();
      }
    }

    ValidityBitmask64 m = SimdValidityScanner::scanBatch(
        ql::span<const ValueId>{subBatch.data(), subBatch.size()});
    EXPECT_EQ(m.rawMask(), expectedMask);
  }
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, ScanColumnLarge) {
  const size_t numRows = 2500;
  std::vector<ValueId> column(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    if (i % 7 == 0) {
      column[i] = ValueId::makeUndefined();
    } else {
      column[i] = ValueId::makeFromInt(static_cast<int64_t>(i));
    }
  }

  auto bitmasks = SimdValidityScanner::scanColumn(column);
  EXPECT_EQ(bitmasks.size(), (numRows + 63) / 64);

  for (size_t r = 0; r < numRows; ++r) {
    size_t batchIdx = r / 64;
    size_t bitIdx = r % 64;
    bool expectedValid = (r % 7 != 0);
    EXPECT_EQ(bitmasks[batchIdx].isRowValid(bitIdx), expectedValid)
        << "Mismatch at row " << r;
  }
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, VectorizedStoreCsvAndTsv) {
  // Test 64 delimiter tokens
  std::string buffer(128, 'x');

  // CSV store
  char* nextCsv = SimdValidityScanner::writeUnboundBatchCsv(buffer.data(), ',');
  EXPECT_EQ(nextCsv, buffer.data() + 64);
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(buffer[i], ',');
  }
  EXPECT_EQ(buffer[64], 'x');  // No buffer overflow

  // TSV store
  std::fill(buffer.begin(), buffer.end(), 'y');
  char* nextTsv =
      SimdValidityScanner::writeUnboundBatchTsv(buffer.data(), '\t');
  EXPECT_EQ(nextTsv, buffer.data() + 64);
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(buffer[i], '\t');
  }
  EXPECT_EQ(buffer[64], 'y');

  // Row pairs (CSV with newline: 64 * 2 = 128 bytes)
  std::string rowBuffer(256, 'z');
  char* nextRowsCsv =
      SimdValidityScanner::writeUnboundRowsCsv(rowBuffer.data(), ',', '\n');
  EXPECT_EQ(nextRowsCsv, rowBuffer.data() + 128);
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(rowBuffer[i * 2], ',');
    EXPECT_EQ(rowBuffer[i * 2 + 1], '\n');
  }
  EXPECT_EQ(rowBuffer[128], 'z');

  // Row pairs (TSV with newline: 64 * 2 = 128 bytes)
  std::fill(rowBuffer.begin(), rowBuffer.end(), 'w');
  char* nextRowsTsv =
      SimdValidityScanner::writeUnboundRowsTsv(rowBuffer.data(), '\t', '\n');
  EXPECT_EQ(nextRowsTsv, rowBuffer.data() + 128);
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(rowBuffer[i * 2], '\t');
    EXPECT_EQ(rowBuffer[i * 2 + 1], '\n');
  }
  EXPECT_EQ(rowBuffer[128], 'w');
}

// _____________________________________________________________________________
TEST(SimdValidityBitmaskTest, UnalignedMemorySafety) {
  // Test that unaligned pointers do not cause faults or bad stores
  std::vector<char> buffer(256, '0');
  for (size_t offset = 1; offset < 32; ++offset) {
    char* dest = buffer.data() + offset;
    char* written = SimdValidityScanner::writeUnboundBatchCsv(dest, ',');
    EXPECT_EQ(written, dest + 64);
    for (size_t i = 0; i < 64; ++i) {
      EXPECT_EQ(dest[i], ',');
    }
  }
}
