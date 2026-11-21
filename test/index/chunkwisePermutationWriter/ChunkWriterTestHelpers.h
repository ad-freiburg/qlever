// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITERTESTHELPERS_H
#define QLEVER_TEST_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITERTESTHELPERS_H

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "../../util/PermutationPairTestHelpers.h"
#include "global/Constants.h"
#include "index/chunkwisePermutationWriter/ChunkWriter.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/File.h"

// NOTE: The `ChunkWriter` only exists in the C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Helpers for the tests of the `ChunkWriter` and of the consolidation of the
// results of several chunks (see `BoundaryConsolidator`).
namespace chunkWriterTestHelpers {
using namespace chunkwisePermutationWriter;
using ad_utility::source_location;
// The rows of the test input have the four columns `col0`, `col1`, `col2`, and
// the graph.
using permutationPairTestHelpers::scanRelation;
using permutationPairTestHelpers::TestRow;
using permutationPairTestHelpers::TestRows;
using permutationPairTestHelpers::twinRows;
using permutationPairTestHelpers::V;

inline constexpr size_t NUM_COLUMNS = 4;
// The blocksize of the permutations, so relations with more than 16 rows are
// large.
inline constexpr size_t BLOCKSIZE = 20;

// Return the `numRows` rows of the relation with the given `col0`, sorted.
// Every pair of consecutive rows forms the same triple with two different
// graphs (which therefore have to end up in the same block), and every value
// of `col1` occurs (up to) four times.
inline TestRows relation(int64_t col0, size_t numRows) {
  TestRows rows;
  for (size_t i = 0; i < numRows; ++i) {
    auto j = static_cast<int64_t>(i);
    rows.push_back({col0, j / 4, j / 2, j % 2});
  }
  return rows;
}

// Concatenate the `relations`.
inline TestRows concat(const std::vector<TestRows>& relations) {
  TestRows result;
  for (const auto& rows : relations) {
    result.insert(result.end(), rows.begin(), rows.end());
  }
  return result;
}

// Return the rows `[begin, end)` of the `rows`.
inline TestRows subRows(const TestRows& rows, size_t begin, size_t end) {
  return {rows.begin() + begin, rows.begin() + end};
}

// Convert the `rows` to a `Block`.
inline Block makeBlock(const TestRows& rows) {
  Block block{NUM_COLUMNS, ad_utility::makeUnlimitedAllocator<Id>()};
  block.resize(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    for (size_t col = 0; col < NUM_COLUMNS; ++col) {
      block(i, col) = V(rows[i][col]);
    }
  }
  return block;
}

// Convert the rows of an `IdTable` (with `NUM_COLUMNS` columns) to `TestRows`.
template <typename Table>
inline TestRows toTestRows(const Table& table) {
  TestRows rows;
  for (size_t i = 0; i < table.numRows(); ++i) {
    TestRow row;
    for (size_t col = 0; col < NUM_COLUMNS; ++col) {
      row.push_back(static_cast<int64_t>(table(i, col).getVocabIndex().get()));
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

// The two `CompressedRelationWriter`s of a pair of twin permutations, together
// with the `ChunkWriterContext` of the chunks that write them.
class TwinWriters {
 public:
  std::string basename_;
  std::unique_ptr<CompressedRelationWriter> writer1_;
  std::unique_ptr<CompressedRelationWriter> writer2_;
  ChunkWriterContext context_;
  std::vector<CompressedBlockMetadata> blocks1_;
  std::vector<CompressedBlockMetadata> blocks2_;

  // Create the writers for the permutation with the given `keyOrder`, whose
  // files are named after the `basename`.
  explicit TwinWriters(std::string basename,
                       qlever::KeyOrder keyOrder = qlever::KeyOrder{0, 1, 2, 3},
                       bool removeDuplicates = false)
      : basename_{std::move(basename)},
        writer1_{makeWriter(basename_ + ".perm1")},
        writer2_{makeWriter(basename_ + ".perm2")},
        context_{*writer1_,
                 *writer2_,
                 keyOrder.keys().at(0),
                 compressedRelationHelpers::permutedColIndices(keyOrder,
                                                               NUM_COLUMNS),
                 removeDuplicates,
                 basename_,
                 ad_utility::MemorySize::kilobytes(100)} {}

  ~TwinWriters() {
    ad_utility::deleteFile(basename_ + ".perm1", false);
    ad_utility::deleteFile(basename_ + ".perm2", false);
  }

  // Finish both writers and store their blocks.
  void finish() {
    blocks1_ = std::move(*writer1_).getFinishedBlocks();
    blocks2_ = std::move(*writer2_).getFinishedBlocks();
  }

  // Scan the relation with the given `col0` from the first (`twin == false`)
  // or the second permutation. Return all its rows, including `col0`.
  TestRows scan(int64_t col0, bool twin) const {
    auto scanned =
        scanRelation(basename_ + (twin ? ".perm2" : ".perm1"),
                     twin ? blocks2_ : blocks1_, V(col0), NUM_COLUMNS);
    TestRows rows;
    for (size_t i = 0; i < scanned.numRows(); ++i) {
      TestRow row{col0};
      for (size_t col = 0; col < scanned.numColumns(); ++col) {
        row.push_back(
            static_cast<int64_t>(scanned(i, col).getVocabIndex().get()));
      }
      rows.push_back(std::move(row));
    }
    return rows;
  }

  // Check that both permutations contain exactly the `rows` of the complete
  // relation with the given `col0` (in the first permutation) and their twins
  // (in the second permutation).
  void expectCompleteRelation(int64_t col0, const TestRows& rows,
                              source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(scan(col0, false), rows);
    EXPECT_EQ(scan(col0, true), twinRows(rows));
  }

 private:
  static std::unique_ptr<CompressedRelationWriter> makeWriter(
      const std::string& filename) {
    return std::make_unique<CompressedRelationWriter>(
        NUM_COLUMNS, ad_utility::File{filename, "w"}, BLOCKSIZE,
        ad_utility::testing::multiThreadExecutor());
  }
};

// Process the `rows` with the `writer`, cut into input blocks of
// `inputBlockSize` rows each, and finish the chunk.
inline ChunkResult processRows(ChunkWriter& writer, const TestRows& rows,
                               size_t inputBlockSize) {
  for (size_t begin = 0; begin < rows.size(); begin += inputBlockSize) {
    size_t end = std::min(begin + inputBlockSize, rows.size());
    auto shared = writer.processBlock(makeBlock(subRows(rows, begin, end)));
    EXPECT_NE(shared, nullptr);
  }
  return writer.finishChunk();
}

}  // namespace chunkWriterTestHelpers

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_TEST_INDEX_CHUNKWISEPERMUTATIONWRITER_CHUNKWRITERTESTHELPERS_H
