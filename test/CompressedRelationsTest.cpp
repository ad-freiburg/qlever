//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "index/CompressedRelation.h"
#include "index/Permutations.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {
// Return an `ID` of type `VocabIndex` from `index`. Assert that `index`
// is `>= 0`.
Id V(int64_t index) {
  AD_CHECK(index >= 0);
  return Id::makeFromVocabIndex(VocabIndex::make(index));
}

// A representation of a relation, consisting of the constant `col0_` element
// as well as the 2D-vector for the other two columns. `col1And2_` must be
// sorted lexicographically.
struct RelationInput {
  int col0_;
  std::vector<std::array<int, 2>> col1And2_;
};

// Check that `expected` and `actual` have the same contents. The `int`s in
// expected are converted to `Id`s of type `VocabIndex` using the `V`-function
// before the comparison.
template <size_t NumColumns>
void checkThatTablesAreEqual(
    const std::vector<std::array<int, NumColumns>> expected,
    const IdTable& actual) {
  ASSERT_EQ(NumColumns, actual.numColumns());
  for (size_t i = 0; i < actual.numRows(); ++i) {
    for (size_t j = 0; j < actual.numColumns(); ++j) {
      ASSERT_EQ(V(expected[i][j]), actual(i, j));
    }
  }
}

// Run a set of tests on a permutation that is defined by the `inputs`. The
// `inputs` must be ordered wrt the `col0_`. `testCaseName` is used to create
// a unique name for the required temporary files and for the implicit cache
// of the `CompressedRelationMetaData`. `blocksize` is the size of the blocks
// in which the permutation will be compressed and stored on disk.
void testCompressedRelations(const std::vector<RelationInput>& inputs,
                             std::string testCaseName, size_t blocksize) {
  // First check the invariants of the `inputs`. They must be sorted by the
  // `col0_` and for each of the `inputs` the `col1And2_` must also be sorted.
  AD_CHECK(std::ranges::is_sorted(
      inputs, {}, [](const RelationInput& r) { return r.col0_; }));
  AD_CHECK(std::ranges::all_of(inputs, [](const RelationInput& r) {
    return std::ranges::is_sorted(
        r.col1And2_, [](const auto& a, const auto& b) {
          return std::ranges::lexicographical_compare(a, b);
        });
  }));

  std::string filename = testCaseName + ".dat";

  // First create the on-disk permutation.
  CompressedRelationWriter writer{ad_utility::File{filename, "w"}, blocksize};
  {
    size_t i = 0;
    for (const auto& input : inputs) {
      std::string bufferFilename =
          testCaseName + ".buffers." + std::to_string(i) + ".dat";
      BufferedIdTable buffer{
          ad_utility::BufferedVector<Id>{30, bufferFilename}};
      for (const auto& arr : input.col1And2_) {
        buffer.push_back({V(arr[0]), V(arr[1])});
      }
      // The last argument is the number of distinct elements in `col1`. We
      // store a dummy value here that we can check later.
      writer.addRelation(V(input.col0_), buffer, i + 1);
      ++i;
    }
  }
  writer.finish();
  auto metaData = writer.getFinishedMetaData();
  auto blocks = writer.getFinishedBlocks();
  // Test the serialization of the blocks and the metaData.
  ad_utility::serialization::ByteBufferWriteSerializer w;
  w << metaData;
  w << blocks;
  metaData.clear();
  blocks.clear();
  ad_utility::serialization::ByteBufferReadSerializer r{std::move(w).data()};
  r >> metaData;
  r >> blocks;

  ASSERT_EQ(metaData.size(), inputs.size());

  ad_utility::File file{filename, "r"};
  auto timer = std::make_shared<ad_utility::ConcurrentTimeoutTimer>(
      ad_utility::TimeoutTimer::unlimited());
  // Check the contents of the metadata.
  for (size_t i = 0; i < metaData.size(); ++i) {
    const auto& m = metaData[i];
    ASSERT_EQ(V(inputs[i].col0_), m._col0Id);
    ASSERT_EQ(inputs[i].col1And2_.size(), m._numRows);
    // The number of distinct elements in `col1` was passed in as `i + 1` for
    // testing purposes, so this is the expected multiplicity.
    ASSERT_FLOAT_EQ(m._numRows / static_cast<float>(i + 1),
                    m._multiplicityCol1);
    // Scan for all distinct `col0` and check that we get the expected result.
    IdTable table{2, ad_utility::testing::makeAllocator()};
    CompressedRelationMetadata::scan(metaData[i], blocks,
                                     testCaseName + std::to_string(blocksize),
                                     file, &table, timer);
    const auto& col1And2 = inputs[i].col1And2_;
    checkThatTablesAreEqual(col1And2, table);

    // Check for all distinct combinations of `(col0, col1)` and check that
    // we get the expected result.
    // TODO<joka921>, C++23 use views::chunk_by
    int lastCol1Id = col1And2[0][0];
    std::vector<std::array<int, 1>> col3;

    auto scanAndCheck = [&]() {
      IdTable tableWidthOne{1, ad_utility::testing::makeAllocator()};
      CompressedRelationMetadata::scan(metaData[i], V(lastCol1Id), blocks, file,
                                       &tableWidthOne, timer);
      checkThatTablesAreEqual(col3, tableWidthOne);
    };
    for (size_t j = 0; j < col1And2.size(); ++j) {
      if (col1And2[j][0] == lastCol1Id) {
        col3.push_back({col1And2[j][1]});
        continue;
      }
      scanAndCheck();
      lastCol1Id = col1And2[j][0];
      col3.clear();
      col3.push_back({col1And2[j][1]});
    }
    // Don't forget the last block.
    scanAndCheck();
  }
  file.close();
  ad_utility::deleteFile(filename);
}

// Run `testCompressedRelations` (see above) for the given `inputs` and
// `testCaseName`, but with a set of different `blocksizes` (small and medium
// size, powers of two and odd), to find subtle rounding bugs when creating the
// blocks.
void testWithDifferentBlockSizes(const std::vector<RelationInput>& inputs,
                                 std::string testCaseName) {
  testCompressedRelations(inputs, testCaseName, 37);
  testCompressedRelations(inputs, testCaseName, 237);
  testCompressedRelations(inputs, testCaseName, 4096);
}
}  // namespace

// Test for very small relations many of which are stored in the same block.
TEST(CompressedRelationWriter, SmallRelations) {
  std::vector<RelationInput> inputs;
  for (int i = 1; i < 200; ++i) {
    inputs.push_back(
        RelationInput{i, {{i - 1, i + 1}, {i - 1, i + 2}, {i, i - 1}}});
  }
  testWithDifferentBlockSizes(inputs, "smallRelations");
}

// Test for larger relations that span over several blocks. There are no
// duplicates in the `col1`, so a combination of `(col0, col1)` will be stored
// in a single block.
TEST(CompressedRelationWriter, LargeRelationsDistinctCol1) {
  std::vector<RelationInput> inputs;
  for (int i = 1; i < 6; ++i) {
    std::vector<std::array<int, 2>> col1And2;
    for (int j = 0; j < 200; ++j) {
      col1And2.push_back(std::array{i * j, i * j + 3});
    }
    inputs.push_back(RelationInput{i * 17, std::move(col1And2)});
  }
  testWithDifferentBlockSizes(inputs, "largeRelationsDistinctCol1");
}

// Test for larger relations that span over several blocks. There are many
// duplicates in the `col1`, so a combination of `(col0, col1)` will also be
// stored in several blocks.
TEST(CompressedRelationWriter, LargeRelationsDuplicatesCol1) {
  std::vector<RelationInput> inputs;
  for (int i = 1; i < 6; ++i) {
    std::vector<std::array<int, 2>> col1And2;
    for (int j = 0; j < 200; ++j) {
      col1And2.push_back(std::array{i * 12, i * j + 3});
    }
    inputs.push_back(RelationInput{i * 17, std::move(col1And2)});
  }
  testWithDifferentBlockSizes(inputs, "largeRelationsDuplicatesCol1");
}

// Test a permutation that consists of relations of different sizes and
// characteristics by combining the characteristics of the three test cases
// above.
TEST(CompressedRelationWriter, MixedSizes) {
  std::vector<RelationInput> inputs;
  for (int y = 0; y < 3; ++y) {
    // First some large relations with many duplicates in `col1`.
    for (int i = 1; i < 6; ++i) {
      std::vector<std::array<int, 2>> col1And2;
      for (int j = 0; j < 50; ++j) {
        col1And2.push_back(std::array{i * 12, i * j + 3});
      }
      inputs.push_back(RelationInput{i + (y * 300), std::move(col1And2)});
    }

    // Then some small relations
    for (int i = 9; i < 50; ++i) {
      inputs.push_back(RelationInput{
          i + (y * 300), {{i - 1, i + 1}, {i - 1, i + 2}, {i, i - 1}}});
    }

    // Finally some large relations with few duplicates in `col1`.
    for (int i = 205; i < 221; ++i) {
      std::vector<std::array<int, 2>> col1And2;
      for (int j = 0; j < 80; ++j) {
        col1And2.push_back(std::array{i * j + y, i * j + 3});
      }
      inputs.push_back(RelationInput{i + (y * 300), std::move(col1And2)});
    }
  }
  testWithDifferentBlockSizes(inputs, "mixedSizes");
}

TEST(CompressedRelationWriter, MultiplicityCornerCases) {
  ASSERT_EQ(1.0f, CompressedRelationWriter::computeMultiplicity(12, 12));

  constexpr static size_t veryLarge = 1111111111111111;
  constexpr static size_t plusOne = veryLarge + 1;
  ASSERT_EQ(1.0f, static_cast<float>(plusOne) / static_cast<float>(veryLarge));
  ASSERT_NE(1.0f,
            CompressedRelationWriter::computeMultiplicity(plusOne, veryLarge));
}

TEST(CompressedRelationMetadata, GettersAndSetters) {
  CompressedRelationMetadata m;
  m.setCol1Multiplicity(2.0f);
  ASSERT_FLOAT_EQ(2.0f, m.getCol1Multiplicity());
  ASSERT_FLOAT_EQ(2.0f, m._multiplicityCol1);
  m.setCol2Multiplicity(1.0f);
  ASSERT_FLOAT_EQ(1.0f, m._multiplicityCol2);
  ASSERT_FLOAT_EQ(1.0f, m.getCol2Multiplicity());
  ASSERT_FALSE(m.isFunctional());
  m.setCol1Multiplicity(1.0f);
  ASSERT_TRUE(m.isFunctional());
  m._numRows = 43;
  ASSERT_EQ(43, m._numRows);
}
