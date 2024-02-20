//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "index/CompressedRelation.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/SourceLocation.h"

namespace {

using ad_utility::source_location;

// Return an `ID` of type `VocabIndex` from `index`. Assert that `index`
// is `>= 0`.
Id V(int64_t index) {
  AD_CONTRACT_CHECK(index >= 0);
  return Id::makeFromVocabIndex(VocabIndex::make(index));
}

// A representation of a relation, consisting of the constant `col0_` element
// as well as the 2D-vector for the other two columns. `col1And2_` must be
// sorted lexicographically.
using RowInput = std::vector<int>;
struct RelationInput {
  int col0_;
  std::vector<RowInput> col1And2_;
};

template <typename Inner>
size_t getNumColumns(const std::vector<Inner>& input) {
  if (input.empty()) {
    return 2;
  }
  auto result = input.at(0).size();
  AD_CONTRACT_CHECK(std::ranges::all_of(
      input, [result](const auto& vec) { return vec.size() == result; }));
  return result;
}

size_t getNumColumns(const std::vector<RelationInput>& vec) {
  if (vec.empty()) {
    return 2;
  }
  auto result = getNumColumns(vec.at(0).col1And2_);
  AD_CONTRACT_CHECK(std::ranges::all_of(vec, [&result](const auto& relation) {
    return getNumColumns(relation.col1And2_) == result;
  }));
  return result;
}

// Check that `expected` and `actual` have the same contents. The `int`s in
// expected are converted to `Id`s of type `VocabIndex` using the `V`-function
// before the comparison.
void checkThatTablesAreEqual(const auto& expected, const IdTable& actual,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  ASSERT_EQ(getNumColumns(expected), actual.numColumns());
  if (actual.numRows() != expected.size()) {
    LOG(WARN) << actual.numRows() << "vs " << expected.size() << std::endl;
    LOG(WARN) << "mismatch" << std::endl;
  }
  ASSERT_EQ(actual.numRows(), expected.size());
  for (size_t i = 0; i < actual.numRows(); ++i) {
    for (size_t j = 0; j < actual.numColumns(); ++j) {
      ASSERT_EQ(V(expected[i][j]), actual(i, j));
    }
  }
}

}  // namespace

// Run a set of tests on a permutation that is defined by the `inputs`. The
// `inputs` must be ordered wrt the `col0_`. `testCaseName` is used to create
// a unique name for the required temporary files and for the implicit cache
// of the `CompressedRelationMetaData`. `blocksize` is the size of the blocks
// in which the permutation will be compressed and stored on disk.
void testCompressedRelations(const auto& inputs, std::string testCaseName,
                             ad_utility::MemorySize blocksize) {
  // First check the invariants of the `inputs`. They must be sorted by the
  // `col0_` and for each of the `inputs` the `col1And2_` must also be sorted.
  AD_CONTRACT_CHECK(std::ranges::is_sorted(
      inputs, {}, [](const RelationInput& r) { return r.col0_; }));
  AD_CONTRACT_CHECK(std::ranges::all_of(inputs, [](const RelationInput& r) {
    return std::ranges::is_sorted(
        r.col1And2_, [](const auto& a, const auto& b) {
          return std::ranges::lexicographical_compare(a, b);
        });
  }));

  std::string filename = testCaseName + ".dat";

  // First create the on-disk permutation.
  size_t numColumns = getNumColumns(inputs);
  CompressedRelationWriter writer{numColumns, ad_utility::File{filename, "w"},
                                  blocksize};
  vector<CompressedRelationMetadata> metaData;
  {
    size_t i = 0;
    for (const auto& input : inputs) {
      std::string bufferFilename =
          testCaseName + ".buffers." + std::to_string(i) + ".dat";
      IdTable buffer{numColumns, ad_utility::makeUnlimitedAllocator<Id>()};
      size_t numBlocks = 0;

      auto addBlock = [&]() {
        if (buffer.empty()) {
          return;
        }
        writer.addBlockForLargeRelation(
            V(input.col0_), std::make_shared<IdTable>(std::move(buffer)));
        buffer.clear();
        ++numBlocks;
      };
      for (const auto& arr : input.col1And2_) {
        buffer.push_back(std::views::transform(arr, V));
        if (buffer.numRows() > writer.blocksize()) {
          addBlock();
        }
      }
      if (numBlocks > 0 || buffer.numRows() > 0.8 * writer.blocksize()) {
        addBlock();
        // The last argument is the number of distinct elements in `col1`. We
        // store a dummy value here that we can check later.
        metaData.push_back(writer.finishLargeRelation(i + 1));
      } else {
        metaData.push_back(writer.addSmallRelation(V(input.col0_), i + 1,
                                                   buffer.asStaticView<0>()));
      }
      buffer.clear();
      numBlocks = 0;
      ++i;
    }
  }
  auto blocks = std::move(writer).getFinishedBlocks();
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

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  // Check the contents of the metadata.

  auto cleanup = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [&filename] { ad_utility::deleteFile(filename); });
  CompressedRelationReader reader{ad_utility::makeUnlimitedAllocator<Id>(),
                                  ad_utility::File{filename, "r"}};
  // TODO<C++23> `std::ranges::to<vector>`.
  std::vector<ColumnIndex> additionalColumns;
  std::ranges::copy(std::views::iota(2ul, getNumColumns(inputs)),
                    std::back_inserter(additionalColumns));
  for (size_t i = 0; i < metaData.size(); ++i) {
    const auto& m = metaData[i];
    ASSERT_EQ(V(inputs[i].col0_), m.col0Id_);
    ASSERT_EQ(inputs[i].col1And2_.size(), m.numRows_);
    // The number of distinct elements in `col1` was passed in as `i + 1` for
    // testing purposes, so this is the expected multiplicity.
    ASSERT_FLOAT_EQ(m.numRows_ / static_cast<float>(i + 1),
                    m.multiplicityCol1_);
    // Scan for all distinct `col0` and check that we get the expected result.
    IdTable table = reader.scan(metaData[i], std::nullopt, blocks,
                                additionalColumns, cancellationHandle);
    const auto& col1And2 = inputs[i].col1And2_;
    checkThatTablesAreEqual(col1And2, table);

    table.clear();
    for (const auto& block :
         reader.lazyScan(metaData[i], std::nullopt, blocks, additionalColumns,
                         cancellationHandle)) {
      table.insertAtEnd(block.begin(), block.end());
    }
    checkThatTablesAreEqual(col1And2, table);

    // Check for all distinct combinations of `(col0, col1)` and check that
    // we get the expected result.
    // TODO<joka921>, C++23 use views::chunk_by
    int lastCol1Id = col1And2[0][0];
    std::vector<std::array<int, 1>> col3;

    auto scanAndCheck = [&]() {
      auto size =
          reader.getResultSizeOfScan(metaData[i], V(lastCol1Id), blocks);
      IdTable tableWidthOne =
          reader.scan(metaData[i], V(lastCol1Id), blocks,
                      Permutation::ColumnIndicesRef{}, cancellationHandle);
      ASSERT_EQ(tableWidthOne.numColumns(), 1);
      EXPECT_EQ(size, tableWidthOne.numRows());
      checkThatTablesAreEqual(col3, tableWidthOne);
      tableWidthOne.clear();
      for (const auto& block :
           reader.lazyScan(metaData[i], V(lastCol1Id), blocks,
                           Permutation::ColumnIndices{}, cancellationHandle)) {
        tableWidthOne.insertAtEnd(block.begin(), block.end());
      }
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
}

namespace {

// Run `testCompressedRelations` (see above) for the given `inputs` and
// `testCaseName`, but with a set of different `blocksizes` (small and medium
// size, powers of two and odd), to find subtle rounding bugs when creating the
// blocks.
void testWithDifferentBlockSizes(const std::vector<RelationInput>& inputs,
                                 std::string testCaseName) {
  testCompressedRelations(inputs, testCaseName, 19_B);
  testCompressedRelations(inputs, testCaseName, 237_B);
  testCompressedRelations(inputs, testCaseName, 4096_B);
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
    std::vector<RowInput> col1And2;
    for (int j = 0; j < 200; ++j) {
      col1And2.push_back({i * j, i * j + 3});
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
    std::vector<RowInput> col1And2;
    for (int j = 0; j < 200; ++j) {
      col1And2.push_back({i * 12, i * j + 3});
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
      std::vector<RowInput> col1And2;
      for (int j = 0; j < 50; ++j) {
        col1And2.push_back({i * 12, i * j + 3});
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
      std::vector<RowInput> col1And2;
      for (int j = 0; j < 80; ++j) {
        col1And2.push_back({i * j + y, i * j + 3});
      }
      inputs.push_back(RelationInput{i + (y * 300), std::move(col1And2)});
    }
  }
  testWithDifferentBlockSizes(inputs, "mixedSizes");
}

TEST(CompressedRelationWriter, AdditionalColumns) {
  std::vector<RelationInput> inputs;
  for (int y = 0; y < 3; ++y) {
    // First some large relations with many duplicates in `col1`.
    for (int i = 1; i < 6; ++i) {
      std::vector<RowInput> col1And2;
      for (int j = 0; j < 50; ++j) {
        col1And2.push_back({i * 12, i * j + 3});
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
      std::vector<RowInput> col1And2;
      for (int j = 0; j < 80; ++j) {
        col1And2.push_back({i * j + y, i * j + 3});
      }
      inputs.push_back(RelationInput{i + (y * 300), std::move(col1And2)});
    }
  }

  // Add two separate columns.
  for (auto& relation : inputs) {
    for (auto& row : relation.col1And2_) {
      row.push_back(row.at(0) + 42);
      row.push_back(row.at(1) * 42);
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
  ASSERT_FLOAT_EQ(2.0f, m.multiplicityCol1_);
  m.setCol2Multiplicity(1.0f);
  ASSERT_FLOAT_EQ(1.0f, m.multiplicityCol2_);
  ASSERT_FLOAT_EQ(1.0f, m.getCol2Multiplicity());
  ASSERT_FALSE(m.isFunctional());
  m.setCol1Multiplicity(1.0f);
  ASSERT_TRUE(m.isFunctional());
  m.numRows_ = 43;
  ASSERT_EQ(43, m.numRows_);
}

TEST(CompressedRelationReader, getBlocksForJoinWithColumn) {
  CompressedBlockMetadata block1{
      {}, 0, {V(16), V(0), V(0)}, {V(38), V(4), V(12)}};
  CompressedBlockMetadata block2{
      {}, 0, {V(42), V(3), V(0)}, {V(42), V(4), V(12)}};
  CompressedBlockMetadata block3{
      {}, 0, {V(42), V(4), V(13)}, {V(42), V(6), V(9)}};

  // We are only interested in blocks with a col0 of `42`.
  CompressedRelationMetadata relation;
  relation.col0Id_ = V(42);

  std::vector blocks{block1, block2, block3};
  CompressedRelationReader::MetadataAndBlocks metadataAndBlocks{
      relation, blocks, std::nullopt, std::nullopt};

  auto test = [&metadataAndBlocks](
                  const std::vector<Id>& joinColumn,
                  const std::vector<CompressedBlockMetadata>& expectedBlocks,
                  source_location l = source_location::current()) {
    auto t = generateLocationTrace(l);
    auto result = CompressedRelationReader::getBlocksForJoin(joinColumn,
                                                             metadataAndBlocks);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expectedBlocks));
  };
  // We have fixed the `col0Id` to be 42. The col1/2Ids of the matching blocks
  // are as follows (starting at `block2`)
  // [(3, 0)-(4, 12)], [(4, 13)-(6, 9)]

  // Tests for a fixed col0Id, so the join is on the middle column.
  test({V(1), V(3), V(17), V(29)}, {block2});
  test({V(2), V(3), V(4), V(5)}, {block2, block3});
  test({V(4)}, {block2, block3});
  test({V(6)}, {block3});

  // Test with a fixed col1Id. We now join on the last column, the first column
  // is fixed (42), and the second column is also fixed (4).
  metadataAndBlocks.col1Id_ = V(4);
  test({V(11), V(27), V(30)}, {block2, block3});
  test({V(12)}, {block2});
  test({V(13)}, {block3});
}
TEST(CompressedRelationReader, getBlocksForJoin) {
  CompressedBlockMetadata block1{
      {}, 0, {V(16), V(0), V(0)}, {V(38), V(4), V(12)}};
  CompressedBlockMetadata block2{
      {}, 0, {V(42), V(3), V(0)}, {V(42), V(4), V(12)}};
  CompressedBlockMetadata block3{
      {}, 0, {V(42), V(5), V(13)}, {V(42), V(8), V(9)}};
  CompressedBlockMetadata block4{
      {}, 0, {V(42), V(8), V(16)}, {V(42), V(20), V(9)}};
  CompressedBlockMetadata block5{
      {}, 0, {V(42), V(20), V(16)}, {V(42), V(20), V(63)}};

  // We are only interested in blocks with a col0 of `42`.
  CompressedRelationMetadata relation;
  relation.col0Id_ = V(42);

  std::vector blocks{block1, block2, block3, block4, block5};
  CompressedRelationReader::MetadataAndBlocks metadataAndBlocks{
      relation, blocks, std::nullopt, std::nullopt};

  CompressedBlockMetadata blockB1{
      {}, 0, {V(16), V(0), V(0)}, {V(38), V(4), V(12)}};
  CompressedBlockMetadata blockB2{
      {}, 0, {V(47), V(3), V(0)}, {V(47), V(6), V(12)}};
  CompressedBlockMetadata blockB3{
      {}, 0, {V(47), V(7), V(13)}, {V(47), V(9), V(9)}};
  CompressedBlockMetadata blockB4{
      {}, 0, {V(47), V(38), V(7)}, {V(47), V(38), V(8)}};
  CompressedBlockMetadata blockB5{
      {}, 0, {V(47), V(38), V(9)}, {V(47), V(38), V(12)}};
  CompressedBlockMetadata blockB6{
      {}, 0, {V(47), V(38), V(13)}, {V(47), V(38), V(15)}};

  // We are only interested in blocks with a col0 of `42`.
  CompressedRelationMetadata relationB;
  relationB.col0Id_ = V(47);

  std::vector blocksB{blockB1, blockB2, blockB3, blockB4, blockB5, blockB6};
  CompressedRelationReader::MetadataAndBlocks metadataAndBlocksB{
      relationB, blocksB, std::nullopt, std::nullopt};

  auto test = [&metadataAndBlocks, &metadataAndBlocksB](
                  const std::array<std::vector<CompressedBlockMetadata>, 2>&
                      expectedBlocks,
                  source_location l = source_location::current()) {
    auto t = generateLocationTrace(l);
    auto result = CompressedRelationReader::getBlocksForJoin(
        metadataAndBlocks, metadataAndBlocksB);
    EXPECT_THAT(result[0], ::testing::ElementsAreArray(expectedBlocks[0]));
    EXPECT_THAT(result[1], ::testing::ElementsAreArray(expectedBlocks[1]));

    result = CompressedRelationReader::getBlocksForJoin(metadataAndBlocksB,
                                                        metadataAndBlocks);
    EXPECT_THAT(result[1], ::testing::ElementsAreArray(expectedBlocks[0]));
    EXPECT_THAT(result[0], ::testing::ElementsAreArray(expectedBlocks[1]));
  };

  // We have fixed the `col0Id` to be 42 for the left input and 47 for the right
  // input. The col1/2Ids of the blocks that have this col0Id are as follows:

  // (starting at `block2`.
  // [(3, 0)- (4, 12)], [(5, 13) - (8, 9)], [(8, 16) - (20, 9)], [(20, 16) -
  // (20, 63)]

  // Starting at `blockB2`.
  // [(3, 0)-(6, 12)], [(7, 13)-(9, 9)], [(38, 7)-(38, 8)], [(38, 9)-(38, 12)],
  // [(38, 13)-(38, 15)]

  // Test for only the `col0Id` fixed.
  test({std::vector{block2, block3, block4}, std::vector{blockB2, blockB3}});
  // Test with a fixed col1Id on both sides. We now join on the last column.
  metadataAndBlocks.col1Id_ = V(20);
  metadataAndBlocksB.col1Id_ = V(38);
  test({std::vector{block4}, std::vector{blockB4, blockB5}});

  // Fix only the col1Id of the left input.
  metadataAndBlocks.col1Id_ = V(4);
  metadataAndBlocksB.col1Id_ = std::nullopt;
  test({std::vector{block2}, std::vector{blockB2, blockB3}});

  // Fix only the col1Id of the right input.
  metadataAndBlocks.col1Id_ = std::nullopt;
  metadataAndBlocksB.col1Id_ = V(7);
  test({std::vector{block4, block5}, std::vector{blockB3}});
}

TEST(CompressedRelationReader, PermutedTripleToString) {
  auto tr = CompressedBlockMetadata::PermutedTriple{V(12), V(13), V(27)};
  std::stringstream str;
  str << tr;
  ASSERT_EQ(str.str(), "Triple: VocabIndex:12 VocabIndex:13 VocabIndex:27\n");
}
