// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/sparqlExpressions/SetOfIntervals.h"
#include "engine/sparqlExpressions/SparqlExpressionGeneratorSparse.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/AllocatorTestHelpers.h"
#include "util/GTestHelpers.h"

using ::testing::ElementsAre;

using namespace sparqlExpression::detail;

using ad_utility::SetOfIntervals;
using sparqlExpression::VectorWithMemoryLimit;

namespace {}  // namespace

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, EmptyIndices) {
  std::vector<Id> valuesVec = {Id::makeFromInt(0), Id::makeFromInt(1),
                               Id::makeFromInt(2)};
  VectorWithMemoryLimit<Id> values(valuesVec.begin(), valuesVec.end(),
                                   ad_utility::testing::makeAllocator());
  std::vector<size_t> indices;

  auto generator = makeGeneratorSparse(values, values.size(), nullptr, indices,
                                       ql::identity{});

  std::vector<Id> result;
  ql::ranges::copy(generator, std::back_inserter(result));
  EXPECT_TRUE(result.empty());
}

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, SingleIndex) {
  std::vector<Id> valuesVec = {Id::makeFromInt(0), Id::makeFromInt(1),
                               Id::makeFromInt(2)};
  VectorWithMemoryLimit<Id> values(valuesVec.begin(), valuesVec.end(),
                                   ad_utility::testing::makeAllocator());
  std::vector<size_t> indices = {1};

  auto generator = makeGeneratorSparse(values, values.size(), nullptr, indices,
                                       ql::identity{});

  std::vector<Id> result;
  ql::ranges::copy(generator, std::back_inserter(result));
  ASSERT_THAT(result, ElementsAre(values[1]));
}

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, MultipleIndices) {
  std::vector<Id> valuesVec = {Id::makeFromInt(0), Id::makeFromInt(1),
                               Id::makeFromInt(2), Id::makeFromInt(3)};
  VectorWithMemoryLimit<Id> values(valuesVec.begin(), valuesVec.end(),
                                   ad_utility::testing::makeAllocator());
  std::vector<size_t> indices = {0, 2, 3};

  auto generator = makeGeneratorSparse(values, values.size(), nullptr, indices,
                                       ql::identity{});

  std::vector<Id> result;
  ql::ranges::copy(generator, std::back_inserter(result));
  ASSERT_THAT(result, ElementsAre(values[0], values[2], values[3]));
}

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, TransformationIsApplied) {
  std::vector<Id> valuesVec = {Id::makeFromInt(1), Id::makeFromInt(2),
                               Id::makeFromInt(3)};
  VectorWithMemoryLimit<Id> values(valuesVec.begin(), valuesVec.end(),
                                   ad_utility::testing::makeAllocator());
  std::vector<size_t> indices = {0, 2};

  auto transformation = [](const Id& id) {
    return Id::makeFromInt(id.getInt() * 10);
  };

  auto generator = makeGeneratorSparse(values, values.size(), nullptr, indices,
                                       transformation);

  std::vector<Id> result;
  ql::ranges::copy(generator, std::back_inserter(result));

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].getInt(), 10);
  EXPECT_EQ(result[1].getInt(), 30);
}

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, DuplicateIndicesAreAllowed) {
  std::vector<Id> valuesVec = {Id::makeFromInt(0), Id::makeFromInt(1),
                               Id::makeFromInt(2)};
  VectorWithMemoryLimit<Id> values(valuesVec.begin(), valuesVec.end(),
                                   ad_utility::testing::makeAllocator());
  // Duplicate index 1 should yield the corresponding value twice.
  std::vector<size_t> indices = {1, 1, 2};

  auto generator = makeGeneratorSparse(values, values.size(), nullptr, indices,
                                       ql::identity{});

  std::vector<Id> result;
  ql::ranges::copy(generator, std::back_inserter(result));
  ASSERT_THAT(result, ElementsAre(values[1], values[1], values[2]));
}

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, UnsortedIndicesTriggerContractCheck) {
  std::vector<Id> valuesVec = {Id::makeFromInt(0), Id::makeFromInt(1),
                               Id::makeFromInt(2)};
  VectorWithMemoryLimit<Id> values(valuesVec.begin(), valuesVec.end(),
                                   ad_utility::testing::makeAllocator());

  // Indices must be sorted in ascending order. This violates the
  // precondition and should trigger an `AD_CONTRACT_CHECK`.
  std::vector<size_t> indices = {1, 0};

  auto buildAndConsumeGenerator = [&]() {
    auto generator = makeGeneratorSparse(values, values.size(), nullptr,
                                         indices, ql::identity{});
    std::vector<Id> result;
    ql::ranges::copy(generator, std::back_inserter(result));
  };

  AD_EXPECT_THROW_WITH_MESSAGE(
      buildAndConsumeGenerator(),
      ::testing::HasSubstr("currentTarget >= previousTarget"));
}

// _____________________________________________________________________________
TEST(SparqlExpressionGeneratorSparse, WorksWithSetOfIntervals) {
  // A nontrivial `SetOfIntervals` is also a valid `SingleExpressionResult`.
  // It is expanded to a sequence of boolean IDs of length `numItems`.
  SetOfIntervals intervals{{{1, 3}, {4, 5}}};
  const size_t numItems = 7;

  // Materialize the dense boolean result and then compare the sparse
  // selection against the corresponding subset.
  auto denseBoolVector = SetOfIntervals::toBitVector(intervals, numItems);

  std::vector<size_t> indices = {0, 1, 2, 4, 6};

  auto generator = makeGeneratorSparse(intervals, numItems, nullptr, indices,
                                       ql::identity{});

  std::vector<Id> result;
  ql::ranges::copy(generator, std::back_inserter(result));

  ASSERT_EQ(result.size(), indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    const auto index = indices[i];
    ASSERT_EQ(result[i].getDatatype(), Datatype::Bool);
    EXPECT_EQ(result[i].getBool(), denseBoolVector[index]);
  }
}
