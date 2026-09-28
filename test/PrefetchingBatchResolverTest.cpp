// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "engine/PrefetchingBatchResolver.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/VocabIndex.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/CompactStringVector.h"
#include "util/GTestHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/SoftwarePrefetch.h"

using namespace ql::engine::prefetch;
using namespace std::string_literals;

namespace {

TEST(PrefetchingBatchResolver, PrefetchIntrinsicSmokeTest) {
  int x = 42;
  // Verify prefetch does not fault on null or valid pointers
  EXPECT_NO_THROW(prefetchVocabEntry(nullptr));
  EXPECT_NO_THROW(prefetchVocabEntry(&x));
  EXPECT_NO_THROW(prefetchVocabEntry(&x, 16));
  EXPECT_NO_THROW(prefetchAddress(&x));
}

TEST(PrefetchingBatchResolver, EquivalenceWithStandardBatchResolution) {
  std::string kg =
      "<s> <p> \"first\" . <s> <p> \"second\" . <s> <p> \"third\" . <s> <p> "
      "\"fourth\" . <s> <p> 123 . <s> <p> <http://example.org/resource> .";
  auto qec = ad_utility::testing::getQec(kg);
  const auto& index = qec->getIndex();
  auto getId = ad_utility::testing::makeGetId(index);

  std::vector<Id> testIds = {getId("\"first\""),
                             getId("<s>"),
                             ad_utility::testing::IntId(123),
                             getId("\"second\""),
                             getId("<http://example.org/resource>"),
                             getId("\"third\""),
                             getId("\"fourth\""),
                             ad_utility::testing::UndefId()};

  LocalVocab localVocab;

  // Standard baseline resolution
  auto baselineResults = ql::exportIds::idsToStringAndType(
      index, testIds, localVocab, ql::identity{});

  // Prefetched resolution with various prefetch distances
  for (size_t distance : {1, 2, 4, 8, 16}) {
    PrefetchingBatchResolver resolver(PrefetchConfig{distance});
    auto prefetchedResults =
        resolver.idsToStringAndType(index, testIds, localVocab, ql::identity{});

    ASSERT_EQ(baselineResults.size(), prefetchedResults.size());
    for (size_t i = 0; i < baselineResults.size(); ++i) {
      EXPECT_EQ(baselineResults[i], prefetchedResults[i])
          << "Mismatch at index " << i << " with prefetch distance "
          << distance;
    }
  }
}

TEST(PrefetchingBatchResolver, CompactVectorPipelinedResolution) {
  CompactVectorOfStrings<char> words;
  std::vector<std::string> rawWords = {"<iri1>", "<iri2>", "\"literal1\"",
                                       "\"literal2\"", "\"longer_literal_3\""};
  words.build(rawWords);

  PrefetchingBatchResolver resolver(PrefetchConfig{4});

  std::vector<size_t> queryIndices = {0, 4, 1, 3, 2, 4, 0, 1};
  std::vector<std::string> resolvedWords(queryIndices.size());

  resolver.resolveCompactVectorPipelined(
      words, queryIndices,
      [&resolvedWords](size_t slot, size_t, std::string_view view) {
        resolvedWords[slot] = std::string(view);
      });

  for (size_t i = 0; i < queryIndices.size(); ++i) {
    EXPECT_EQ(resolvedWords[i], rawWords[queryIndices[i]]);
  }
}

TEST(PrefetchingBatchResolver, EmptyAndBoundaryInputs) {
  std::string kg = "<s> <p> <o> .";
  auto qec = ad_utility::testing::getQec(kg);
  const auto& index = qec->getIndex();

  PrefetchingBatchResolver resolver;
  LocalVocab localVocab;

  // Empty IDs span
  auto emptyResults =
      resolver.idsToStringAndType(index, ql::span<const Id>{}, localVocab);
  EXPECT_TRUE(emptyResults.empty());

  // Empty positions: the resolver intentionally returns early and resolves
  // nothing, so this must not throw even though `results` is smaller than
  // `ids` (the size contract only applies to non-empty position spans).
  std::vector<std::optional<std::pair<std::string, const char*>>> results(1);
  std::vector<Id> ids = {ad_utility::testing::IntId(1)};
  EXPECT_NO_THROW(resolver.resolveVocabIndexIds(
      index, ids, ql::span<const size_t>{}, results));
}

// _____________________________________________________________________________
// Boundary cases of `ad_utility::forEachWordPrefetched`: prefetch distances of
// zero, larger than the input and `SIZE_MAX` (the look-ahead guards must not
// wrap), a vector that was never built (no callback), and out-of-range indices
// including `SIZE_MAX` (the correctness check must fire before any access).
TEST(PrefetchingBatchResolver, ForEachWordPrefetchedBoundaries) {
  CompactVectorOfStrings<char> words;
  const std::vector<std::string> rawWords{"a", "bb", "", "dddd"};
  words.build(rawWords);
  const std::vector<size_t> indices{3, 2, 1, 0, 3};

  for (size_t distance :
       {size_t{0}, size_t{1}, indices.size(), indices.size() + 1, SIZE_MAX}) {
    std::vector<std::string> resolved;
    ad_utility::forEachWordPrefetched(
        words, indices, distance,
        [&resolved, &indices](size_t slot, size_t idx, std::string_view w) {
          EXPECT_EQ(slot, resolved.size());
          EXPECT_EQ(idx, indices[slot]);
          resolved.emplace_back(w);
        });
    ASSERT_EQ(resolved.size(), indices.size()) << "distance " << distance;
    for (size_t i = 0; i < indices.size(); ++i) {
      EXPECT_EQ(resolved[i], rawWords[indices[i]]);
    }
  }

  size_t numCalls = 0;
  auto count = [&numCalls](size_t, size_t, std::string_view) { ++numCalls; };
  ad_utility::forEachWordPrefetched(CompactVectorOfStrings<char>{}, indices, 8,
                                    count);
  ad_utility::forEachWordPrefetched(words, ql::span<const size_t>{}, 8, count);
  EXPECT_EQ(numCalls, 0u);

  for (size_t badIndex : {rawWords.size(), size_t{SIZE_MAX}}) {
    const std::vector<size_t> bad{0, badIndex};
    AD_EXPECT_THROW_WITH_MESSAGE(
        ad_utility::forEachWordPrefetched(words, bad, 8, count),
        ::testing::HasSubstr("curIdx"));
  }
}

}  // namespace
