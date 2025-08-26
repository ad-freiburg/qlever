//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_INDEXTESTHELPERS_H
#define QLEVER_TEST_UTIL_INDEXTESTHELPERS_H

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include "AllocatorTestHelpers.h"
#include "engine/QueryExecutionContext.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/EncodedIriManager.h"
#include "index/Index.h"
#include "util/MemorySize/MemorySize.h"

// Several useful functions to quickly set up an `Index` and a
// `QueryExecutionContext` that store a small example knowledge graph. Those can
// be used for unit tests.

namespace ad_utility::testing {
// Create an empty `Index` object that has certain default settings overwritten
// such that very small indices, as they are typically used for unit tests,
// can be built without a lot of time and memory overhead. Using the parameter
// parserBufferSize the buffer size can be increased, when needed for larger
// tests (like polygon testing in Spatial Joins).
Index makeIndexWithTestSettings(ad_utility::MemorySize parserBufferSize = 1_kB);

// Get names of all index files for a given basename. Needed for cleaning up
// after tests using a test index.
//
// TODO: A better approach would be if the `Index` class itself kept track of
// the files it creates and provides a member function to obtain all their
// names. But for now this is good enough (and better then what we had before
// when the files were not deleted after the test).
std::vector<std::string> getAllIndexFilenames(const std::string& indexBasename);

// A struct, that defines the configuration of an index, and after that,
// a function that takes this struct and creates an index from it.

struct TestIndexConfig {
  // A turtle string, from which the index is built. If `nullopt`, a default
  // input will be used and the resulting index will have the following
  // properties: Its vocabulary contains the literals `"alpha", "älpha", "A",
  // "Beta"`. These vocabulary entries are expected by the tests for the
  // subclasses of `SparqlExpression`. The concrete triple contents are
  // currently used in `GroupByTest.cpp`.
  std::optional<std::string> turtleInput = std::nullopt;
  bool loadAllPermutations = true;
  bool usePatterns = true;
  bool usePrefixCompression = true;
  ad_utility::MemorySize blocksizePermutations = 16_B;
  bool createTextIndex = false;
  bool addWordsFromLiterals = true;
  std::optional<std::pair<std::string, std::string>>
      contentsOfWordsFileAndDocsfile = std::nullopt;
  // The following buffer size can be increased, if larger triples are to be
  // parsed
  //(like large geometry literals for testing spatial operations).
  ad_utility::MemorySize parserBufferSize = 1_kB;
  std::optional<TextScoringMetric> scoringMetric = std::nullopt;
  std::optional<std::pair<float, float>> bAndKParam = std::nullopt;
  qlever::Filetype indexType = qlever::Filetype::Turtle;
  std::optional<VocabularyType> vocabularyType = std::nullopt;
  std::optional<EncodedIriManager> encodedIriManager = std::nullopt;

  // A very typical use case is to only specify the turtle input, and leave all
  // the other members as the default. We therefore have a dedicated constructor
  // for this case.
  TestIndexConfig() = default;
  explicit TestIndexConfig(std::string turtleKgInput)
      : turtleInput{std::move(turtleKgInput)} {}

  // Hashing.
  template <typename H>
  friend H AbslHashValue(H h, const TestIndexConfig& c) {
    return H::combine(std::move(h), c.turtleInput, c.loadAllPermutations,
                      c.usePatterns, c.usePrefixCompression,
                      c.blocksizePermutations, c.createTextIndex,
                      c.addWordsFromLiterals, c.contentsOfWordsFileAndDocsfile,
                      c.parserBufferSize, c.scoringMetric, c.bAndKParam,
                      c.indexType, c.encodedIriManager);
  }
  bool operator==(const TestIndexConfig&) const = default;
};

// Create a test index at the given `indexBasename` and with the given `config`.
Index makeTestIndex(const std::string& indexBasename, TestIndexConfig config);

// Create a test index at the given `indexBasename` and with the given `turtle`
// as input, leave all other settings at the default.
Index makeTestIndex(const std::string& indexBasename, std::string turtle);

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaulted.
QueryExecutionContext* getQec(TestIndexConfig config);

// Overload of `getQec` for the simple case where we only care about the turtle
// input. All other settings are left at their default values.
QueryExecutionContext* getQec(
    std::optional<std::string> turtleInput = std::nullopt,
    std::optional<VocabularyType> vocabularyType = std::nullopt);

// Return a lambda that takes a string and converts it into an ID by looking
// it up in the vocabulary of `index`. An `AD_CONTRACT_CHECK` will fail if the
// string cannot be found in the vocabulary.
std::function<Id(const std::string&)> makeGetId(const Index& index);

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_INDEXTESTHELPERS_H
