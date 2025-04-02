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

// Create an `Index` from the given `turtleInput`. If the `turtleInput` is not
// specified, a default input will be used and the resulting index will have the
// following properties: Its vocabulary contains the literals `"alpha",
// "älpha", "A", "Beta"`. These vocabulary entries are expected by the tests
// for the subclasses of `SparqlExpression`.
// The concrete triple contents are currently used in `GroupByTest.cpp`. Using
// the parameter parserBufferSize the buffer size can be increased, when needed
// for larger tests (like polygon testing in Spatial Joins).
Index makeTestIndex(
    const std::string& indexBasename,
    std::optional<std::string> turtleInput = std::nullopt,
    bool loadAllPermutations = true, bool usePatterns = true,
    bool usePrefixCompression = true,
    ad_utility::MemorySize blocksizePermutations = 16_B,
    bool createTextIndex = false, bool addWordsFromLiterals = true,
    std::optional<std::pair<std::string, std::string>>
        contentsOfWordsFileAndDocsfile = std::nullopt,
    ad_utility::MemorySize parserBufferSize = 1_kB,
    std::optional<TextScoringMetric> scoringMetric = std::nullopt,
    std::optional<std::pair<float, float>> bAndKParam = std::nullopt,
    qlever::Filetype indexType = qlever::Filetype::Turtle);

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaulted. Using
// the parameter parserBufferSize the buffer size can be increased, when needed
// for larger tests (like polygon testing in Spatial Joins).
QueryExecutionContext* getQec(
    std::optional<std::string> turtleInput = std::nullopt,
    bool loadAllPermutations = true, bool usePatterns = true,
    bool usePrefixCompression = true,
    ad_utility::MemorySize blocksizePermutations = 16_B,
    bool createTextIndex = false, bool addWordsFromLiterals = true,
    std::optional<std::pair<std::string, std::string>>
        contentsOfWordsFileAndDocsfile = std::nullopt,
    ad_utility::MemorySize parserBufferSize = 1_kB,
    std::optional<TextScoringMetric> scoringMetric = std::nullopt,
    std::optional<std::pair<float, float>> bAndKParam = std::nullopt,
    qlever::Filetype indexType = qlever::Filetype::Turtle);

// Return a lambda that takes a string and converts it into an ID by looking
// it up in the vocabulary of `index`. An `AD_CONTRACT_CHECK` will fail if the
// string cannot be found in the vocabulary.
std::function<Id(const std::string&)> makeGetId(const Index& index);

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_INDEXTESTHELPERS_H
