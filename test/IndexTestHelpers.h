//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <gtest/gtest.h>

#include "./util/AllocatorTestHelpers.h"
#include "absl/cleanup/cleanup.h"
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
// can be built without a lot of time and memory overhead.
Index makeIndexWithTestSettings();

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
// The concrete triple contents are currently used in `GroupByTest.cpp`.
Index makeTestIndex(const std::string& indexBasename,
                    std::optional<std::string> turtleInput = std::nullopt,
                    bool loadAllPermutations = true, bool usePatterns = true,
                    bool usePrefixCompression = true,
                    ad_utility::MemorySize blocksizePermutations = 16_B);

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaulted.
QueryExecutionContext* getQec(
    std::optional<std::string> turtleInput = std::nullopt,
    bool loadAllPermutations = true, bool usePatterns = true,
    bool usePrefixCompression = true,
    ad_utility::MemorySize blocksizePermutations = 16_B);

// Return a lambda that takes a string and converts it into an ID by looking
// it up in the vocabulary of `index`. An `AD_CONTRACT_CHECK` will fail if the
// string cannot be found in the vocabulary.
std::function<Id(const std::string&)> makeGetId(const Index& index);

}  // namespace ad_utility::testing
