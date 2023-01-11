//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/QueryExecutionContext.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"

// Several useful functions to quickly set up an `Index` and a
// `QueryExecutionContext` that store a small example knowledge graph. Those can
// be used for unit tests.

namespace ad_utility::testing {
// Create an empty `Index` object that has certain default settings overwritten
// such that very small indices, as they are typically used for unit tests,
// can be built without a lot of time and memory overhead.
inline Index makeIndexWithTestSettings() {
  Index index;
  index.setNumTriplesPerBatch(2);
  index.stxxlMemoryInBytes() = 1024ul * 1024ul * 50;
  return index;
}

// Create an `Index` from the given `turtleInput`. If the `turtleInput` is not
// specified, a default input will be used and the resulting index will have the
// following properties: Its vocabulary contains the literals `"alpha",
// "älpha", "A", "Beta"`. These vocabulary entries are expected by the tests
// for the subclasses of `SparqlExpression`.
// The concrete triple contents are currently used in `GroupByTest.cpp`.
inline Index makeTestIndex(std::string turtleInput = "") {
  // Ignore the (irrelevant) log output of the index building and loading during
  // these tests.
  static std::ostringstream ignoreLogStream;
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  std::string filename = "relationalExpressionTestIndex.ttl";
  if (turtleInput.empty()) {
    turtleInput =

        "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . "
        "<x> "
        "<label> \"Beta\". <x> <is-a> <y>. <y> <is-a> <x>. <z> <label> "
        "\"zz\"@en";
  }

  FILE_BUFFER_SIZE() = 1000;
  std::fstream f(filename, std::ios_base::out);
  f << turtleInput;
  f.close();
  std::string indexBasename = "_staticGlobalTestIndex";
  {
    Index index = makeIndexWithTestSettings();
    index.setOnDiskBase(indexBasename);
    index.createFromFile<TurtleParserAuto>(filename);
  }
  Index index;
  index.createFromOnDiskIndex(indexBasename);
  return index;
}

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex()` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaulted.
static inline QueryExecutionContext* getQec(std::string turtleInput = "") {
  static ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)};
  static ad_utility::HashMap<std::string, Index> turtleToIndexMap;
  static ad_utility::HashMap<std::string, QueryExecutionContext> turtleToContextMap;
  static ad_utility::HashMap<std::string, std::unique_ptr<QueryResultCache>>
      turtleToCacheMap;
  static const Engine engine{};
  if (!turtleToIndexMap.contains(turtleInput)) {
    AD_CHECK(!turtleToContextMap.contains(turtleInput));
    turtleToIndexMap.emplace(turtleInput, makeTestIndex(turtleInput));
    turtleToCacheMap.emplace(turtleInput, std::make_unique<QueryResultCache>());
    turtleToContextMap.emplace(
        turtleInput,
        QueryExecutionContext{
            turtleToIndexMap.at(turtleInput),
            engine,
            turtleToCacheMap.at(turtleInput).get(),
            ad_utility::AllocatorWithLimit<Id>{
                ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)},
            {}});
  }
  return &turtleToContextMap.at(turtleInput);
}

// Create an unlimited allocator.
ad_utility::AllocatorWithLimit<Id>& makeAllocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

}  // namespace ad_utility::testing
