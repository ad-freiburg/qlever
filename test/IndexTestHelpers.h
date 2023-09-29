//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

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
inline Index makeIndexWithTestSettings() {
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.setNumTriplesPerBatch(2);
  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  index.stxxlMemory() = 50_MB;
  return index;
}

// Get names of all index files for a given basename. Needed for cleaning up
// after tests using a test index.
//
// TODO: A better approach would be if the `Index` class itself kept track of
// the files it creates and provides a member function to obtain all their
// names. But for now this is good enough (and better then what we had before
// when the files were not deleted after the test).
inline std::vector<std::string> getAllIndexFilenames(
    const std::string indexBasename) {
  return {indexBasename + ".ttl",
          indexBasename + ".index.pos",
          indexBasename + ".index.pso",
          indexBasename + ".index.sop",
          indexBasename + ".index.sop.meta",
          indexBasename + ".index.spo",
          indexBasename + ".index.spo.meta",
          indexBasename + ".index.ops",
          indexBasename + ".index.ops.meta",
          indexBasename + ".index.osp",
          indexBasename + ".index.osp.meta",
          indexBasename + ".index.patterns",
          indexBasename + ".meta-data.json",
          indexBasename + ".prefixes",
          indexBasename + ".vocabulary.internal",
          indexBasename + ".vocabulary.external",
          indexBasename + ".vocabulary.external.idsAndOffsets.mmap"};
}

// Create an `Index` from the given `turtleInput`. If the `turtleInput` is not
// specified, a default input will be used and the resulting index will have the
// following properties: Its vocabulary contains the literals `"alpha",
// "älpha", "A", "Beta"`. These vocabulary entries are expected by the tests
// for the subclasses of `SparqlExpression`.
// The concrete triple contents are currently used in `GroupByTest.cpp`.
inline Index makeTestIndex(
    const std::string& indexBasename,
    std::optional<std::string> turtleInput = std::nullopt,
    bool loadAllPermutations = true, bool usePatterns = true,
    bool usePrefixCompression = true,
    size_t blocksizePermutationsInBytes = 32) {
  // Ignore the (irrelevant) log output of the index building and loading during
  // these tests.
  static std::ostringstream ignoreLogStream;
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  std::string inputFilename = indexBasename + ".ttl";
  if (!turtleInput.has_value()) {
    turtleInput =
        "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . "
        "<x> "
        "<label> \"Beta\". <x> <is-a> <y>. <y> <is-a> <x>. <z> <label> "
        "\"zz\"@en";
  }

  FILE_BUFFER_SIZE() = 1000;
  std::fstream f(inputFilename, std::ios_base::out);
  f << turtleInput.value();
  f.close();
  {
    Index index = makeIndexWithTestSettings();
    // This is enough for 2 triples per block. This is deliberately chosen as a
    // small value, s.t. the tiny knowledge graphs from unit tests also contain
    // multiple blocks. Should this value or the semantics of it (how many
    // triples it may store) ever change, then some unit tests might have to be
    // adapted.
    index.blocksizePermutationsInBytes() = blocksizePermutationsInBytes;
    index.setOnDiskBase(indexBasename);
    index.setUsePatterns(usePatterns);
    index.setPrefixCompression(usePrefixCompression);
    index.createFromFile(inputFilename);
  }
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.setUsePatterns(usePatterns);
  index.setLoadAllPermutations(loadAllPermutations);
  index.createFromOnDiskIndex(indexBasename);
  ad_utility::setGlobalLoggingStream(&std::cout);
  return index;
}

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaulted.
inline QueryExecutionContext* getQec(
    std::optional<std::string> turtleInput = std::nullopt,
    bool loadAllPermutations = true, bool usePatterns = true,
    bool usePrefixCompression = true,
    size_t blocksizePermutationsInBytes = 32) {
  // Similar to `absl::Cleanup`. Calls the `callback_` in the destructor, but
  // the callback is stored as a `std::function`, which allows to store
  // different types of callbacks in the same wrapper type.
  // TODO<joka921> RobinTF has a similar tool in the pipeline that can be used
  // here.
  struct TypeErasedCleanup {
    std::function<void()> callback_;
    ~TypeErasedCleanup() { callback_(); }
    TypeErasedCleanup(std::function<void()> callback)
        : callback_{std::move(callback)} {}
    TypeErasedCleanup(const TypeErasedCleanup& rhs) = delete;
    TypeErasedCleanup& operator=(const TypeErasedCleanup&) = delete;
    // When being moved from, then the callback is disabled.
    TypeErasedCleanup(TypeErasedCleanup&& rhs)
        : callback_(std::exchange(rhs.callback_, [] {})) {}
    TypeErasedCleanup& operator=(TypeErasedCleanup&& rhs) {
      callback_ = std::exchange(rhs.callback_, [] {});
      return *this;
    }
  };

  // A `QueryExecutionContext` together with all data structures that it
  // depends on. The members are stored as `unique_ptr`s so that the references
  // inside the `QueryExecutionContext` remain stable even when moving the
  // `Context`.
  struct Context {
    TypeErasedCleanup cleanup_;
    std::unique_ptr<Index> index_;
    std::unique_ptr<QueryResultCache> cache_;
    std::unique_ptr<QueryExecutionContext> qec_ =
        std::make_unique<QueryExecutionContext>(
            *index_, cache_.get(), makeAllocator(), SortPerformanceEstimator{});
  };

  using Key = std::tuple<std::optional<string>, bool, bool, bool, size_t>;
  static ad_utility::HashMap<Key, Context> contextMap;

  auto key = Key{turtleInput, loadAllPermutations, usePatterns,
                 usePrefixCompression, blocksizePermutationsInBytes};

  if (!contextMap.contains(key)) {
    std::string testIndexBasename =
        "_staticGlobalTestIndex" + std::to_string(contextMap.size());
    contextMap.emplace(
        key, Context{TypeErasedCleanup{[testIndexBasename]() {
                       for (const std::string& indexFilename :
                            getAllIndexFilenames(testIndexBasename)) {
                         // Don't log when a file can't be deleted,
                         // because the logging might already be
                         // destroyed.
                         ad_utility::deleteFile(indexFilename, false);
                       }
                     }},
                     std::make_unique<Index>(makeTestIndex(
                         testIndexBasename, turtleInput, loadAllPermutations,
                         usePatterns, usePrefixCompression,
                         blocksizePermutationsInBytes)),
                     std::make_unique<QueryResultCache>()});
  }
  return contextMap.at(key).qec_.get();
}

// Return a lambda that takes a string and converts it into an ID by looking
// it up in the vocabulary of `index`.
auto makeGetId = [](const Index& index) {
  return [&index](const std::string& el) {
    Id id;
    bool success = index.getId(el, &id);
    AD_CONTRACT_CHECK(success);
    return id;
  };
};

}  // namespace ad_utility::testing
