//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "../IndexTestHelpers.h"

#include "./GTestHelpers.h"
#include "index/IndexImpl.h"

namespace ad_utility::testing {

// ______________________________________________________________
Index makeIndexWithTestSettings() {
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.setNumTriplesPerBatch(2);
  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS = 10;
  index.memoryLimitIndexBuilding() = 50_MB;
  return index;
}

std::vector<std::string> getAllIndexFilenames(
    const std::string& indexBasename) {
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

namespace {
// Check that the old pattern implementation (separate patterns in separate
// files) have exactly the same contents as the patterns that are folded into
// the PSO and POS permutation.
void checkConsistencyBetweenOldAndNewPatterns(const Index& index) {
  auto checkConsistencyForCol0IdAndPermutation =
      [&](Id col0Id, Permutation::Enum permutation, size_t subjectColIdx) {
        auto cancellationDummy =
            std::make_shared<ad_utility::CancellationHandle<>>();
        auto scanResult =
            index.scan(col0Id, std::nullopt, permutation,
                       std::array{ColumnIndex{2}}, cancellationDummy);
        ASSERT_EQ(scanResult.numColumns(), 3u);
        for (const auto& row : scanResult) {
          auto subject = row[subjectColIdx].getVocabIndex().get();
          auto patternIdx = row[2].getInt();
          if (subject >= index.getHasPattern().size()) {
            EXPECT_EQ(patternIdx, NO_PATTERN);
          } else {
            EXPECT_EQ(patternIdx, index.getHasPattern()[subject])
                << subject << ' '
                << index.idToOptionalString(row[subjectColIdx].getVocabIndex())
                       .value()
                << ' ' << index.getHasPattern().size() << ' ' << NO_PATTERN;
          }
        }
      };

  auto checkConsistencyForPredicate = [&](Id predicateId) {
    using enum Permutation::Enum;
    checkConsistencyForCol0IdAndPermutation(predicateId, PSO, 0);
    checkConsistencyForCol0IdAndPermutation(predicateId, POS, 1);
  };
  auto checkConsistencyForObject = [&](Id objectId) {
    using enum Permutation::Enum;
    checkConsistencyForCol0IdAndPermutation(objectId, OPS, 1);
    checkConsistencyForCol0IdAndPermutation(objectId, OSP, 0);
  };
  const auto& predicates = index.getImpl().PSO().metaData().data();
  for (const auto& predicate : predicates) {
    checkConsistencyForPredicate(predicate.col0Id_);
  }
  const auto& objects = index.getImpl().OSP().metaData().data();
  for (const auto& object : objects) {
    checkConsistencyForObject(object.col0Id_);
  }
  // NOTE: The SPO and SOP permutations currently don't have patterns stored.
  // with them.
}
}  // namespace

// ______________________________________________________________
Index makeTestIndex(const std::string& indexBasename,
                    std::optional<std::string> turtleInput,
                    bool loadAllPermutations, bool usePatterns,
                    bool usePrefixCompression,
                    ad_utility::MemorySize blocksizePermutations) {
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
    index.blocksizePermutationsPerColumn() = blocksizePermutations;
    index.setOnDiskBase(indexBasename);
    index.usePatterns() = usePatterns;
    index.setPrefixCompression(usePrefixCompression);
    index.loadAllPermutations() = loadAllPermutations;
    index.createFromFile(inputFilename);
  }
  if (!usePatterns || !loadAllPermutations) {
    // If we have no patterns, or only two permutations, then check the graceful
    // fallback even if the options were not explicitly specified during the
    // loading of the server.
    Index index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.usePatterns() = true;
    index.loadAllPermutations() = true;
    EXPECT_NO_THROW(index.createFromOnDiskIndex(indexBasename));
    EXPECT_EQ(index.loadAllPermutations(), loadAllPermutations);
    EXPECT_EQ(index.usePatterns(), usePatterns);
  }
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.usePatterns() = usePatterns;
  index.loadAllPermutations() = loadAllPermutations;
  index.createFromOnDiskIndex(indexBasename);
  ad_utility::setGlobalLoggingStream(&std::cout);

  if (usePatterns && loadAllPermutations) {
    checkConsistencyBetweenOldAndNewPatterns(index);
  }
  return index;
}

// ________________________________________________________________________________
QueryExecutionContext* getQec(std::optional<std::string> turtleInput,
                              bool loadAllPermutations, bool usePatterns,
                              bool usePrefixCompression,
                              ad_utility::MemorySize blocksizePermutations) {
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

  using Key = std::tuple<std::optional<string>, bool, bool, bool,
                         ad_utility::MemorySize>;
  static ad_utility::HashMap<Key, Context> contextMap;

  auto key = Key{turtleInput, loadAllPermutations, usePatterns,
                 usePrefixCompression, blocksizePermutations};

  if (!contextMap.contains(key)) {
    std::string testIndexBasename =
        "_staticGlobalTestIndex" + std::to_string(contextMap.size());
    contextMap.emplace(
        key,
        Context{TypeErasedCleanup{[testIndexBasename]() {
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
                    usePatterns, usePrefixCompression, blocksizePermutations)),
                std::make_unique<QueryResultCache>()});
  }
  return contextMap.at(key).qec_.get();
}

// ___________________________________________________________
std::function<Id(const std::string&)> makeGetId(const Index& index) {
  return [&index](const std::string& el) {
    Id id;
    bool success = index.getId(el, &id);
    AD_CONTRACT_CHECK(success);
    return id;
  };
}
}  // namespace ad_utility::testing
