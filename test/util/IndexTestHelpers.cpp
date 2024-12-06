//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "IndexTestHelpers.h"

#include "./GTestHelpers.h"
#include "./TripleComponentTestHelpers.h"
#include "global/SpecialIds.h"
#include "index/IndexImpl.h"
#include "util/ProgressBar.h"

namespace ad_utility::testing {

// ______________________________________________________________
Index makeIndexWithTestSettings() {
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.setNumTriplesPerBatch(2);
  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // Decrease various default batch sizes such that there are multiple batches
  // also for the very small test indices (important for test coverage).
  BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS = 10;
  BATCH_SIZE_VOCABULARY_MERGE = 2;
  DEFAULT_PROGRESS_BAR_BATCH_SIZE = 2;
  index.memoryLimitIndexBuilding() = 50_MB;
  return index;
}

std::vector<std::string> getAllIndexFilenames(
    const std::string& indexBasename) {
  return {indexBasename + ".ttl",
          indexBasename + ".index.pos",
          indexBasename + ".index.pos.meta",
          indexBasename + ".index.pso",
          indexBasename + ".index.pso.meta",
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
          indexBasename + ".vocabulary.external.offsets",
          indexBasename + ".wordsfile",
          indexBasename + ".docsfile",
          indexBasename + ".text.index",
          indexBasename + ".text.vocabulary",
          indexBasename + ".text.docsDB"};
}

namespace {
// Check that the patterns as stored in the `ql:has-pattern` relation in the PSO
// and POS permutations have exactly the same contents as the patterns that are
// folded into the permutations as additional columns.
void checkConsistencyBetweenPatternPredicateAndAdditionalColumn(
    const Index& index) {
  const DeltaTriplesManager& deltaTriplesManager{index.deltaTriplesManager()};
  auto sharedLocatedTriplesSnapshot = deltaTriplesManager.getCurrentSnapshot();
  const auto& locatedTriplesSnapshot = *sharedLocatedTriplesSnapshot;
  static constexpr size_t col0IdTag = 43;
  auto cancellationDummy = std::make_shared<ad_utility::CancellationHandle<>>();
  auto iriOfHasPattern =
      TripleComponent::Iri::fromIriref(HAS_PATTERN_PREDICATE);
  auto checkSingleElement = [&cancellationDummy, &iriOfHasPattern,
                             &locatedTriplesSnapshot](
                                const Index& index, size_t patternIdx, Id id) {
    auto scanResultHasPattern = index.scan(
        ScanSpecificationAsTripleComponent{iriOfHasPattern, id, std::nullopt},
        Permutation::Enum::PSO, {}, cancellationDummy, locatedTriplesSnapshot);
    // Each ID has at most one pattern, it can have none if it doesn't
    // appear as a subject in the knowledge graph.
    AD_CORRECTNESS_CHECK(scanResultHasPattern.numRows() <= 1);
    if (scanResultHasPattern.numRows() == 0) {
      EXPECT_EQ(patternIdx, NO_PATTERN) << id << ' ' << NO_PATTERN;
    } else {
      auto actualPattern = scanResultHasPattern(0, 0).getInt();
      EXPECT_EQ(patternIdx, actualPattern) << id << ' ' << actualPattern;
    }
  };

  auto checkConsistencyForCol0IdAndPermutation =
      [&](Id col0Id, Permutation::Enum permutation, size_t subjectColIdx,
          size_t objectColIdx) {
        auto cancellationDummy =
            std::make_shared<ad_utility::CancellationHandle<>>();
        auto scanResult = index.scan(
            ScanSpecification{col0Id, std::nullopt, std::nullopt}, permutation,
            std::array{ColumnIndex{ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN},
                       ColumnIndex{ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN}},
            cancellationDummy, locatedTriplesSnapshot);
        ASSERT_EQ(scanResult.numColumns(), 4u);
        for (const auto& row : scanResult) {
          auto patternIdx = row[2].getInt();
          Id subjectId = row[subjectColIdx];
          checkSingleElement(index, patternIdx, subjectId);
          Id objectId = objectColIdx == col0IdTag ? col0Id : row[objectColIdx];
          auto patternIdxObject = row[3].getInt();
          checkSingleElement(index, patternIdxObject, objectId);
        }
      };

  auto checkConsistencyForPredicate = [&](Id predicateId) {
    using enum Permutation::Enum;
    checkConsistencyForCol0IdAndPermutation(predicateId, PSO, 0, 1);
    checkConsistencyForCol0IdAndPermutation(predicateId, POS, 1, 0);
  };
  auto checkConsistencyForObject = [&](Id objectId) {
    using enum Permutation::Enum;
    checkConsistencyForCol0IdAndPermutation(objectId, OPS, 1, col0IdTag);
    checkConsistencyForCol0IdAndPermutation(objectId, OSP, 0, col0IdTag);
  };

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto predicates = index.getImpl().PSO().getDistinctCol0IdsAndCounts(
      cancellationHandle, locatedTriplesSnapshot);
  for (const auto& predicate : predicates.getColumn(0)) {
    checkConsistencyForPredicate(predicate);
  }
  auto objects = index.getImpl().OSP().getDistinctCol0IdsAndCounts(
      cancellationHandle, locatedTriplesSnapshot);
  for (const auto& object : objects.getColumn(0)) {
    checkConsistencyForObject(object);
  }
  // NOTE: The SPO and SOP permutations currently don't have patterns stored.
  // with them.
}
}  // namespace

// ______________________________________________________________
Index makeTestIndex(const std::string& indexBasename,
                    std::optional<std::string> turtleInput,
                    bool loadAllPermutations, bool usePatterns,
                    [[maybe_unused]] bool usePrefixCompression,
                    ad_utility::MemorySize blocksizePermutations,
                    bool createTextIndex, bool addWordsFromLiterals,
                    std::optional<std::pair<std::string, std::string>>
                        contentsOfWordsFileAndDocsFile) {
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
        "\"zz\"@en . <zz> <label> <zz>";
  }

  FILE_BUFFER_SIZE = 1000;
  BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP = 2;
  {
    std::fstream f(inputFilename, std::ios_base::out);
    f << turtleInput.value();
    f.close();
  }
  {
    std::fstream settingsFile(inputFilename + ".settings.json",
                              std::ios_base::out);
    nlohmann::json settingsJson;
    if (!createTextIndex) {
      settingsJson["prefixes-external"] = std::vector<std::string>{""};
      settingsJson["languages-internal"] = std::vector<std::string>{""};
    }
    settingsFile << settingsJson.dump();
  }
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
    index.setSettingsFile(inputFilename + ".settings.json");
    index.loadAllPermutations() = loadAllPermutations;
    qlever::InputFileSpecification spec{inputFilename, qlever::Filetype::Turtle,
                                        std::nullopt};
    index.createFromFiles({spec});
    if (createTextIndex) {
      if (contentsOfWordsFileAndDocsFile.has_value()) {
        // Create and write to words- and docsfile to later build a full text
        // index from them
        ad_utility::File wordsFile(indexBasename + ".wordsfile", "w");
        ad_utility::File docsFile(indexBasename + ".docsfile", "w");
        wordsFile.write(contentsOfWordsFileAndDocsFile.value().first.c_str(),
                        contentsOfWordsFileAndDocsFile.value().first.size());
        docsFile.write(contentsOfWordsFileAndDocsFile.value().second.c_str(),
                       contentsOfWordsFileAndDocsFile.value().second.size());
        wordsFile.close();
        docsFile.close();
        index.setKbName(indexBasename);
        index.setTextName(indexBasename);
        index.setOnDiskBase(indexBasename);
        if (addWordsFromLiterals) {
          index.addTextFromContextFile(indexBasename + ".wordsfile", true);
        } else {
          index.addTextFromContextFile(indexBasename + ".wordsfile", false);
        }
        index.buildDocsDB(indexBasename + ".docsfile");
      } else if (addWordsFromLiterals) {
        index.addTextFromContextFile("", true);
      }
    }
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
  if (createTextIndex) {
    index.addTextFromOnDiskIndex();
  }
  ad_utility::setGlobalLoggingStream(&std::cout);

  if (usePatterns && loadAllPermutations) {
    checkConsistencyBetweenPatternPredicateAndAdditionalColumn(index);
  }
  return index;
}

// ________________________________________________________________________________
QueryExecutionContext* getQec(std::optional<std::string> turtleInput,
                              bool loadAllPermutations, bool usePatterns,
                              bool usePrefixCompression,
                              ad_utility::MemorySize blocksizePermutations,
                              bool createTextIndex, bool addWordsFromLiterals,
                              std::optional<std::pair<std::string, std::string>>
                                  contentsOfWordsFileAndDocsFile) {
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
            *index_, cache_.get(), makeAllocator(MemorySize::megabytes(100)),
            SortPerformanceEstimator{});
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
                         blocksizePermutations, createTextIndex,
                         addWordsFromLiterals, contentsOfWordsFileAndDocsFile)),
                     std::make_unique<QueryResultCache>()});
  }
  auto* qec = contextMap.at(key).qec_.get();
  qec->getIndex().getImpl().setGlobalIndexAndComparatorOnlyForTesting();
  return qec;
}

// ___________________________________________________________
std::function<Id(const std::string&)> makeGetId(const Index& index) {
  return [&index](const std::string& el) {
    auto literalOrIri = [&el]() -> TripleComponent {
      if (el.starts_with('<') || el.starts_with('@')) {
        return TripleComponent::Iri::fromIriref(el);
      } else {
        AD_CONTRACT_CHECK(el.starts_with('\"'));
        return TripleComponent::Literal::fromStringRepresentation(el);
      }
    }();
    auto id = literalOrIri.toValueId(index.getVocab());
    AD_CONTRACT_CHECK(id.has_value());
    return id.value();
  };
}
}  // namespace ad_utility::testing
