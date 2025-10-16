//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "IndexTestHelpers.h"

#include "./GTestHelpers.h"
#include "./TripleComponentTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/NamedResultCache.h"
#include "global/SpecialIds.h"
#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/ProgressBar.h"

using qlever::TextScoringMetric;
namespace ad_utility::testing {

// ______________________________________________________________
Index makeIndexWithTestSettings(ad_utility::MemorySize parserBufferSize) {
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.setNumTriplesPerBatch(2);
  EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  // Decrease various default batch sizes such that there are multiple batches
  // also for the very small test indices (important for test coverage).
  BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS = 10;
  BATCH_SIZE_VOCABULARY_MERGE = 2;
  DEFAULT_PROGRESS_BAR_BATCH_SIZE = 2;
  index.memoryLimitIndexBuilding() = 50_MB;
  index.parserBufferSize() =
      parserBufferSize;  // Note that the default value remains unchanged, but
                         // some tests (i.e. polygon testing in Spatial Joins)
                         // require a larger buffer size
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
      EXPECT_EQ(patternIdx, Pattern::NoPattern)
          << id << ' ' << Pattern::NoPattern;
    } else {
      auto actualPattern = scanResultHasPattern(0, 0).getInt();
      EXPECT_EQ(patternIdx, actualPattern) << id << ' ' << actualPattern;
    }
  };

  auto checkConsistencyForCol0IdAndPermutation =
      [&](Id col0Id, Permutation::Enum permutation, size_t subjectColIdx,
          size_t objectColIdx) {
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

  auto predicates = index.getImpl().PSO().getDistinctCol0IdsAndCounts(
      cancellationDummy, locatedTriplesSnapshot);
  for (const auto& predicate : predicates.getColumn(0)) {
    checkConsistencyForPredicate(predicate);
  }
  auto objects = index.getImpl().OSP().getDistinctCol0IdsAndCounts(
      cancellationDummy, locatedTriplesSnapshot);
  for (const auto& object : objects.getColumn(0)) {
    checkConsistencyForObject(object);
  }
  // NOTE: The SPO and SOP permutations currently don't have patterns stored.
  // with them.
}
}  // namespace

// _____________________________________________________________________________
Index makeTestIndex(const std::string& indexBasename, TestIndexConfig c) {
  // Ignore the (irrelevant) log output of the index building and loading during
  // these tests.
  static std::ostringstream ignoreLogStream;
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  std::string inputFilename = indexBasename + ".ttl";
  if (!c.turtleInput.has_value()) {
    c.turtleInput =
        "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . "
        "<x> "
        "<label> \"Beta\". <x> <is-a> <y>. <y> <is-a> <x>. <z> <label> "
        "\"zz\"@en . <zz> <label> <zz> .";
  }

  BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP = 2;
  {
    std::fstream f(inputFilename, std::ios_base::out);
    f << c.turtleInput.value();
    f.close();
  }
  {
    std::fstream settingsFile(inputFilename + ".settings.json",
                              std::ios_base::out);
    nlohmann::json settingsJson;
    if (!c.createTextIndex) {
      settingsJson["prefixes-external"] = std::vector<std::string>{""};
      settingsJson["languages-internal"] = std::vector<std::string>{""};
    }
    settingsFile << settingsJson.dump();
  }
  {
    Index index = makeIndexWithTestSettings(c.parserBufferSize);
    // This is enough for 2 triples per block. This is deliberately chosen as a
    // small value, s.t. the tiny knowledge graphs from unit tests also contain
    // multiple blocks. Should this value or the semantics of it (how many
    // triples it may store) ever change, then some unit tests might have to be
    // adapted.
    index.blocksizePermutationsPerColumn() = c.blocksizePermutations;
    index.setOnDiskBase(indexBasename);
    index.usePatterns() = c.usePatterns;
    index.setSettingsFile(inputFilename + ".settings.json");
    index.loadAllPermutations() = c.loadAllPermutations;
    qlever::InputFileSpecification spec{inputFilename, c.indexType,
                                        std::nullopt};
    // randomly choose one of the vocabulary implementations
    index.getImpl().setVocabularyTypeForIndexBuilding(
        c.vocabularyType.has_value() ? c.vocabularyType.value()
                                     : VocabularyType::random());
    if (c.encodedIriManager.has_value()) {
      // Extract prefixes without angle brackets from the EncodedIriManager
      std::vector<std::string> prefixes;
      for (const auto& prefix : c.encodedIriManager.value().prefixes_) {
        AD_CORRECTNESS_CHECK(ql::starts_with(prefix, '<') &&
                             !ql::ends_with(prefix, '>'));
        prefixes.push_back(prefix.substr(1));
      }
      index.getImpl().setPrefixesForEncodedValues(std::move(prefixes));
    }
    index.createFromFiles({spec});
    if (c.createTextIndex) {
      TextIndexBuilder textIndexBuilder = TextIndexBuilder(
          ad_utility::makeUnlimitedAllocator<Id>(), index.getOnDiskBase());
      // First test the case of invalid b and k parameters for BM25, it should
      // throw
      AD_EXPECT_THROW_WITH_MESSAGE(
          textIndexBuilder.buildTextIndexFile(
              std::nullopt, true, TextScoringMetric::BM25, {2.0f, 0.5f}),
          ::testing::HasSubstr("Invalid values"));
      AD_EXPECT_THROW_WITH_MESSAGE(
          textIndexBuilder.buildTextIndexFile(
              std::nullopt, true, TextScoringMetric::BM25, {0.5f, -1.0f}),
          ::testing::HasSubstr("Invalid values"));
      c.scoringMetric = c.scoringMetric.value_or(TextScoringMetric::EXPLICIT);
      c.bAndKParam = c.bAndKParam.value_or(std::pair{0.75f, 1.75f});

      // The following tests that garbage values for b and k work if these
      // parameters are unnecessary because we don't use `BM25`.
      if (c.scoringMetric.value() != TextScoringMetric::BM25) {
        c.bAndKParam = std::pair{-3.f, -3.f};
      }
      auto buildTextIndex = [&textIndexBuilder, &c](auto wordsAndDocsfile,
                                                    bool addWordsFromLiterals) {
        textIndexBuilder.buildTextIndexFile(
            std::move(wordsAndDocsfile), addWordsFromLiterals,
            c.scoringMetric.value(), c.bAndKParam.value());
      };
      if (c.contentsOfWordsFileAndDocsfile.has_value()) {
        // Create and write to words- and docsfile to later build a full text
        // index from them
        ad_utility::File wordsFile(indexBasename + ".wordsfile", "w");
        ad_utility::File docsFile(indexBasename + ".docsfile", "w");
        wordsFile.write(c.contentsOfWordsFileAndDocsfile.value().first.c_str(),
                        c.contentsOfWordsFileAndDocsfile.value().first.size());
        docsFile.write(c.contentsOfWordsFileAndDocsfile.value().second.c_str(),
                       c.contentsOfWordsFileAndDocsfile.value().second.size());
        wordsFile.close();
        docsFile.close();
        textIndexBuilder.setKbName(indexBasename);
        textIndexBuilder.setTextName(indexBasename);
        textIndexBuilder.setOnDiskBase(indexBasename);
        buildTextIndex(
            std::pair<std::string, std::string>{indexBasename + ".wordsfile",
                                                indexBasename + ".docsfile"},
            c.addWordsFromLiterals);
        textIndexBuilder.buildDocsDB(indexBasename + ".docsfile");
      } else if (c.addWordsFromLiterals) {
        buildTextIndex(std::nullopt, true);
      }
    }
  }
  if (!c.usePatterns || !c.loadAllPermutations) {
    // If we have no patterns, or only two permutations, then check the graceful
    // fallback even if the options were not explicitly specified during the
    // loading of the server.
    Index index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.usePatterns() = true;
    index.loadAllPermutations() = true;
    EXPECT_NO_THROW(index.createFromOnDiskIndex(indexBasename, false));
    EXPECT_EQ(index.loadAllPermutations(), c.loadAllPermutations);
    EXPECT_EQ(index.usePatterns(), c.usePatterns);
  }

  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.usePatterns() = c.usePatterns;
  index.loadAllPermutations() = c.loadAllPermutations;
  index.createFromOnDiskIndex(indexBasename, false);
  if (c.createTextIndex) {
    index.addTextFromOnDiskIndex();
  }
  ad_utility::setGlobalLoggingStream(&std::cout);

  if (c.usePatterns && c.loadAllPermutations) {
    checkConsistencyBetweenPatternPredicateAndAdditionalColumn(index);
  }
  return index;
}

// _____________________________________________________________________________
Index makeTestIndex(const std::string& indexBasename, std::string turtle) {
  return makeTestIndex(indexBasename, TestIndexConfig{std::move(turtle)});
}

// ________________________________________________________________________________
QueryExecutionContext* getQec(TestIndexConfig c) {
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
    std::unique_ptr<NamedResultCache> namedCache_;
    std::unique_ptr<QueryExecutionContext> qec_ =
        std::make_unique<QueryExecutionContext>(
            *index_, cache_.get(), makeAllocator(MemorySize::megabytes(100)),
            SortPerformanceEstimator{}, namedCache_.get());
  };

  static ad_utility::HashMap<TestIndexConfig, Context> contextMap;

  if (!contextMap.contains(c)) {
    std::string testIndexBasename =
        "_staticGlobalTestIndex" + std::to_string(contextMap.size());
    contextMap.emplace(
        c, Context{TypeErasedCleanup{[testIndexBasename]() {
                     for (const std::string& indexFilename :
                          getAllIndexFilenames(testIndexBasename)) {
                       // Don't log when a file can't be deleted,
                       // because the logging might already be
                       // destroyed.
                       ad_utility::deleteFile(indexFilename, false);
                     }
                   }},
                   std::make_unique<Index>(makeTestIndex(testIndexBasename, c)),
                   std::make_unique<QueryResultCache>(),
                   std::make_unique<NamedResultCache>()});
  }
  auto* qec = contextMap.at(c).qec_.get();
  qec->getIndex().getImpl().setGlobalIndexAndComparatorOnlyForTesting();
  return qec;
}

// _____________________________________________________________________________
QueryExecutionContext* getQec(std::optional<std::string> turtleInput,
                              std::optional<VocabularyType> vocabularyType) {
  TestIndexConfig config;
  config.turtleInput = std::move(turtleInput);
  config.vocabularyType = vocabularyType;
  return getQec(std::move(config));
}

// ___________________________________________________________
std::function<Id(const std::string&)> makeGetId(const Index& index) {
  return [&index](const std::string& el) {
    auto literalOrIri = [&el]() -> TripleComponent {
      if (ql::starts_with(el, '<') || ql::starts_with(el, '@')) {
        return TripleComponent::Iri::fromIriref(el);
      } else {
        AD_CONTRACT_CHECK(ql::starts_with(el, '\"'));
        return TripleComponent::Literal::fromStringRepresentation(el);
      }
    }();
    static const EncodedIriManager encodedIriManager;
    auto id = literalOrIri.toValueId(index.getVocab(), encodedIriManager);
    AD_CONTRACT_CHECK(id.has_value());
    return id.value();
  };
}
}  // namespace ad_utility::testing
