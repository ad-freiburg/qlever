// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_LIBQLEVER_QLEVER_H
#define QLEVER_SRC_LIBQLEVER_QLEVER_H

#include <util/MemorySize/MemorySize.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "engine/QueryExecutionContext.h"
#include "engine/QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "index/Index.h"
#include "index/InputFileSpecification.h"
#include "util/AllocatorWithLimit.h"
#include "util/http/MediaTypes.h"

namespace qlever {

// The common configuration shared by the index building and query execution.
struct CommonConfig {
  // A basename for all files that QLever will write as part of the index
  // building.
  std::string baseName_;

  // The name of the dataset, will be output by the index when requested to
  // identify a dataset.
  std::string kbIndexName = "no index name specified";

  // A memory limit that will be applied during the index building as well as
  // during the query processing.
  std::optional<ad_utility::MemorySize> memoryLimit_ =
      ad_utility::MemorySize::gigabytes(1);

  // If set to true, then no so-called patterns will be built. Patterns are
  // useful for autocompletion and for certain statistics queries, but not for
  // typical SELECT queries.
  bool noPatterns_ = false;

  // Only build two permutations. This is sufficient if all queries have a fixed
  // predicate.
  // TODO<joka921> We haven't tested this mode in a while, it is currently
  // probably broken because the UPDATE mechanism doesn't support only two
  // permutations.
  bool onlyPsoAndPos_ = false;
};

// The configuration required for building an index.
struct IndexBuilderConfig : CommonConfig {
  // The specification of the input files (Turtle/NT or NQuad) from which the
  // index will be built.
  std::vector<InputFileSpecification> inputFiles_;

  // The RDF parser (that parses the Turtle/Ntriples/NQuad files) will parse
  // chunks of this size. Typically it is not necessary to set this parameter as
  // the default value should be suitable for most use cases.
  std::optional<ad_utility::MemorySize> parserBufferSize_;

  // Optionally a filename to a JSON file with additional settings...
  // TODO<joka921> Make these settings part of this struct directly
  // TODO<joka921> Document these additional settings.
  std::string settingsFile_;

  // Specify how QLever's vocabulary (the mapping from IRIs and literals to IDs)
  // is stored, e.g. uncompressed vs compressed and in-memory vs on-disk.
  ad_utility::VocabularyType vocabType_{
      ad_utility::VocabularyType::Enum::OnDiskCompressed};

  // If set to true, then certain temporary files which are created while
  // building the index are not deleted. This can be useful for debugging.
  bool keepTemporaryFiles_ = false;

  /*
  A list of IRI prefixes (without angle brackets).
  IRIs that start with one of these prefixes, followed by a sequence of
  digits, do not require a vocabulary entry, but are directly encoded
  in the ID. NOTE: When using ORDER BY, the order among encoded IRIs and
  among non-encoded IRIs is correct, but the order between encoded
  and non-encoded IRIs is not);
  */
  std::vector<std::string> prefixesForIdEncodedIris_;

  // The following members are only required if QLever's full-text search
  // extension is to be used.

  // If set, then literals from the vocabulary will be also indexed in QLever's
  // full-test index.
  bool addWordsFromLiterals_ = false;

  // Filenames to the `wordsfile` and `docsfile` for QLever's fulltext index
  // (see the documentation for the expected format of the files).
  std::string wordsfile_;
  std::string docsfile_;

  // A human-readable description of the text index, will be emitted by the
  // index if requested to identify the dataset.
  std::string textIndexName_;

  // If `true`, then no RDF index will be built, but only a fulltext index will
  // be added to an existing RDF index.
  bool onlyAddTextIndex_ = false;

  // Configuration of the scores of word occurrences of texts. See
  // `IndexBuilderMain.cpp` for details.
  TextScoringMetric textScoringMetric_ = TextScoringMetric::EXPLICIT;
  float bScoringParam_ = 0.75;
  float kScoringParam_ = 1.75;

  // Assert that the given configuration is valid.
  void validate() const;

  // True if both of the `wordsfile_` and `docsfile_` are nonempty.
  bool wordsAndDocsFileSpecified() const {
    return !(wordsfile_.empty() || docsfile_.empty());
  }
};

// The configuration required for executing queries on a previously built index.
struct EngineConfig : CommonConfig {
  explicit EngineConfig(const IndexBuilderConfig& c)
      : CommonConfig{static_cast<const CommonConfig&>(c)} {}
  EngineConfig() = default;

  // If `true`, then the fulltext index will also be loaded. This requires that
  // the fulltext index has previously been built.
  bool loadTextIndex_ = false;

  // If `true` then the effects of SPARQL UPDATE requests will be persisted on
  // disk and are still present after a restart of QLever.
  bool persistUpdates_ = true;
};

// A class that can be used to use QLever without the HTTP server, e.g. as part
// of another program.
class Qlever {
 private:
  // All the data members required to run an instance of QLever.

  // The cache is threadsafe, so making it `mutable` is reasonably safe.
  mutable QueryResultCache cache_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  SortPerformanceEstimator sortPerformanceEstimator_;
  Index index_;
  bool enablePatternTrick_;

 public:
  // Build a persistent on disk index using the `config`.
  static void buildIndex(IndexBuilderConfig config);

  // Load the qlever index from file.
  explicit Qlever(const EngineConfig& config);

  // The result of parsing and planning a query. Using a `QueryPlan` one can
  // simply
  using QueryPlan =
      std::tuple<std::shared_ptr<QueryExecutionTree>,
                 std::shared_ptr<QueryExecutionContext>, ParsedQuery>;

  // Parse the given `query` and run the query planner for it.
  QueryPlan parseAndPlanQuery(std::string query) const;

  // Run the given query on the index. Currently only queries, but no updates
  // are supported.
  // TODO<joka921> Support UPDATE, support
  // cancellation, time limits, and observable queries via runtime information.
  std::string query(std::string query,
                    ad_utility::MediaType mediaType =
                        ad_utility::MediaType::sparqlJson) const;

  // Same as `query` above, but takes a previously preconstructed `QueryPlan`
  std::string query(const QueryPlan& queryPlan,
                    ad_utility::MediaType mediaType =
                        ad_utility::MediaType::sparqlJson) const;
};
}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_QLEVER_H
