// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_LIBQLEVER_QLEVER_H
#define QLEVER_SRC_LIBQLEVER_QLEVER_H

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "engine/NamedResultCache.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "index/Index.h"
#include "index/InputFileSpecification.h"
#include "libqlever/QleverTypes.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "util/http/MediaTypes.h"

namespace qlever {

// The common configuration shared by the index building and query execution.
struct CommonConfig {
  // The basename of all files that QLever will write as part of the index
  // building.
  std::string baseName_;

  // The name of the index that will be built for a given dataset. This has no
  // particular semantics, except that it will be returned when asked for.
  std::string kbIndexName_ = "no index name specified";

  // An upper bound on the amount of memory that QLever will use during index
  // building and query processing. If more memory is required, an exception
  // is thrown.
  std::optional<ad_utility::MemorySize> memoryLimit_ =
      ad_utility::MemorySize::gigabytes(1);

  // Option to disable the pre-computation of QLever's so-called "patterns". If
  // enabled, QLever pre-computes the set of distinct predicates for each
  // subject, the pattern of the subject. These patterns are stored along with
  // each triple in each of the pre-computed permutations. This makes queries
  // like the following, which are useful but extremely hard, feasible:
  //
  // SELECT ?p (COUNT(DISTINCT ?s) AS ?count) WHERE {
  //   [group graph pattern containing ?s but not ?p or ?o]
  //   ?s ?p ?o .
  // } GROUP BY ?p
  bool noPatterns_ = false;

  // Option to build only the PSO and POS permutations of the triples. These
  // two permutations are sufficient to answer queries, where the predicate is
  // fixed in all triple patterns.
  //
  // TODO: We have not tested this mode in a while. In particular, it is
  // unlikely to work when updates are involved.
  bool onlyPsoAndPos_ = false;
};

// Additional configuration used for building an index for a given dataset.
struct IndexBuilderConfig : CommonConfig {
  // The specification of the input files, for which the index is built. See
  // `src/engine/LibQleverExample.cpp` for an example use.
  std::vector<InputFileSpecification> inputFiles_;

  // QLever's parses the input in chunks of this size. The chunk size must be
  // large enough, so that every statement from the input data (triple, quad,
  // or subject with predicate-object list in Turtle) fits into a single chunk.
  // The default chunk size is large enough for most input sets.
  std::optional<ad_utility::MemorySize> parserBufferSize_;

  // Filename of a JSON file with additional settings. Examples can be seen in
  // https://github.com/ad-freiburg/qlever-control/tree/main/src/qlever/Qleverfiles
  // If empty, default settings are used.
  //
  // NOTE: The practically most relevant key, for which a non-default value is
  // often needed, is `num-triples-per-batch`. In determines the number of
  // triples processed at once during index building by a single thread. If this
  // batch size is too small, there is an overhead of merging the related
  // intermediate files. If this batch size is too large, then the memory
  // consumption might be too large.
  //
  // TODO: The reason for this JSON is historical. We should have an own
  // parameter for each of the possible settings in the JSON file.
  std::string settingsFile_;

  // Specify how Qlever stores its mapping from IRIs and literals to internal
  // IDs. See `src/index/vocabulary/VocabularyType.h` for the possible options.
  ad_utility::VocabularyType vocabType_{
      ad_utility::VocabularyType::Enum::OnDiskCompressed};

  // If set to true, then certain temporary files which are created while
  // building the index are not deleted. This can be useful for debugging.
  bool keepTemporaryFiles_ = false;

  // A list of IRI prefixes (without angle brackets). IRIs that start with one
  // of these prefixes, followed by a sequence of a bounded number of digits
  // are encoded directly in the internal ID. This reduces the size of the
  // vocabulary (see above) and the time for exporting results involving
  // such IRIs.
  //
  // NOTE: Read the description of
  // https://github.com/ad-freiburg/qlever/pull/2299 for the details and
  // limitations regarding the correctness of FILTER and ORDER BY.
  std::vector<std::string> prefixesForIdEncodedIris_;

  // The remaining members of this class, are only relevant if a full-text
  // index is built in addition to the RDF index. By default, no fulltext index
  // is built. The full-text index enables efficient keyword search in text
  // records specified by the user.

  // If set, build a full-text index for all literals from the input data.
  bool addWordsFromLiterals_ = false;

  // If set, build a full-text index for the text records specified by the
  // following two files. See `https://github.com/ad-freiburg/qlever` for
  // documentation and examples.
  std::string wordsfile_;
  std::string docsfile_;

  // The name of the full-text index, analogously to `kbIndexName_` above.
  std::string textIndexName_;

  // If set to true, add a text index to an already existing RDF index.
  bool onlyAddTextIndex_ = false;

  // Configuration of the parameters of the BM25 scoring function. See
  // `src/index/IndexBuilderMain.cpp` for details.
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

// Additional configuration used for executing queries based on a previously
// built index.
struct EngineConfig : CommonConfig {
  explicit EngineConfig(const IndexBuilderConfig& c)
      : CommonConfig{static_cast<const CommonConfig&>(c)} {}
  EngineConfig() = default;

  // If set to true, the full-text index (with the same basename as the RDF
  // index) will be loaded. The requires that a full-text index has previously
  // been built.
  bool loadTextIndex_ = false;

  // If set to true, updates will be persisted on disk in a file
  // `basename.update-triples` (which will be read when the index is loaded
  // after a restart). To revert to the state of the index without updates,
  // simply delete this file.
  bool persistUpdates_ = true;
};

// Class to use QLever as an embedded database, without the HTTP server. See
// `src/engine/LibQleverExample.cpp` for an example use.
class Qlever {
 private:
  // The cache is threadsafe, so making it `mutable` is reasonably safe.
  mutable QueryResultCache cache_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  SortPerformanceEstimator sortPerformanceEstimator_;
  Index index_;
  mutable NamedResultCache namedResultCache_;
  bool enablePatternTrick_;

 public:
  // Build an index, using an `IndexBuilderConfig` as explained above.
  static void buildIndex(IndexBuilderConfig config);

  // Create a QLever instance for querying using an `EngineConfig` as
  // explained above.
  explicit Qlever(const EngineConfig& config);

  // Parse and plan the given `query`.
  //
  // NOTE: This is useful as a separate function for the following reasons.
  //
  // 1. Using a `QueryPlan`, one can execute a `query` multiple times without
  // having to parse and plan it again.
  //
  // 2. It helps measuring the time for the parsing and planning separately
  // from the time for the execution.
  //
  // 3. It enables an inspection or even modification of the query plan before
  // executing it (this requires some expertise).
  using QueryPlan = qlever::QueryPlan;
  QueryPlan parseAndPlanQuery(std::string query) const;

  // Run the given parsed and planned query. The result is returned as a
  // string; see `src/util/http/MediaTypes.h` for the supported formats.
  //
  // NOTE: With `ad_utility::MediaType::qleverJson`, the result also contains
  // detailed information on the query execution, including timings of the
  // various parts of the query plan.
  std::string query(const QueryPlan& queryPlan,
                    ad_utility::MediaType mediaType =
                        ad_utility::MediaType::sparqlJson) const;

  // Plan, parse, and execute a query using a single function call. This is
  // equivalent to calling `parseAndPlanQuery` followed by `query`.
  //
  // TODO: Also support timeouts, manual cancellation, updates, live timings
  // while the query is running, etc, These are all supported by QLever, but
  // not by this class yet.
  std::string query(std::string query,
                    ad_utility::MediaType mediaType =
                        ad_utility::MediaType::sparqlJson) const;

  // Plan, parse, and execute the given `query` and pin the result to the cache
  // with the given options (name and possibly request for building a geometry
  // index). This result can then be reused in a query as follows: `SERVICE
  // ql:cached-result-with-name-<name> {}`.
  void queryAndPinResultWithName(
      QueryExecutionContext::PinResultWithName options, std::string query);
  // Shorthand using only the name and no geo index for convenience and
  // compatibility.
  void queryAndPinResultWithName(std::string name, std::string query);

  // Clear the result with the given `name` from the cache.
  void eraseResultWithName(std::string name);
  void clearNamedResultCache();
};
}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_QLEVER_H
