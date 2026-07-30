// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_LIBQLEVER_QLEVER_H
#define QLEVER_SRC_LIBQLEVER_QLEVER_H

#include <gtest/gtest_prod.h>

#include <boost/optional.hpp>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "backports/filesystem.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/NamedResultCacheSerializer.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "index/DeltaTriples.h"
#include "index/Index.h"
#include "index/IndexRebuilderTypes.h"
#include "index/InputFileSpecification.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/QleverTypes.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Synchronized.h"
#include "util/TransparentFunctors.h"
#include "util/http/MediaTypes.h"
#include "util/json.h"

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
  std::optional<ad_utility::MemorySize> memoryLimit_ = std::nullopt;

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

  // Option to add `ql:has-word` triples for each word in each literal. For
  // each literal, a triple `<literal> ql:has-word "word"` is added for each
  // word in the literal. This is useful for keyword search in literals.
  bool addHasWordTriples_ = false;
};

// Configuration for relocating a runtime-rebuilt index. It bundles the four
// basenames that are involved in swapping a freshly rebuilt index into place.
// All paths are relative to the working directory of the engine. The base names
// are validated and fixed at construction time and afterwards only readable via
// the accessors. The constructor enforces that the base names do not collide in
// a way that would overwrite files that are still needed.
class IndexRebuildConfig {
 private:
  // These are documented at their accessors below.
  std::string oldIndexSource_;
  std::string newIndexSource_;
  std::string oldIndexTarget_;
  std::string newIndexTarget_;

 public:
  // Construct from the four base names (see the accessors below for their
  // meaning). Throws if the base names collide.
  IndexRebuildConfig(std::string oldIndexSource, std::string newIndexSource,
                     std::string oldIndexTarget, std::string newIndexTarget);

  // The base name of the index that is currently being served, i.e. the index
  // that is about to be replaced by the freshly rebuilt one. This is where the
  // old index is moved *from*.
  const std::string& oldIndexSource() const { return oldIndexSource_; }

  // The base name under which the freshly rebuilt index was built in a
  // temporary location. This is where the new index is moved *from*. After the
  // new index has been moved to its final place, the containing directory is
  // typically removed again.
  const std::string& newIndexSource() const { return newIndexSource_; }

  // The base name to which the files of the old (currently served) index are
  // moved when the new index is swapped in. This is where the old index is
  // moved *to*. The resulting files form a complete index that a server can be
  // started on in case something is wrong with the new index.
  const std::string& oldIndexTarget() const { return oldIndexTarget_; }

  // The base name under which the new index is served after the swap (and from
  // which a later restart loads it). This is where the new index is moved *to*.
  // Typically the same location as the currently served index, so that the
  // "current" index has a stable location.
  const std::string& newIndexTarget() const { return newIndexTarget_; }

  // The JSON that is reported to the client after a successful rebuild: a
  // human-readable message plus the directory to which the old index was
  // retired (the resolved value of the `rebuild-previous-index-dir` command
  // parameter, which the client does not know when the default was used).
  nlohmann::json successResponseAsJson() const;
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

  // A list of regexes for IRIs that should be treated as blank nodes. During
  // index building, an IRI that is fully matched by one of these regexes (via
  // `RE2::FullMatch`, applied to the full IRI text including the angle
  // brackets) is not stored in the vocabulary, but converted to a blank node.
  // The match has to cover the entire IRI, so each regex must describe a full
  // IRI and therefore has to start with `<`; to allow an arbitrary suffix, end
  // it with `.*` (e.g. `<https://example\.org/statement/.*>`). This is useful
  // for IRIs that only act as internal connector nodes (e.g. statement nodes),
  // to save vocabulary memory. Only IRIs are affected; literals are never
  // converted.
  //
  // NOTE: This is an experimental feature. The affected IRIs behave as ordinary
  // blank nodes, so they are no longer recognized as those IRIs if used, e.g.,
  // in a query or an update. See `TripleComponentWithIndex::isBlankNode`.
  std::vector<std::string> blankNodeIriRegexes_;

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

  // Materialized views to be written after normal index build is complete.
  using WriteMaterializedViews =
      std::vector<std::pair<std::string, std::string>>;
  WriteMaterializedViews writeMaterializedViews_;

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

  // If set to true, no permutations will be loaded from disk. This is useful
  // when only queries that don't require accessing the permutations need to be
  // executed (e.g., queries that only compute constant expressions, or query
  // that only rely on the `NamedQueryCache` which can be populated
  // separately).
  bool doNotLoadPermutations_ = false;

  // QLever doesn't use a cancelable sorting algorithm, but before starting a
  // sort estimates whether the sort will time out. To do so, on index load it
  // sorts some `IdTable`s to measure the time that sorting takes on the
  // concrete machine. This step can take some time and can be disabled by
  // setting this flag to `false`. In that case, sort operations are always
  // started and run to completion (unless the query times out or is canceled
  // before the sort operation starts).
  bool computeSortPerformanceEstimators_ = true;

  // A list of IRI prefixes that are allowed as `SERVICE` endpoints. If empty
  // (the default), all IRIs are allowed. If non-empty, `SERVICE` requests to
  // IRIs that do not start with any of the given prefixes are rejected.
  std::vector<std::string> serviceAllowedIriPrefixes_;

  // If set to true, caching is disabled for all operations. Default is
  // to for each operation query the corresponding runtime parameter.
  QueryExecutionContext::DisableCaching disableCaching_ =
      QueryExecutionContext::DisableCaching::FromRuntimeParameter;

  // Names of materialized views to load from disk during initialization.
  // If a view doesn't exist, a warning is logged and startup continues.
  std::vector<std::string> preloadMaterializedViews_ = {};
};

// Class to use QLever as an embedded database, without the HTTP server. See
// `src/engine/LibQleverExample.cpp` for an example use.
class Qlever {
 public:
  using PlannedQuery = qlever::PlannedQuery;

  // Bundle the `Index` and the `MaterializedViewsManager` under a single mutex
  // so that an index rebuild can atomically swap both in, while other threads
  // continue to read the previous instances via the `shared_ptr`s they hold.
  struct IndexAndViews {
    Index index_;
    MaterializedViewsManager materializedViewsManager_;

    // Create an instance.
    IndexAndViews(Index index,
                  MaterializedViewsManager materializedViewsManager)
        : index_{std::move(index)},
          materializedViewsManager_{std::move(materializedViewsManager)} {}

    // Make sue this is only passed around as a shared pointer or reference.
    IndexAndViews(IndexAndViews&&) noexcept = delete;
    IndexAndViews& operator=(IndexAndViews&&) noexcept = delete;
    IndexAndViews(const IndexAndViews&) noexcept = delete;
    IndexAndViews& operator=(const IndexAndViews&) noexcept = delete;

    // Helper function to decompose `self` into a pair of two shared pointers
    // pointing to the individual members via aliasing semantics.
    friend std::pair<std::shared_ptr<Index>,
                     std::shared_ptr<MaterializedViewsManager>>
    getPointerPair(std::shared_ptr<IndexAndViews> self) {
      std::shared_ptr<Index> index{self, &self->index_};
      auto& viewsManagerRef = self->materializedViewsManager_;
      return std::pair{std::move(index),
                       std::shared_ptr<MaterializedViewsManager>{
                           std::move(self), &viewsManagerRef}};
    }
  };

 private:
  // The cache is threadsafe, so making it `mutable` is reasonably safe.
  mutable QueryResultCache cache_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  SortPerformanceEstimator sortPerformanceEstimator_;
  mutable NamedResultCache namedResultCache_;
  ad_utility::Synchronized<std::shared_ptr<IndexAndViews>> indexAndViews_;
  // The configuration that this instance was created with. It is kept because
  // publishing a new generation of the auxiliary index reloads the index from
  // disk (see `buildAuxIndex`) and has to apply the same settings again.
  EngineConfig config_;
  bool enablePatternTrick_;
  QueryExecutionContext::DisableCaching disableCaching_;
  using TimeLimit = std::chrono::milliseconds;
  using SharedCancellationHandle = ad_utility::SharedCancellationHandle;

  // Handles the (de)serialization of the vocabulary and the `NamedResultCache`
  // to and from a compressed blob (see the delegating public methods
  // `serializeVocabAndNamedCacheToCompressedBlob` /
  // `deserializeVocabAndNamedCacheFromCompressedBlob` below). It is a friend of
  // this class so that it can access the internals it needs.
  NamedCachedQueryBlobManager blobManager_;
  friend class NamedCachedQueryBlobManager;

  FRIEND_TEST(LibQlever, swapIndexAndViewsThrowsWithNonEmptyNamedCache);

 public:
  // Build a new generation of the auxiliary index (see `index/AuxIndex.h`) from
  // the delta triples that are currently held in RAM, and publish it. The
  // triples then live on disk instead of in RAM, and are merged into the index
  // scans from there.
  //
  // Publishing works by atomically swapping in a freshly loaded `Index` that
  // uses the new generation. Queries that are still running keep the `Index`
  // (and hence the generation of the auxiliary vocabulary) that they started
  // with, which is what makes the `Id`s of an auxiliary vocabulary -- which are
  // only valid for a single generation -- safe to use.
  //
  // PRECONDITION: No update may be applied concurrently, else it would be
  // applied to the retired `Index` and hence be lost. The server guarantees
  // this by running this on its single-threaded update executor. Note that the
  // updates that arrive between the start of the build and the swap are carried
  // over (see `DeltaTriples::addFromSnapshotDiff`), so moving the build off
  // that executor only requires making the *swap* mutually exclusive with
  // updates.
  void buildAuxIndex(const SharedCancellationHandle& cancellationHandle);

  // Build an index, using an `IndexBuilderConfig` as explained above.
  static void buildIndex(IndexBuilderConfig config);

  // Create a QLever instance for querying using an `EngineConfig` as
  // explained above. If `skipLoading` is true, no index is loaded from disk
  // (in particular, none of the on-disk index files, not even the vocabulary
  // or the `.meta-data.json`, need to exist); the instance must then be
  // populated from a blob via `deserializeVocabAndNamedCacheFromCompressedBlob`
  // before it can answer queries.
  explicit Qlever(const EngineConfig& config, bool skipLoading = false);

  // Run the query planner on `parsedQuery`. Despite the name, `ParsedQuery`
  // is also used to represent SPARQL update operations (see
  // ParsedQuery::hasUpdateClause()); this function handles both cases
  // uniformly.
  //
  // If `requestTimer` is set, the elapsed time of that timer at the end of
  // query planning is stored in the query's runtime information as
  // `timeQueryPlanning`. This information can be accessed via the
  // query execution tree's root operation.
  //
  // TODO<joka921,damekt> The `timeLimit` is currently only used for
  // non-cancelable operations (in particular sorting). The time limit applies
  // from the time this function is called until the execution of the query
  // has finished. This might be very unintuitive when the `PlannedQuery` is
  // stored for later execution. This is not an issue for now (only the
  // `Server` actually imposes time limits and then executes the queries right
  // away), but should be addressed in the future once the timeout management
  // also is moved into the `QLever` class.
  PlannedQuery planQuery(ParsedQuery&& parsedQuery, QueryExecutionContext& qec,
                         SharedCancellationHandle handle,
                         std::optional<TimeLimit> timeLimit,
                         boost::optional<const ad_utility::Timer&>
                             requestTimer = boost::none) const;

  // Plan a query that was parsed by `parseQuery` (or bundled by
  // `bindParsedQuery`, both see below). The query is planned against the
  // `QueryExecutionContext` that `parsedQuery` carries, so the two can not get
  // out of sync. Implemented in terms of the `planQuery` overload above.
  //
  // For the semantics of `handle`, `timeLimit`, and `requestTimer`, see
  // `planQuery` above.
  PlannedQuery planQuery(
      ParsedQueryAndContext parsedQuery,
      SharedCancellationHandle handle =
          std::make_shared<ad_utility::CancellationHandle<>>(),
      std::optional<TimeLimit> timeLimit = std::nullopt,
      boost::optional<const ad_utility::Timer&> requestTimer =
          boost::none) const;

  // Parse the given `query` (despite the name, `query` may also be a SPARQL
  // update operation) and return it together with the
  // `QueryExecutionContext` to plan and execute it against, see
  // `ParsedQueryAndContext`.
  //
  // This is the first half of `parseAndPlanQuery`, the second half being the
  // `planQuery` overload above. Calling the two separately is useful to inspect
  // or modify the `ParsedQuery` before it is planned, to measure the time for
  // the parsing and the planning separately, and to reuse a parsed query (see
  // below).
  //
  // NOTE ON REUSING A PARSED QUERY: The `ParsedQuery` depends on the
  // `QueryExecutionContext` it was parsed with, but only through that context's
  // `EncodedIriManager` (which determines which IRIs are encoded directly in
  // the ID). It is therefore valid, and saves the repeated parsing of the same
  // query, to take the `ParsedQuery` out of the result and plan it against a
  // *different* context, as long as that context has an equivalent
  // `EncodedIriManager` (see `bindParsedQuery`). This is in particular the case
  // for several `Qlever` instances whose indexes were built with the same
  // `IndexBuilderConfig::prefixesForIdEncodedIris_`. If the
  // `EncodedIriManager`s differ, the affected IRIs are silently misinterpreted,
  // so this has to be ensured by the caller.
  ParsedQueryAndContext parseQuery(
      std::string query, const std::vector<DatasetClause>& datasetClauses = {},
      std::function<void(std::string)> updateCallback = ad_utility::noop,
      bool pinSubtrees = false, bool pinResult = false) const;

  // Bundle an already-parsed query with a fresh `QueryExecutionContext` of this
  // instance, so that it can be planned here. Together with `parseQuery` this
  // makes it possible to parse a query once and plan it on several instances.
  //
  // PRECONDITION: `parsedQuery` must have been parsed with a context whose
  // `EncodedIriManager` is equivalent to this instance's; see the note on
  // reusing a parsed query in `parseQuery` above. This is not checked.
  ParsedQueryAndContext bindParsedQuery(
      ParsedQuery parsedQuery,
      std::function<void(std::string)> updateCallback = ad_utility::noop,
      bool pinSubtrees = false, bool pinResult = false) const;

  // Parse and plan the given `query` (see `planQuery` above; despite the
  // name, `query` may also be a SPARQL update operation). This is exactly
  // `parseQuery` followed by `planQuery`.
  //
  // NOTES: This is useful as a separate function for the following reasons.
  //
  // 1. Using a `PlannedQuery`, one can execute a `query` multiple times without
  // having to parse and plan it again.
  //
  // 2. It helps measuring the time for the parsing and planning separately
  // from the time for the execution.
  //
  // 3. It enables an inspection or even modification of the query plan before
  // executing it (this requires some expertise).
  //
  // TODO<joka921,damekt> The `timeLimit` is currently only used for
  // non-cancelable operations (in particular sorting). The time limit applies
  // from the time this function is called until the execution of the query
  // has finished. This might be very unintuitive when the `PlannedQuery` is
  // stored for later execution. This is not an issue for now (only the
  // `Server` actually imposes time limits and then executes the queries right
  // away), but should be addressed in the future once the timeout management
  // also is moved into the `QLever` class.
  PlannedQuery parseAndPlanQuery(
      std::string query, const std::vector<DatasetClause>& datasetClauses = {},
      SharedCancellationHandle handle =
          std::make_shared<ad_utility::CancellationHandle<>>(),
      std::optional<TimeLimit> timeLimit = std::nullopt,
      boost::optional<const ad_utility::Timer&> requestTimer = boost::none,
      std::function<void(std::string)> updateCallback = ad_utility::noop,
      bool pinSubtrees = false, bool pinResult = false) const;

  // Run the given parsed and planned query. The result is returned as a
  // string; see `src/util/http/MediaTypes.h` for the supported formats.
  //
  // NOTE: With `ad_utility::MediaType::qleverJson`, the result also contains
  // detailed information on the query execution, including timings of the
  // various parts of the query plan.
  std::string query(const PlannedQuery& plannedQuery,
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
  // Completely clear the `NamedResultCache`.
  void clearNamedResultCache();

  // Write a new materialized view with `name` to disk and store the result of
  // `query`.
  //
  // `requestTimer`, `timeLimit`, and `handle` are forwarded to `planQuery`
  // (see there for their exact semantics). If omitted, the query is planned
  // and executed without a timer, without a time limit, and with a fresh,
  // never-triggered cancellation handle, i.e. it always runs to completion.
  void writeMaterializedView(
      std::string name, std::string query,
      const std::vector<DatasetClause>& datasetClauses = {},
      SharedCancellationHandle handle =
          std::make_shared<ad_utility::CancellationHandle<>>(),
      std::optional<TimeLimit> timeLimit = std::nullopt,
      boost::optional<const ad_utility::Timer&> requestTimer =
          boost::none) const;

  // Preload a materialized view s.t. the first query to the view does not have
  // to load the view.
  void loadMaterializedView(std::string name) const;

  // Check if a materialized view with the given name is currently loaded.
  bool isMaterializedViewLoaded(const std::string& name) const;

  // Delete the materialized view with the given name: unload it if loaded and
  // delete its files from disk. Throws if the view does not exist.
  void deleteMaterializedView(std::string name) const;

  // Serialize the index metadata JSON, the vocabulary, and the
  // `NamedResultCache` of this instance into a single, self-contained,
  // ZSTD-compressed blob that can later be loaded via
  // `deserializeVocabAndNamedCacheFromCompressedBlob` (e.g. by a different
  // process, without needing access to the on-disk index). For details see
  // `NamedCachedQueryBlobManager::serialize`.
  std::vector<char> serializeVocabAndNamedCacheToCompressedBlob() const {
    return blobManager_.serialize(*this);
  }

  // Load a blob previously written by
  // `serializeVocabAndNamedCacheToCompressedBlob`. For details see
  // `NamedCachedQueryBlobManager::deserialize`.
  //
  // PRECONDITION: Must only be called while no other thread can concurrently
  // access this instance, e.g. right after construction and before the first
  // query is answered. Must not be called more than once on the same
  // instance.
  void deserializeVocabAndNamedCacheFromCompressedBlob(
      ql::span<const char> blob,
      ql::pmr::polymorphic_allocator<char> allocator = {}) {
    // Note: `polymorphic_allocator` is cheap to copy and has no
    // dedicated move operations.
    blobManager_.deserialize(*this, blob, allocator);
  }

  // Clear the query result cache.
  void clearCache() { cache_.clearAll(); }

  // Create a Query Execution Context needed for execution of single SPARQL
  // query. Use an explicitly snapshotted `IndexAndViews` to make sure we have a
  // consistent state.
  std::shared_ptr<QueryExecutionContext> createQueryExecutionContext(
      std::shared_ptr<IndexAndViews> indexAndViews,
      std::function<void(std::string)> updateCallback = ad_utility::noop,
      bool pinSubtrees = false, bool pinResult = false,
      QueryExecutionContext::DisableCaching disableCaching =
          QueryExecutionContext::DisableCaching::FromRuntimeParameter) const;

  // Atomically snapshot both the `Index` and the `MaterializedViewsManager`
  // under a single read lock, so that all code paths handling a single request
  // observe a matching pair even if a concurrent rebuild swaps the pointers
  // between two reads.
  std::shared_ptr<IndexAndViews> indexAndViewsSnapshot() const {
    return *indexAndViews_.rlock();
  }

  // Atomically swap in a freshly built `IndexAndViews`. The old instance stays
  // alive as long as some `shared_ptr` (e.g. obtained via
  // `indexAndViewsSnapshot()`) still references it.
  //
  // PRECONDITION: The `NamedResultCache` must be empty. Its entries reference
  // IDs (and possibly zero-copy views) that are only valid for the specific
  // index snapshot they were created against; swapping in a different index
  // would silently invalidate them. Callers that want to swap the index must
  // therefore clear the named result cache first. (This is a deliberately
  // minimally invasive guard; full support for keeping the named result cache
  // across index snapshots is future work.)
  void swapIndexAndViews(std::shared_ptr<IndexAndViews> indexAndViews) {
    AD_CONTRACT_CHECK(
        namedResultCache_.numEntries() == 0,
        "The index snapshot must not be swapped while the named result cache "
        "is not empty");
    *indexAndViews_.wlock() = std::move(indexAndViews);
  }

  // Assemble the `IndexRebuildConfig` for a rebuild of `index` (which has to be
  // the index that is currently being served) from the two directories a
  // rebuild can be configured with: `rebuildTmpDir`, in which the new index
  // is built, and `rebuildPreviousIndexDir`, to which the old index is retired.
  // Both default (if `std::nullopt`) to a directory that is derived from the
  // current time resp. from the build date of the current index. Inside these
  // directories, and for the new index after the swap, the file name of
  // `index.getOnDiskBase()` is used: the new index has to end up at the base
  // name the current index is served from, so that a later restart loads it.
  //
  // The two directories must be relative paths (they are resolved against the
  // working directory of the engine, just like the base name of the current
  // index), must be empty or not exist yet, and must lie inside the directory
  // of `index.getOnDiskBase()`, so that the index directories are not nested
  // ever deeper. Throws `std::runtime_error` if one of these conditions is
  // violated, and (via the `IndexRebuildConfig` constructor) if the resulting
  // base names collide.
  static IndexRebuildConfig makeIndexRebuildConfig(
      const Index& index, std::optional<std::string> rebuildTmpDir,
      std::optional<std::string> rebuildPreviousIndexDir);

  // Move a freshly rebuilt index into the place of the old one. There are two
  // indices involved, both with base names given by `config`: the old index
  // that is currently being served (at `config.oldIndexSource()`), and the
  // freshly rebuilt index `newIndexAndViews` (built in a temporary location at
  // `config.newIndexSource()`). This function performs two renames and then
  // re-anchors the new index in memory:
  //
  // 1. Move the files of the old index (including its materialized views and
  //    its build log) from `config.oldIndexSource()` to
  //    `config.oldIndexTarget()`.
  // 2. Move the files of the freshly rebuilt index from
  //    `config.newIndexSource()` to `config.newIndexTarget()`.
  // 3. Re-anchor all path-derived state of the new index in memory (on-disk
  //    base name, files for persisted updates and graph names, and the views
  //    manager) to `config.newIndexTarget()`.
  // 4. Remove the directory that contained `config.newIndexSource()`, which
  //    step 2 has emptied (if it is actually empty). A failure here is only
  //    logged as a warning.
  //
  // Typically, `config.newIndexTarget()` is `config.oldIndexSource()`, i.e. the
  // new index is served from the place of the old index (so that a later
  // restart loads the latest index); this works because step 1 has already
  // freed that place. Existing files that may still exist at
  // `config.oldIndexTarget()` or `config.newIndexTarget()` may be overwritten,
  // so callers have to make sure the directories to write to are safe. The
  // renames keep the open file handles of both indices valid, so running
  // queries are not affected. This must be called BEFORE swapping in the new
  // `IndexAndViews`, and with the guarantee that no updates are added
  // concurrently (an update between the renames and the re-anchoring would
  // persist to the old path). If this throws halfway through, the in-memory
  // state still refers to a consistent old index, but some files will have been
  // moved and others won't, so when restarting, files need to be moved into the
  // proper directory first. This should realistically never happen since all
  // this function does is string concatenation and moving files around. This
  // function assumes that file handles are never reopened, so moving the files
  // while the file handle is still open is fine in POSIX compliant systems.
  static void moveRebuiltIndexIntoPlace(IndexAndViews& newIndexAndViews,
                                        const IndexRebuildConfig& config);

  // The result of the first phase of an index rebuild (see
  // `rebuildIndexToDisk`): a snapshot of the delta triples taken at the start
  // of the rebuild, the mapping from the old vocabulary `Id`s to the new ones,
  // and the freshly built index (loaded from disk) paired with a fresh, empty
  // `MaterializedViewsManager`.
  using RebuildResult =
      std::tuple<LocatedTriplesSharedState, indexRebuilder::IndexRebuildMapping,
                 std::shared_ptr<IndexAndViews>>;

  // The two functions below implement an index rebuild. They are only available
  // in the C++20 build. They rely on `materializeToIndex` and
  // `DeltaTriples::addFromSnapshotDiff`, which are excluded from the reduced
  // C++17 feature set.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

  // Build a new index from the current state of `index` and write it to disk
  // under the base name `config.newIndexSource()` (the containing directory is
  // created if it does not exist), then load it into a fresh `IndexAndViews`.
  // This is the expensive, read-only first phase of an index rebuild. It
  // returns the data required by `swapInRebuiltIndex` to atomically switch over
  // to the new index. `handle` can be used to cancel the rebuild. The reason
  // why `index` has to be passed in manually instead of using
  // `indexAndViewsSnapshot()` is to avoid a TOCTOU class of bugs.
  [[nodiscard]] RebuildResult rebuildIndexToDisk(
      Index& index, const IndexRebuildConfig& config,
      const ad_utility::SharedCancellationHandle& handle) const;

  // Remap the delta triples that accumulated on the old `index` (which has to
  // be the exact same index that was used to create `rebuildResult`) onto the
  // freshly built index (using the `rebuildResult` produced by
  // `rebuildIndexToDisk`) and atomically swap the new index in. Calling this
  // also persists the remapped delta triples to disk so that they are not lost
  // if the engine is later restarted on the rebuilt index. The reason why
  // `index` has to be passed in manually instead of using
  // `indexAndViewsSnapshot()` is to avoid a TOCTOU class of bugs. It is crucial
  // that this function is only called when you can guarantee no updates are
  // added during the duration of this function call.
  //
  // Before the swap, `moveRebuiltIndexIntoPlace` is called, which moves the
  // files of the old index to `config.oldIndexTarget()` and the files of the
  // new index from `config.newIndexSource()` to `config.newIndexTarget()` (by
  // default the place of the old index) and removes the directory in which the
  // new index was built.
  void swapInRebuiltIndex(const Index& index, RebuildResult rebuildResult,
                          const ad_utility::SharedCancellationHandle& handle,
                          const IndexRebuildConfig& config);
#endif

  QueryResultCache& cache() { return cache_; }
  const QueryResultCache& cache() const { return cache_; }

  ad_utility::AllocatorWithLimit<Id>& allocator() { return allocator_; }
  const ad_utility::AllocatorWithLimit<Id>& allocator() const {
    return allocator_;
  }

  SortPerformanceEstimator& sortPerformanceEstimator() {
    return sortPerformanceEstimator_;
  }
  const SortPerformanceEstimator& sortPerformanceEstimator() const {
    return sortPerformanceEstimator_;
  }

  NamedResultCache& namedResultCache() { return namedResultCache_; }
  const NamedResultCache& namedResultCache() const { return namedResultCache_; }
};
}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_QLEVER_H
