//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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
#include "index/vocabulary/VocabularyTypes.h"
#include "util/AllocatorWithLimit.h"
#include "util/http/MediaTypes.h"

namespace qlever {

// The common configuration shared by the index building and query execution.
struct CommonConfig {
  // A basename for all files that QLever will write as part of the index
  // building.
  std::string baseName;

  // A memory limit that will be applied during the index building as well as
  // during the query processing.
  std::optional<ad_utility::MemorySize> memoryLimit =
      ad_utility::MemorySize::gigabytes(1);

  // If set to true, then no so-called patterns will be built. Patterns are
  // useful for autocompletion and for certain statistics queries, but not for
  // typical SELECT queries.
  bool noPatterns = false;

  // Only build two permutations. This is sufficient if all queries have a fixed
  // predicate.
  // TODO<joka921> We haven't tested this mode in a while, it is currently
  // probably broken because the UPDATE mechanism doesn't support only two
  // permutations.
  bool onlyPsoAndPos = false;
};

// The configuration required for building an index.
struct IndexBuilderConfig : CommonConfig {
  // The specification of the input files (Turtle/NT or NQuad) from which the
  // index will be built.
  std::vector<qlever::InputFileSpecification> inputFiles;

  std::optional<ad_utility::MemorySize> parserBufferSize;

  // Optionally a filename to a .json file with additional settings...
  // TODO<joka921> Make these settings part of this struct directly
  // TODO<joka921> Document these additional settings.
  std::string settingsFile;

  ad_utility::VocabularyType vocabType{
      ad_utility::VocabularyType::Enum::OnDiskCompressed};

  // The following members are only required if QLever's full-text search
  // extension is to be used, see `IndexBuilderMain.cpp` for additional details.
  bool addWordsFromLiterals = false;
  std::string kbIndexName = "no index name specified";
  std::string wordsfile;
  std::string docsfile;
  std::string textIndexName;
  bool onlyAddTextIndex = false;

  // If set to true, then certain temporary files which are created while
  // building the index are not deleted. This can be useful for debugging.
  bool keepTemporaryFiles = false;

  TextScoringMetric textScoringMetric = TextScoringMetric::EXPLICIT;
  float bScoringParam = 0.75;
  float kScoringParam = 1.75;

  // Assert that the given configuration is valid.
  void validate();
};

// The configuration required for executing queries on a previously built index.
struct EngineConfig : CommonConfig {
  explicit EngineConfig(const IndexBuilderConfig& c)
      : CommonConfig{static_cast<const CommonConfig&>(c)} {}
  EngineConfig() = default;
  bool text = false;
  bool persistUpdates = true;
};

// A class that can be used to use QLever without the HTTP server, e.g. as part
// of another program.
class Qlever {
 private:
  QueryResultCache cache_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  SortPerformanceEstimator sortPerformanceEstimator_;
  Index index_;
  bool enablePatternTrick_;
  static inline std::ostringstream ignoreLogStream;

 public:
  // Build a persistent on disk index using the `config`.
  static void buildIndex(IndexBuilderConfig config);

  // Load the qlever index from file.
  explicit Qlever(const EngineConfig& config);

  using QueryPlan =
      std::tuple<std::shared_ptr<QueryExecutionTree>,
                 std::shared_ptr<QueryExecutionContext>, ParsedQuery>;

  // Parse the given query plan and run the query planner for it.
  QueryPlan parseAndPlanQuery(std::string query);

  // Run the given query on the index. Currently only queries, but no updates
  // are supported.
  // TODO<joka921> Support UPDATE, support
  // cancellation, time limits, and observable queries via runtime information.
  std::string query(std::string query, ad_utility::MediaType mediaType =
                                           ad_utility::MediaType::sparqlJson);

  // Same as `query` above, but takes a previously preconstructed `QueryPlan`
  std::string query(
      const QueryPlan& queryPlan,
      ad_utility::MediaType mediaType = ad_utility::MediaType::sparqlJson);
};
}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_QLEVER_H
