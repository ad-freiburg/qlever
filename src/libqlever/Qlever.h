//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <index/vocabulary/VocabularyType.h>
#include <util/MemorySize/MemorySize.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/NamedQueryCache.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "index/Index.h"
#include "index/InputFileSpecification.h"
#include "index/vocabulary/VocabularyType.h"
#include "parser/SparqlParser.h"
#include "util/AllocatorWithLimit.h"
#include "util/http/MediaTypes.h"

namespace qlever {

// A configuration for a QLever instance.
struct QleverConfig {
  // A basename for all files that QLever will write as part of the index
  // building.
  std::string baseName;

  // The specification of the input files (Turtle/NT or NQuad) from which the
  // index will be built.
  std::vector<qlever::InputFileSpecification> inputFiles;

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

  // Optionally a filename to a .json file with additional settings...
  // TODO<joka921> Make these settings part of this struct directly
  // TODO<joka921> Document these additional settings.
  std::string settingsFile;

  // Specify whether the vocabulary is stored on disk or in RAM, compressed or
  // uncompressed.
  ad_utility::VocabularyType vocabularyType_{
      ad_utility::VocabularyType::Enum::CompressedOnDisk};

  // The following members are only required if QLever's full-text search
  // extension is to be used, see `IndexBuilderMain.cpp` for additional details.
  bool addWordsFromLiterals = false;
  std::string kbIndexName;
  std::string wordsfile;
  std::string docsfile;
  std::string textIndexName;
  bool onlyAddTextIndex = false;

  // If set to true, then certain temporary files which are created while
  // building the index are not deleted. This can be useful for debugging.
  bool keepTemporaryFiles = false;
};

// A class that can be used to use QLever without the HTTP server, e.g. as part
// of another program.
class Qlever {
 private:
  QueryResultCache cache_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  SortPerformanceEstimator sortPerformanceEstimator_;
  Index index_;
  NamedQueryCache namedQueryCache_;
  bool enablePatternTrick_;
  static inline std::ostringstream ignoreLogStream;

 public:
  // Build a persistent on disk index using the `config`.
  static void buildIndex(QleverConfig config);

  // Load the qlever index from file.
  explicit Qlever(const QleverConfig& config);

  // Run the given query on the index. Currently only SELECT and ASK queries are
  // supported, and the result will always be in sparql-results+json format.
  // TODO<joka921> Support other formats + CONSTRUCT queries, support
  // cancellation, time limits, and observable queries.
  std::string query(std::string query, ad_utility::MediaType mediaType =
                                           ad_utility::MediaType::sparqlJson);

  using QueryPlan =
      std::tuple<std::shared_ptr<QueryExecutionTree>,
                 std::shared_ptr<QueryExecutionContext>, ParsedQuery>;
  using QueryOrPlan = std::variant<std::string, QueryPlan>;

  // Pin a query to the named query cache. In a subsequent query, this cache can
  // be accessed via `SERVICE ql:
  [[maybe_unused]] std::string pinNamed(
      QueryOrPlan query, std::string name,
      ad_utility::MediaType mediaType = ad_utility::MediaType::sparqlJson,
      bool returnResult = true);

  QueryPlan parseAndPlanQuery(std::string query);
};
}  // namespace qlever
