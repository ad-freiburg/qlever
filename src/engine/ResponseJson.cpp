// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ResponseJson.h"

#include "CompilationInfo.h"
#include "backports/filesystem.h"
#include "global/Constants.h"
#include "util/StringUtils.h"

namespace responseJson {

// _____________________________________________________________________________
json composeIndexStats(const Index& index) {
  json result;
  result["name-index"] = index.getKbName();
  result["git-hash-index"] = index.getGitShortHash();
  result["git-hash-server"] =
      *qlever::version::gitShortHashWithoutLinking.wlock();
  result["version-server"] =
      *qlever::version::projectVersionWithoutLinking.wlock();
  result["num-permutations"] = (index.hasAllPermutations() ? 6 : 2);
  result["num-predicates-normal"] = index.numDistinctPredicates().normal;
  result["num-predicates-internal"] = index.numDistinctPredicates().internal;
  if (index.hasAllPermutations()) {
    result["num-subjects-normal"] = index.numDistinctSubjects().normal;
    result["num-subjects-internal"] = index.numDistinctSubjects().internal;
    result["num-objects-normal"] = index.numDistinctObjects().normal;
    result["num-objects-internal"] = index.numDistinctObjects().internal;
  }

  auto numTriples = index.numTriples();
  result["num-triples-normal"] = numTriples.normal;
  result["num-triples-internal"] = numTriples.internal;
  result["name-text-index"] = index.getTextName();
  result["num-text-records"] = index.getNofTextRecords();
  result["num-word-occurrences"] = index.getNofWordPostings();
  result["num-entity-occurrences"] = index.getNofEntityPostings();
  return result;
}

// _____________________________________________________________________________
json composeCacheStats(const QueryResultCache& cache,
                       const NamedResultCache& namedResultCache) {
  json result;
  result["num-results-unpinned"] = cache.numNonPinnedEntries();
  result["num-results-pinned-unnamed"] = cache.numPinnedEntries();
  result["num-results-pinned-named"] = namedResultCache.numEntries();

  // TODO: Get rid of the `getByte()`, once `MemorySize` has it's own JSON
  // converter.
  result["cache-size-unpinned"] = cache.nonPinnedSize().getBytes();
  result["cache-size-pinned"] = cache.pinnedSize().getBytes();
  return result;
}

// _____________________________________________________________________________
json composeRebuildSuccess(const qlever::IndexSwapConfig& config) {
  json result;
  result["message"] = "Index successfully rebuilt and swapped in";
  // Report the directory (not the full base name): it mirrors the
  // `rebuild-previous-index-dir` command parameter and is the one piece of
  // information the client cannot know in advance (the default is derived from
  // the build date of the old index). The new index is not mentioned because
  // it is always served from the base name of the old one.
  result["previous-index-dir"] =
      ql::filesystem::path{config.oldIndexTarget()}.parent_path().string();
  return result;
}

// _____________________________________________________________________________
json composeError(const std::string& query, const std::string& errorMsg,
                  const ad_utility::Timer& requestTimer,
                  const std::optional<ExceptionMetadata>& metadata) {
  json j;
  j["query"] = ad_utility::truncateOperationString(query);
  j["status"] = "ERROR";
  j["resultsize"] = 0;
  j["time"]["total"] = requestTimer.msecs().count();
  j["time"]["computeResult"] = requestTimer.msecs().count();
  j["exception"] = errorMsg;

  // If the error location is truncated don't send its location.
  if (metadata.has_value() &&
      metadata.value().stopIndex_ < MAX_LENGTH_OPERATION_ECHO) {
    auto& value = metadata.value();
    j["metadata"]["startIndex"] = value.startIndex_;
    j["metadata"]["stopIndex"] = value.stopIndex_;
    j["metadata"]["line"] = value.line_;
    j["metadata"]["positionInLine"] = value.charPositionInLine_;
  }

  return j;
}

}  // namespace responseJson
