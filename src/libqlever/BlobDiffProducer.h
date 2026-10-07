// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFFPRODUCER_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFFPRODUCER_H

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "backports/span.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/json.h"

namespace qlever {

// The configuration of the weekly blob diff workflow (see `BlobDiffProducer`).
// It is read from a JSON file of the following form (all keys of `index`,
// `blob`, and `geoCompaction` are optional, unknown keys are rejected):
//
// {
//   "index": {
//     "vocabularyType": "in-memory-compressed",
//     "numThreads": 4,
//     "blankNodeIriRegexes": [],
//     "prefixesForIdEncodedIris": [],
//     "settingsFile": ""
//   },
//   "blob": { "excludedEntryRegexes": [] },
//   "geoCompaction": { "maxSegments": 8, "maxDeadShapeRatio": 0.3 },
//   "namedQueries": [
//     { "name": "streets", "query": "SELECT ...",
//       "geoIndexVar": "?wkt", "simplificationMeters": 1.0 }
//   ]
// }
struct BlobDiffConfig {
  // A query whose result is pinned in the named result cache (and thereby
  // becomes part of the blob), optionally with a geo index.
  struct NamedQuery {
    std::string name_;
    std::string query_;
    // The variable (including the leading `?`) with the geometries that are
    // indexed.
    std::optional<std::string> geoIndexVar_;
    // The maximal error in meters of the simplification of the indexed
    // geometries. Only allowed together with `geoIndexVar_`.
    std::optional<double> simplificationMeters_;
  };

  // The vocabulary of the index. It has to be one that is held in memory,
  // because the on-disk vocabularies cannot be serialized into a blob.
  ad_utility::VocabularyType vocabularyType_{
      ad_utility::VocabularyType::Enum::InMemoryCompressed};
  // The number of threads for building the index (the default if unset).
  std::optional<size_t> numThreads_;
  std::vector<std::string> blankNodeIriRegexes_;
  std::vector<std::string> prefixesForIdEncodedIris_;
  std::string settingsFile_;

  // The vocabulary entries that are left out of the blob, see
  // `BlobSerializationConfig::excludedEntryRegexes_`.
  std::vector<std::string> excludedEntryRegexes_;

  // The compaction thresholds for the geo indices, see
  // `BlobSerializationConfig::IncrementalBase`.
  size_t maxGeoSegments_ = 8;
  double maxDeadShapeRatio_ = 0.3;

  std::vector<NamedQuery> namedQueries_;

  // Parse and validate a config. Throw with a descriptive message if the JSON
  // is malformed, contains unknown keys, has duplicate query names, a geo
  // variable that does not start with `?`, a simplification that is not
  // positive, or an on-disk vocabulary type.
  static BlobDiffConfig fromJson(const nlohmann::json& json);

  // Read the file `path` and call `fromJson` on its content.
  static BlobDiffConfig fromFile(const std::string& path);

  // Return the config as JSON (the inverse of `fromJson`).
  nlohmann::json toJson() const;

  // Return a hex string that identifies all settings that influence the
  // content of the index and of the blob (everything except the geo
  // compaction thresholds). It is stored in the state of the producer to detect
  // that the config changed between two weeks.
  std::string fingerprint() const;
};

// The producer side of the weekly workflow: it holds the state that is needed
// to turn the data of week `k` into a blob `B_k` and a small diff file `D_k`,
// such that the consumer, who only holds the blob `B_{k-1}`, can compute `B_k`
// by `apply(B_{k-1}, D_k)` (byte for byte). All functions are static, the whole
// state lives in a state directory.
//
// WHAT THE PRODUCER KEEPS between weeks (all in the state directory):
//  1. The index of week 0 (`index/index.*`, never modified after `init`).
//  2. The persisted delta triples (`index/index.update-triples`), which
//     contain the net difference of all following weeks to the index of week 0
//     (inserted and deleted triples, and their words that are not in the
//     vocabulary of week 0).
//  3. `current.ttl`, the Turtle file of the current week. The next week's
//     Turtle file is compared against it to obtain the triples to insert and
//     to delete.
//  4. `current.blob`, the current compressed blob. It is the base for the
//     incremental serialization (which keeps the `Id`s of the words of the
//     secondary vocabulary stable) and for the diff.
//  5. `state.json`: the week counter, timestamps, sizes, and the fingerprint of
//     the config. The config itself is passed on every call and has to be the
//     same in every week (its fingerprint is checked).
//
// WHAT THE CONSUMER KEEPS: only its current compressed blob (`B_{k-1}`), and
// afterwards the one it has computed. No index files and no Turtle files.
//
// IMPORTANT: The blob is only a small diff to the previous one if the main
// vocabulary (and hence all `Id`s) of week 0 stay valid. That is why the
// producer never rebuilds the index but applies the weekly changes as delta
// triples to the index of week 0 (with new words going to the append-only
// secondary vocabulary of the blob).
//
// NOTE: The query plan of a pinned query (and thereby the order of the columns
// and the sort order of its result) may differ between two runs, even for the
// same data. The producer therefore writes every entry with its columns ordered
// by variable name and its rows sorted by all columns (see
// `BlobSerializationConfig::sortOnAllColumns_`), which makes the blobs, and
// hence the diffs, independent of the plan. The sort order that the plan
// produced is not preserved in the blob.
class BlobDiffProducer {
 public:
  // The result of one weekly step.
  struct WeeklyResult {
    // The new compressed blob and the compressed diff file that turns the
    // previous blob into it.
    std::vector<char> blob_;
    std::vector<char> diffFile_;
    // A human-readable description of the diff (see `describeBlobDiff`).
    std::string statistics_;
    // The number of triples that were inserted/deleted in this step.
    size_t numInsertedTriples_ = 0;
    size_t numDeletedTriples_ = 0;
  };

  // Week 0: Build the index from `turtleFile` into the (not existing or empty)
  // `stateDir`, pin the queries of `config`, write the first blob (without a
  // base) to `current.blob`, and create the remaining state files. Return the
  // compressed blob. Throw if `stateDir` is not empty.
  static std::vector<char> init(const BlobDiffConfig& config,
                                const std::string& turtleFile,
                                const std::string& stateDir);

  // Week `k > 0`: Apply the difference between the previous Turtle file and
  // `newTurtleFile` as delta triples to the index, pin the queries again, write
  // the new blob incrementally against the previous one, compute the diff, and
  // verify that applying it to the previous blob yields the new blob exactly
  // (otherwise throw, and leave the state unchanged). On success, rotate the
  // state (see above). If `forceCompaction` is true, the geo indices are
  // rebuilt as a single segment each, so this week's diff is larger, but the
  // following weeks start from compact indices.
  static WeeklyResult step(const BlobDiffConfig& config,
                           const std::string& newTurtleFile,
                           const std::string& stateDir,
                           bool forceCompaction = false);

  // The consumer side: apply the diff file to `baseBlob` and return the new
  // compressed blob. Throw if the diff does not belong to the base.
  static std::vector<char> apply(ql::span<const char> baseBlob,
                                 ql::span<const char> diffFile);

  // Return a description of the layout (sizes of all sections) of `blob`, and,
  // if a `diffFile` is given, of how its instructions make up `blob` (which has
  // to be the target of the diff).
  static std::string inspect(ql::span<const char> blob,
                             std::optional<ql::span<const char>> diffFile);
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFFPRODUCER_H
