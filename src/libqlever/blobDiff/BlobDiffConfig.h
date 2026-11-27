// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFCONFIG_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFCONFIG_H

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "index/vocabulary/VocabularyType.h"
#include "libqlever/blobDiff/BlobDiff.h"

namespace qlever {

// The configuration of the weekly blob diff workflow (the producer of the
// diffs is added in a follow-up PR).
// It is read from a YAML file of the following form (all keys of `index`,
// `blob`, and `geoCompaction` are optional, unknown keys are rejected):
//
// index:
//   vocabularyType: in-memory-compressed
//   numThreads: 4
//   blankNodeIriRegexes: []
//   prefixesForIdEncodedIris: []
//   settingsFile: ""
// blob:
//   excludedEntryRegexes: []
//   diffCompressionLevel: 19
// geoCompaction:
//   maxSegments: 8
//   maxDeadShapeRatio: 0.3
// namedQueries:
//   - name: streets
//     query: SELECT ...
//     geoIndexVar: "?wkt"
//     simplificationMeters: 1.0
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

  // The settings of the `index` section. They are passed on unchanged to the
  // fields of `IndexBuilderConfig` (see `Qlever.h`) with the same names (the
  // `vocabularyType_` to its `vocabType_`), which correspond to the options of
  // `qlever-index` that are named below. See there for their meaning.

  // The vocabulary of the index (`--vocabulary-type`). It has to be one that is
  // held in memory, because the on-disk vocabularies cannot be serialized into
  // a blob.
  ad_utility::VocabularyType vocabularyType_{
      ad_utility::VocabularyType::Enum::InMemoryCompressed};
  // The number of threads for building the index (`--num-threads`, the default
  // of `IndexBuilderConfig` if unset).
  std::optional<size_t> numThreads_;
  // The regexes of the IRIs that are treated as blank nodes
  // (`--iri-as-blank-node-regexes`).
  std::vector<std::string> blankNodeIriRegexes_;
  // The IRI prefixes whose IRIs (the prefix followed by digits) are encoded
  // directly into their `Id` (`--encode-as-id`).
  std::vector<std::string> prefixesForIdEncodedIris_;
  // The JSON file with further settings of the index build
  // (`--settings-file`), no file if empty.
  std::string settingsFile_;

  // The vocabulary entries that are left out of the blob, see
  // `BlobSerializationConfig::excludedEntryRegexes_`.
  std::vector<std::string> excludedEntryRegexes_;
  // The ZSTD compression level of the diff files (1 to 22), see
  // `blobDiff::computeBlobDiff`.
  int diffCompressionLevel_ = blobDiff::defaultBlobDiffCompressionLevel;

  // The compaction thresholds for the geo indices, see
  // `BlobSerializationConfig::IncrementalBase`.
  size_t maxGeoSegments_ = 8;
  double maxDeadShapeRatio_ = 0.3;

  std::vector<NamedQuery> namedQueries_;

  // Parse and validate a config from its YAML representation. Throw with a
  // descriptive message if the YAML is malformed, contains unknown keys, has
  // duplicate query names, a geo variable that does not start with `?`, a
  // simplification that is not positive, or an on-disk vocabulary type.
  static BlobDiffConfig fromYaml(std::string_view yaml);

  // Read the file `path` and call `fromYaml` on its content.
  static BlobDiffConfig fromFile(const std::string& path);

  // Return the config as YAML (the inverse of `fromYaml`).
  std::string toYaml() const;

  // Return a hex string that identifies all settings that influence the
  // content of the index and of the decompressed blob (everything except the
  // geo compaction thresholds, the number of threads, and the compression
  // level of the diffs). It is stored in the state of the producer of the
  // diffs to detect that the config changed between two weeks.
  std::string fingerprint() const;
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFCONFIG_H
