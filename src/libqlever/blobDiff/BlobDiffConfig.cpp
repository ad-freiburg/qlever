// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/BlobDiffConfig.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <yaml-cpp/yaml.h>

#include <cmath>
#include <stdexcept>
#include <string>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "rdfTypes/Variable.h"
#include "util/Algorithm.h"
#include "util/CryptographicHashUtils.h"
#include "util/File.h"
#include "util/HashSet.h"

namespace qlever {

namespace {
// Throw a `std::runtime_error` that starts with a common prefix.
[[noreturn]] void configError(const std::string& message) {
  throw std::runtime_error{absl::StrCat("Invalid blob diff config: ", message)};
}

// Return true if `node` has the key `key` with a value that is not null (a key
// without a value is treated like a missing key).
bool hasKey(const YAML::Node& node, const std::string& key) {
  const YAML::Node value = node[key];
  return value.IsDefined() && !value.IsNull();
}

// Throw if `node` is not a map, has a key that is not in `allowedKeys`, or has
// a key more than once. `context` describes where `node` is located in the
// config.
//
// NOTE: YAML forbids duplicate keys, but `yaml-cpp` accepts them, and
// `node[key]` then silently returns the first value. The iteration below,
// however, visits all entries, including the duplicates.
void checkKeys(const YAML::Node& node, const std::string& context,
               const std::vector<std::string>& allowedKeys) {
  if (!node.IsMap()) {
    configError(absl::StrCat("`", context, "` has to be a YAML map."));
  }
  ad_utility::HashSet<std::string> seenKeys;
  for (const auto& entry : node) {
    if (!entry.first.IsScalar()) {
      configError(
          absl::StrCat("`", context, "` has a key that is not a string."));
    }
    const auto& key = entry.first.Scalar();
    if (!ad_utility::contains(allowedKeys, key)) {
      configError(absl::StrCat(
          "unknown key `", key, "` in `", context,
          "`. The allowed keys are: ", absl::StrJoin(allowedKeys, ", "), "."));
    }
    if (!seenKeys.insert(key).second) {
      configError(
          absl::StrCat("the key `", key, "` occurs twice in `", context, "`."));
    }
  }
}

// Return the value of `node[key]` converted to `T`, or `defaultValue` if the
// key is missing. Throw with a message that names the key if the conversion
// fails.
template <typename T>
T getOr(const YAML::Node& node, const std::string& key,
        const std::string& context, T defaultValue) {
  if (!hasKey(node, key)) {
    return defaultValue;
  }
  try {
    return node[key].as<T>();
  } catch (const YAML::Exception& e) {
    configError(absl::StrCat("the value of `", context, ".", key,
                             "` has the wrong type (", e.what(), ")."));
  }
}

// Parse and validate the root node of a config.
BlobDiffConfig parseConfig(const YAML::Node& yaml) {
  using NamedQuery = BlobDiffConfig::NamedQuery;
  checkKeys(yaml, "<root>", {"index", "blob", "geoCompaction", "namedQueries"});
  BlobDiffConfig config;
  if (hasKey(yaml, "index")) {
    const YAML::Node index = yaml["index"];

    checkKeys(index, "index",
              {"vocabularyType", "numThreads", "blankNodeIriRegexes",
               "prefixesForIdEncodedIris", "settingsFile"});
    if (hasKey(index, "vocabularyType")) {
      auto name = getOr<std::string>(index, "vocabularyType", "index", "");
      try {
        config.vocabularyType_ = ad_utility::VocabularyType::fromString(name);
      } catch (const std::exception& e) {
        configError(absl::StrCat("`index.vocabularyType`: ", e.what()));
      }
      using Enum = ad_utility::VocabularyType::Enum;
      auto type = config.vocabularyType_.value();
      if (type != Enum::InMemoryCompressed &&
          type != Enum::InMemoryUncompressed) {
        configError(absl::StrCat(
            "the vocabulary type `", name,
            "` cannot be used, because a vocabulary that is stored on disk "
            "cannot be serialized into a blob. Use `in-memory-compressed` or "
            "`in-memory-uncompressed`."));
      }
    }
    if (hasKey(index, "numThreads")) {
      size_t numThreads = getOr<size_t>(index, "numThreads", "index", 0);
      if (numThreads == 0) {
        configError("`index.numThreads` has to be positive.");
      }
      config.numThreads_ = numThreads;
    }
    config.blankNodeIriRegexes_ = getOr<std::vector<std::string>>(
        index, "blankNodeIriRegexes", "index", {});
    config.prefixesForIdEncodedIris_ = getOr<std::vector<std::string>>(
        index, "prefixesForIdEncodedIris", "index", {});
    config.settingsFile_ =
        getOr<std::string>(index, "settingsFile", "index", "");
  }
  if (hasKey(yaml, "blob")) {
    const YAML::Node blob = yaml["blob"];
    checkKeys(blob, "blob", {"excludedEntryRegexes", "diffCompressionLevel"});
    config.excludedEntryRegexes_ = getOr<std::vector<std::string>>(
        blob, "excludedEntryRegexes", "blob", {});
    config.diffCompressionLevel_ = getOr<int>(
        blob, "diffCompressionLevel", "blob", config.diffCompressionLevel_);
    if (config.diffCompressionLevel_ < 1 || config.diffCompressionLevel_ > 22) {
      configError("`blob.diffCompressionLevel` has to be in [1, 22].");
    }
  }
  if (hasKey(yaml, "geoCompaction")) {
    const YAML::Node geo = yaml["geoCompaction"];
    checkKeys(geo, "geoCompaction", {"maxSegments", "maxDeadShapeRatio"});
    config.maxGeoSegments_ = getOr<size_t>(geo, "maxSegments", "geoCompaction",
                                           config.maxGeoSegments_);
    if (config.maxGeoSegments_ == 0) {
      configError("`geoCompaction.maxSegments` has to be positive.");
    }
    config.maxDeadShapeRatio_ = getOr<double>(
        geo, "maxDeadShapeRatio", "geoCompaction", config.maxDeadShapeRatio_);
    // NOTE: The negated form also rejects NaN.
    if (!(config.maxDeadShapeRatio_ >= 0.0 &&
          config.maxDeadShapeRatio_ <= 1.0)) {
      configError("`geoCompaction.maxDeadShapeRatio` has to be in [0, 1].");
    }
  }
  if (!hasKey(yaml, "namedQueries") || !yaml["namedQueries"].IsSequence()) {
    configError("`namedQueries` has to be a YAML sequence.");
  }
  ad_utility::HashSet<std::string> names;
  for (const auto& entry : yaml["namedQueries"]) {
    checkKeys(entry, "namedQueries[]",
              {"name", "query", "geoIndexVar", "simplificationMeters"});
    NamedQuery query;
    query.name_ = getOr<std::string>(entry, "name", "namedQueries[]", "");
    query.query_ = getOr<std::string>(entry, "query", "namedQueries[]", "");
    if (query.name_.empty() ||
        query.name_.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN"
                                      "OPQRSTUVWXYZ0123456789_-") !=
            std::string::npos) {
      configError(absl::StrCat(
          "the name of a named query (here `", query.name_,
          "`) has to be non-empty and consist of letters, digits, `_` and "
          "`-`, because it is used in `ql:cached-result-with-name-<name>`."));
    }
    if (query.query_.empty()) {
      configError(
          absl::StrCat("the named query `", query.name_, "` has no `query`."));
    }
    if (!names.insert(query.name_).second) {
      configError(absl::StrCat("the name `", query.name_, "` is used twice."));
    }
    if (hasKey(entry, "geoIndexVar")) {
      auto var = getOr<std::string>(entry, "geoIndexVar", "namedQueries[]", "");
      if (!ql::starts_with(var, "?") || !Variable::isValidVariableName(var)) {
        configError(absl::StrCat("`geoIndexVar` of the named query `",
                                 query.name_,
                                 "` has to be a valid variable that starts "
                                 "with `?`, but is `",
                                 var, "`."));
      }
      query.geoIndexVar_ = std::move(var);
    }
    if (hasKey(entry, "simplificationMeters")) {
      double meters =
          getOr<double>(entry, "simplificationMeters", "namedQueries[]", 0.0);
      // NOTE: The negated form also rejects NaN.
      if (!(meters > 0.0) || !std::isfinite(meters)) {
        configError(absl::StrCat("`simplificationMeters` of the named query `",
                                 query.name_,
                                 "` has to be positive and finite."));
      }
      if (!query.geoIndexVar_.has_value()) {
        configError(absl::StrCat("the named query `", query.name_,
                                 "` has a `simplificationMeters`, but no "
                                 "`geoIndexVar`."));
      }
      query.simplificationMeters_ = meters;
    }
    config.namedQueries_.push_back(std::move(query));
  }
  return config;
}

// Return the config as a YAML node (the inverse of `parseConfig`).
YAML::Node toNode(const BlobDiffConfig& config) {
  YAML::Node yaml;
  YAML::Node index = yaml["index"];
  index["vocabularyType"] = std::string{config.vocabularyType_.toString()};
  if (config.numThreads_.has_value()) {
    index["numThreads"] = config.numThreads_.value();
  }
  index["blankNodeIriRegexes"] = config.blankNodeIriRegexes_;
  index["prefixesForIdEncodedIris"] = config.prefixesForIdEncodedIris_;
  index["settingsFile"] = config.settingsFile_;
  YAML::Node blob = yaml["blob"];
  blob["excludedEntryRegexes"] = config.excludedEntryRegexes_;
  blob["diffCompressionLevel"] = config.diffCompressionLevel_;
  YAML::Node geo = yaml["geoCompaction"];
  geo["maxSegments"] = config.maxGeoSegments_;
  geo["maxDeadShapeRatio"] = config.maxDeadShapeRatio_;
  YAML::Node queries{YAML::NodeType::Sequence};
  for (const auto& query : config.namedQueries_) {
    YAML::Node entry;
    entry["name"] = query.name_;
    entry["query"] = query.query_;
    if (query.geoIndexVar_.has_value()) {
      entry["geoIndexVar"] = query.geoIndexVar_.value();
    }
    if (query.simplificationMeters_.has_value()) {
      entry["simplificationMeters"] = query.simplificationMeters_.value();
    }
    queries.push_back(entry);
  }
  yaml["namedQueries"] = queries;
  return yaml;
}

// Parse the YAML `input` (a string or a stream), which has to consist of
// exactly one document. `source` names the `input` in error messages.
template <typename Input>
YAML::Node loadSingleDocument(Input& input, std::string_view source) {
  std::vector<YAML::Node> documents;
  try {
    documents = YAML::LoadAll(input);
  } catch (const YAML::Exception& e) {
    configError(absl::StrCat(source, " is not valid YAML (", e.what(), ")."));
  }
  if (documents.size() != 1) {
    configError(absl::StrCat(source,
                             " has to consist of exactly one YAML document, "
                             "but consists of ",
                             documents.size(), "."));
  }
  return documents.front();
}

// Return `node` as a YAML string.
std::string emit(const YAML::Node& node) {
  YAML::Emitter emitter;
  emitter << node;
  return emitter.c_str();
}
}  // namespace

// _____________________________________________________________________________
BlobDiffConfig BlobDiffConfig::fromYaml(std::string_view yaml) {
  std::string input{yaml};
  return parseConfig(loadSingleDocument(input, "the config"));
}

// _____________________________________________________________________________
BlobDiffConfig BlobDiffConfig::fromFile(const std::string& path) {
  auto stream = ad_utility::makeIfstream(path);
  return parseConfig(
      loadSingleDocument(stream, absl::StrCat("the file `", path, "`")));
}

// _____________________________________________________________________________
std::string BlobDiffConfig::toYaml() const { return emit(toNode(*this)); }

// _____________________________________________________________________________
nlohmann::json BlobDiffConfig::readSettingsJson() const {
  if (settingsFile_.empty()) {
    return nlohmann::json::object();
  }
  try {
    return nlohmann::json::parse(ad_utility::makeIfstream(settingsFile_));
  } catch (const nlohmann::json::exception& e) {
    configError(absl::StrCat("the settings file `", settingsFile_,
                             "` is not valid JSON (", e.what(), ")."));
  }
}

// _____________________________________________________________________________
std::string BlobDiffConfig::fingerprint() const {
  auto yaml = toNode(*this);
  // Remove the settings that influence neither the content of the index nor
  // the decompressed blob: the geo compaction thresholds, the number of
  // threads, and the compression level of the diffs.
  yaml.remove("geoCompaction");
  yaml["index"].remove("numThreads");
  yaml["blob"].remove("diffCompressionLevel");
  // Replace the path of the settings file by its content.
  yaml["index"].remove("settingsFile");
  yaml["index"]["settingsJson"] = readSettingsJson().dump();
  auto digest = ad_utility::HashSha256{}(emit(yaml));
  return absl::StrJoin(digest, "", ad_utility::hexFormatter);
}

}  // namespace qlever
