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

// Throw if `json` is not an object or has a key that is not in `allowedKeys`.
// `context` describes where `json` is located in the config.
void checkKeys(const nlohmann::json& json, const std::string& context,
               const std::vector<std::string>& allowedKeys) {
  if (!json.is_object()) {
    configError(absl::StrCat("`", context, "` has to be a JSON object."));
  }
  for (const auto& [key, value] : json.items()) {
    if (!ad_utility::contains(allowedKeys, key)) {
      configError(absl::StrCat(
          "unknown key `", key, "` in `", context,
          "`. The allowed keys are: ", absl::StrJoin(allowedKeys, ", "), "."));
    }
  }
}

// Return the value of `json[key]` converted to `T`, or `defaultValue` if the
// key is missing. Throw with a message that names the key if the conversion
// fails.
template <typename T>
T getOr(const nlohmann::json& json, const std::string& key,
        const std::string& context, T defaultValue) {
  if (!json.contains(key)) {
    return defaultValue;
  }
  try {
    return json.at(key).get<T>();
  } catch (const nlohmann::json::exception& e) {
    configError(absl::StrCat("the value of `", context, ".", key,
                             "` has the wrong type (", e.what(), ")."));
  }
}
}  // namespace

// _____________________________________________________________________________
BlobDiffConfig BlobDiffConfig::fromJson(const nlohmann::json& json) {
  checkKeys(json, "<root>", {"index", "blob", "geoCompaction", "namedQueries"});
  BlobDiffConfig config;
  if (json.contains("index")) {
    const auto& index = json.at("index");
    checkKeys(index, "index",
              {"vocabularyType", "numThreads", "blankNodeIriRegexes",
               "prefixesForIdEncodedIris", "settingsFile"});
    if (index.contains("vocabularyType")) {
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
    if (index.contains("numThreads")) {
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
  if (json.contains("blob")) {
    const auto& blob = json.at("blob");
    checkKeys(blob, "blob", {"excludedEntryRegexes", "diffCompressionLevel"});
    config.excludedEntryRegexes_ = getOr<std::vector<std::string>>(
        blob, "excludedEntryRegexes", "blob", {});
    config.diffCompressionLevel_ = getOr<int>(
        blob, "diffCompressionLevel", "blob", config.diffCompressionLevel_);
    if (config.diffCompressionLevel_ < 1 || config.diffCompressionLevel_ > 22) {
      configError("`blob.diffCompressionLevel` has to be in [1, 22].");
    }
  }
  if (json.contains("geoCompaction")) {
    const auto& geo = json.at("geoCompaction");
    checkKeys(geo, "geoCompaction", {"maxSegments", "maxDeadShapeRatio"});
    config.maxGeoSegments_ = getOr<size_t>(geo, "maxSegments", "geoCompaction",
                                           config.maxGeoSegments_);
    config.maxDeadShapeRatio_ = getOr<double>(
        geo, "maxDeadShapeRatio", "geoCompaction", config.maxDeadShapeRatio_);
    if (!(config.maxDeadShapeRatio_ >= 0.0 &&
          config.maxDeadShapeRatio_ <= 1.0)) {
      configError("`geoCompaction.maxDeadShapeRatio` has to be in [0, 1].");
    }
  }
  if (!json.contains("namedQueries") || !json.at("namedQueries").is_array()) {
    configError("`namedQueries` has to be an array.");
  }
  ad_utility::HashSet<std::string> names;
  for (const auto& entry : json.at("namedQueries")) {
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
    if (entry.contains("geoIndexVar")) {
      auto var = getOr<std::string>(entry, "geoIndexVar", "namedQueries[]", "");
      if (var.size() < 2 || var[0] != '?') {
        configError(absl::StrCat("`geoIndexVar` of the named query `",
                                 query.name_,
                                 "` has to be a variable that starts with `?`, "
                                 "but is `",
                                 var, "`."));
      }
      query.geoIndexVar_ = std::move(var);
    }
    if (entry.contains("simplificationMeters")) {
      double meters =
          getOr<double>(entry, "simplificationMeters", "namedQueries[]", 0.0);
      if (!(meters > 0.0)) {
        configError(absl::StrCat("`simplificationMeters` of the named query `",
                                 query.name_, "` has to be positive."));
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

// _____________________________________________________________________________
BlobDiffConfig BlobDiffConfig::fromFile(const std::string& path) {
  auto stream = ad_utility::makeIfstream(path);
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(stream);
  } catch (const nlohmann::json::exception& e) {
    configError(absl::StrCat("the file ", path, " is not valid JSON (",
                             e.what(), ")."));
  }
  return fromJson(json);
}

// _____________________________________________________________________________
nlohmann::json BlobDiffConfig::toJson() const {
  nlohmann::json json;
  auto& index = json["index"];
  index["vocabularyType"] = std::string{vocabularyType_.toString()};
  if (numThreads_.has_value()) {
    index["numThreads"] = numThreads_.value();
  }
  index["blankNodeIriRegexes"] = blankNodeIriRegexes_;
  index["prefixesForIdEncodedIris"] = prefixesForIdEncodedIris_;
  index["settingsFile"] = settingsFile_;
  json["blob"]["excludedEntryRegexes"] = excludedEntryRegexes_;
  json["blob"]["diffCompressionLevel"] = diffCompressionLevel_;
  json["geoCompaction"]["maxSegments"] = maxGeoSegments_;
  json["geoCompaction"]["maxDeadShapeRatio"] = maxDeadShapeRatio_;
  auto& queries = json["namedQueries"] = nlohmann::json::array();
  for (const auto& query : namedQueries_) {
    nlohmann::json entry;
    entry["name"] = query.name_;
    entry["query"] = query.query_;
    if (query.geoIndexVar_.has_value()) {
      entry["geoIndexVar"] = query.geoIndexVar_.value();
    }
    if (query.simplificationMeters_.has_value()) {
      entry["simplificationMeters"] = query.simplificationMeters_.value();
    }
    queries.push_back(std::move(entry));
  }
  return json;
}

// _____________________________________________________________________________
std::string BlobDiffConfig::fingerprint() const {
  auto json = toJson();
  json.erase("geoCompaction");
  // The number of threads does not influence the content of the index, and
  // the compression level of the diffs does not influence the blobs.
  json["index"].erase("numThreads");
  json["blob"].erase("diffCompressionLevel");
  auto digest = ad_utility::HashSha256{}(json.dump());
  return absl::StrJoin(digest, "", ad_utility::hexFormatter);
}

}  // namespace qlever
