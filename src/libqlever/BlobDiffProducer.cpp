// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/BlobDiffProducer.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <sstream>

#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "global/IdTriple.h"
#include "index/DeltaTriples.h"
#include "index/TripleComponentConversions.h"
#include "libqlever/BlobDiff.h"
#include "libqlever/BlobLayout.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "parser/BlankNodeAdder.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/Algorithm.h"
#include "util/CancellationHandle.h"
#include "util/CryptographicHashUtils.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/HashSet.h"

namespace qlever {

namespace {
namespace fs = ql::filesystem;
using Manager = NamedCachedQueryBlobManager;
using PinOptions = QueryExecutionContext::PinResultWithName;

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

// Read the whole file `path` into a vector of bytes.
std::vector<char> readFile(const fs::path& path) {
  auto stream = ad_utility::makeIfstream(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{stream},
          std::istreambuf_iterator<char>{}};
}

// Write `bytes` atomically to `path`: first to a temporary file next to it,
// then rename.
void writeFileAtomically(const fs::path& path, ql::span<const char> bytes) {
  fs::path temporary = path;
  temporary += ".tmp";
  {
    auto stream = ad_utility::makeOfstream(temporary, std::ios::binary);
    stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    stream.flush();
    if (!stream) {
      throw std::runtime_error{
          absl::StrCat("Could not write the file ", temporary.string())};
    }
  }
  fs::rename(temporary, path);
}

// Copy the file `from` atomically to `to`.
void copyFileAtomically(const fs::path& from, const fs::path& to) {
  fs::path temporary = to;
  temporary += ".tmp";
  fs::copy_file(from, temporary, fs::copy_options::overwrite_existing);
  fs::rename(temporary, to);
}

// Hold the paths of the files in a state directory.
struct StatePaths {
  fs::path dir_;
  fs::path indexDir_;
  std::string indexBaseName_;
  fs::path updateTriples_;
  fs::path currentTurtle_;
  fs::path currentBlob_;
  fs::path stateJson_;

  explicit StatePaths(const std::string& stateDir)
      : dir_{stateDir},
        indexDir_{dir_ / "index"},
        indexBaseName_{(indexDir_ / "index").string()},
        updateTriples_{indexBaseName_ + ".update-triples"},
        currentTurtle_{dir_ / "current.ttl"},
        currentBlob_{dir_ / "current.blob"},
        stateJson_{dir_ / "state.json"} {}
};

// Return the current time as an ISO 8601 string.
std::string now() {
  return absl::FormatTime("%Y-%m-%dT%H:%M:%SZ", absl::Now(),
                          absl::UTCTimeZone());
}

// Write the `state.json` file.
void writeState(const StatePaths& paths, const BlobDiffConfig& config,
                size_t week, std::optional<std::string> createdAt,
                size_t blobSize, size_t diffSize) {
  nlohmann::json state;
  state["week"] = week;
  state["createdAt"] = createdAt.value_or(now());
  state["updatedAt"] = now();
  state["configFingerprint"] = config.fingerprint();
  state["blobSizeCompressed"] = blobSize;
  state["lastDiffSizeCompressed"] = diffSize;
  state["turtleSize"] = fs::file_size(paths.currentTurtle_);
  std::string text = state.dump(2) + "\n";
  writeFileAtomically(paths.stateJson_, ql::span<const char>{text});
}

// Read and validate `state.json`.
nlohmann::json readState(const StatePaths& paths,
                         const BlobDiffConfig& config) {
  if (!fs::exists(paths.stateJson_)) {
    throw std::runtime_error{
        absl::StrCat("The state directory ", paths.dir_.string(),
                     " has no `state.json`. Run `init` first.")};
  }
  auto stream = ad_utility::makeIfstream(paths.stateJson_);
  auto state = nlohmann::json::parse(stream);
  if (state.at("configFingerprint").get<std::string>() !=
      config.fingerprint()) {
    throw std::runtime_error{
        "The config differs from the one that was used to initialize the "
        "state directory (the index options, the blob options, or the named "
        "queries changed). The blob diffs of a state directory require the "
        "same config in every week."};
  }
  return state;
}

// Return the options of `Qlever::queryAndPinResultWithName` for `query`.
PinOptions pinOptionsOf(const BlobDiffConfig::NamedQuery& query) {
  PinOptions options;
  options.name_ = query.name_;
  if (query.geoIndexVar_.has_value()) {
    options.geoIndexVar_ = Variable{query.geoIndexVar_.value()};
  }
  options.geoIndexSimplificationInMeters_ = query.simplificationMeters_;
  return options;
}

// Drop all pinned results of `qlever` and pin the queries of `config`.
void pinAllQueries(Qlever& qlever, const BlobDiffConfig& config) {
  qlever.clearNamedResultCache();
  qlever.clearQueryResultCache();
  for (const auto& query : config.namedQueries_) {
    qlever.queryAndPinResultWithName(pinOptionsOf(query), query.query_);
  }
}

// Return the config for loading the index of a state directory.
EngineConfig engineConfigOf(const StatePaths& paths) {
  EngineConfig config;
  config.baseName_ = paths.indexBaseName_;
  // Restore the delta triples of the previous weeks and persist the ones of
  // the current week.
  config.persistUpdates_ = true;
  return config;
}

// Decompress `compressedBlob` or throw.
std::vector<char, Manager::BlobAllocator> decompressOrThrow(
    ql::span<const char> compressedBlob) {
  auto result = Manager::tryToDecompressBlob(compressedBlob, {});
  if (auto* error = std::get_if<Manager::BlobError>(&result)) {
    throw std::runtime_error{
        absl::StrCat("Could not decompress a blob: ", error->message_)};
  }
  return std::get<0>(std::move(result));
}

// Return true iff the two byte ranges are equal.
template <typename A, typename B>
bool sameBytes(const A& a, const B& b) {
  return a.size() == b.size() &&
         (a.empty() || std::memcmp(a.data(), b.data(), a.size()) == 0);
}

// Parse the Turtle file `path` using the settings of `index`.
std::vector<TurtleTriple> parseTurtleFile(const fs::path& path,
                                          const Index& index) {
  auto bytes = readFile(path);
  std::string content{bytes.begin(), bytes.end()};
  RdfStringParser<TurtleParser<Tokenizer>> parser{&index.encodedIriManager()};
  parser.setInputName(path.string());
  parser.setInputStream(content);
  return parser.parseAndReturnAllTriples();
}

// Convert the `triples` into sorted and deduplicated `IdTriple`s in the default
// graph. Words that are not in the vocabulary of the index become
// `LocalVocabIndex` `Id`s that are kept alive by `adder`.
std::vector<IdTriple<0>> toIdTriples(std::vector<TurtleTriple>& triples,
                                     const Index& index,
                                     BlankNodeAdder& adder) {
  auto toId = [&](TripleComponent&& component) {
    return toValueId(adder.resolveParsedComponent(std::move(component)),
                     index.getImpl(), adder.localVocab_);
  };
  // NOTE: The default graph of the triples in the index is the `Id` of the
  // default graph IRI in the vocabulary (as in `ExecuteUpdate`), which is not
  // the same as the one in `qlever::specialIds()`.
  Id defaultGraph = toId(TripleComponent{
      ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)});
  std::vector<IdTriple<0>> result;
  result.reserve(triples.size());
  for (auto& triple : triples) {
    result.push_back(IdTriple<0>{std::array{
        toId(std::move(triple.subject_)), toId(std::move(triple.predicate_)),
        toId(std::move(triple.object_)), defaultGraph}});
  }
  ql::ranges::sort(result);
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

// Serialize the pinned results of `qlever` without a base.
std::vector<char> serializeWithoutBase(const Qlever& qlever,
                                       const BlobDiffConfig& config) {
  BlobSerializationConfig serializationConfig;
  serializationConfig.excludedEntryRegexes_ = config.excludedEntryRegexes_;
  serializationConfig.sortOnAllColumns_ = true;
  return qlever.serializeVocabAndNamedCacheToCompressedBlob(
      serializationConfig);
}

// Implement `BlobDiffProducer::step`. Do not roll back the persisted delta
// triples in case of an error, that is done by the caller.
BlobDiffProducer::WeeklyResult stepImpl(const BlobDiffConfig& config,
                                        const std::string& newTurtleFile,
                                        const StatePaths& paths,
                                        bool forceCompaction,
                                        const nlohmann::json& oldState) {
  BlobDiffProducer::WeeklyResult result;
  Qlever qlever{engineConfigOf(paths)};
  auto snapshot = qlever.indexAndViewsSnapshot();
  Index& index = snapshot->index_;

  // Compute the changes of the data as delta triples.
  auto oldTriples = parseTurtleFile(paths.currentTurtle_, index);
  auto newTriples = parseTurtleFile(newTurtleFile, index);
  // The difference is computed on the level of the `Id`s, which are exact and
  // normalized: a change of a literal that only affects its lexical form (for
  // example `"1"^^xsd:int` to `"01"^^xsd:int`) gives the same `Id` and is thus
  // no change, whereas a change of the type (for example from `1` to `1.0`) is.
  // A comparison of the string representations of the parsed triples would get
  // both cases wrong. The `adder` gives the same `Id` to equal words (also to
  // those that are not in the vocabulary), and to equal blank node labels.
  BlankNodeAdder adder{index.getBlankNodeManager()};
  auto oldIdTriples = toIdTriples(oldTriples, index, adder);
  auto newIdTriples = toIdTriples(newTriples, index, adder);
  std::vector<IdTriple<0>> toInsert;
  std::vector<IdTriple<0>> toDelete;
  std::set_difference(newIdTriples.begin(), newIdTriples.end(),
                      oldIdTriples.begin(), oldIdTriples.end(),
                      std::back_inserter(toInsert));
  std::set_difference(oldIdTriples.begin(), oldIdTriples.end(),
                      newIdTriples.begin(), newIdTriples.end(),
                      std::back_inserter(toDelete));
  result.numInsertedTriples_ = toInsert.size();
  result.numDeletedTriples_ = toDelete.size();

  // Apply all changes in a single modification of the delta triples. The
  // deletions come first, as in `ExecuteUpdate::executeUpdate`. The sets are
  // disjoint by construction.
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  index.deltaTriplesManager().modify<void>([&](DeltaTriples& deltaTriples) {
    if (!toDelete.empty()) {
      deltaTriples.deleteTriples<DeltaTriples::Consolidate::No>(
          handle, std::move(toDelete));
    }
    if (!toInsert.empty()) {
      deltaTriples.insertTriples<DeltaTriples::Consolidate::No>(
          handle, std::move(toInsert));
    }
    deltaTriples.consolidateAll();
  });

  // The direct update path does not clear the caches, but `pinAllQueries` does.
  pinAllQueries(qlever, config);

  // The base is the previous blob, loaded into an instance without index.
  auto previousBlob = readFile(paths.currentBlob_);
  Qlever base{EngineConfig{}, /*skipLoading=*/true};
  base.deserializeVocabAndNamedCacheFromCompressedBlob(previousBlob);

  BlobSerializationConfig serializationConfig;
  serializationConfig.excludedEntryRegexes_ = config.excludedEntryRegexes_;
  serializationConfig.sortOnAllColumns_ = true;
  size_t maxSegments = forceCompaction ? 0 : config.maxGeoSegments_;
  double maxDeadRatio = forceCompaction ? 0.0 : config.maxDeadShapeRatio_;
  serializationConfig.incremental_ = BlobSerializationConfig::IncrementalBase{
      &base, maxSegments, maxDeadRatio};
  result.blob_ =
      qlever.serializeVocabAndNamedCacheToCompressedBlob(serializationConfig);

  // Compute the diff and verify it.
  auto baseBytes = decompressOrThrow(previousBlob);
  auto targetBytes = decompressOrThrow(result.blob_);
  ql::span<const char> baseSpan{baseBytes.data(), baseBytes.size()};
  ql::span<const char> targetSpan{targetBytes.data(), targetBytes.size()};
  auto baseLayout = BlobLayout::parse(baseSpan);
  auto targetLayout = BlobLayout::parse(targetSpan);
  auto diff = computeBlobDiff(baseSpan, baseLayout, targetSpan, targetLayout);
  if (!sameBytes(diff.apply(baseSpan), targetBytes)) {
    throw std::runtime_error{
        "Self-check failed: applying the computed diff to the decompressed "
        "previous blob does not yield the decompressed new blob."};
  }
  result.diffFile_ = serializeBlobDiffToFile(diff);
  if (!sameBytes(applyBlobDiff(previousBlob, result.diffFile_), result.blob_)) {
    throw std::runtime_error{
        "Self-check failed: applying the diff file to the previous compressed "
        "blob does not yield the new compressed blob byte for byte."};
  }
  result.statistics_ = describeBlobDiff(diff, targetLayout);

  // Rotate the state.
  writeFileAtomically(paths.currentBlob_, result.blob_);
  copyFileAtomically(newTurtleFile, paths.currentTurtle_);
  writeState(paths, config, oldState.at("week").get<size_t>() + 1,
             oldState.at("createdAt").get<std::string>(), result.blob_.size(),
             result.diffFile_.size());
  return result;
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
    checkKeys(blob, "blob", {"excludedEntryRegexes"});
    config.excludedEntryRegexes_ = getOr<std::vector<std::string>>(
        blob, "excludedEntryRegexes", "blob", {});
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
  // The number of threads does not influence the content of the index.
  json["index"].erase("numThreads");
  auto digest = ad_utility::HashSha256{}(json.dump());
  return absl::StrJoin(digest, "", ad_utility::hexFormatter);
}

// _____________________________________________________________________________
std::vector<char> BlobDiffProducer::init(const BlobDiffConfig& config,
                                         const std::string& turtleFile,
                                         const std::string& stateDir) {
  StatePaths paths{stateDir};
  if (fs::exists(paths.dir_) &&
      fs::directory_iterator{paths.dir_} != fs::directory_iterator{}) {
    throw std::runtime_error{absl::StrCat(
        "The state directory ", stateDir,
        " is not empty. Refusing to overwrite an existing state.")};
  }
  fs::create_directories(paths.indexDir_);

  IndexBuilderConfig indexConfig;
  indexConfig.baseName_ = paths.indexBaseName_;
  indexConfig.inputFiles_.push_back(
      {turtleFile, Filetype::Turtle, std::nullopt});
  indexConfig.vocabType_ = config.vocabularyType_;
  if (config.numThreads_.has_value()) {
    indexConfig.numThreads_ = config.numThreads_.value();
  }
  indexConfig.blankNodeIriRegexes_ = config.blankNodeIriRegexes_;
  indexConfig.prefixesForIdEncodedIris_ = config.prefixesForIdEncodedIris_;
  indexConfig.settingsFile_ = config.settingsFile_;
  Qlever::buildIndex(indexConfig);

  Qlever qlever{engineConfigOf(paths)};
  pinAllQueries(qlever, config);
  auto blob = serializeWithoutBase(qlever, config);

  writeFileAtomically(paths.currentBlob_, blob);
  copyFileAtomically(turtleFile, paths.currentTurtle_);
  writeState(paths, config, 0, std::nullopt, blob.size(), 0);
  return blob;
}

// _____________________________________________________________________________
BlobDiffProducer::WeeklyResult BlobDiffProducer::step(
    const BlobDiffConfig& config, const std::string& newTurtleFile,
    const std::string& stateDir, bool forceCompaction) {
  StatePaths paths{stateDir};
  auto state = readState(paths, config);
  // The delta triples are persisted by the update, so remember the previous
  // ones, and restore them if the step fails, such that the state stays
  // consistent with `current.ttl` and `current.blob`.
  fs::path backup = paths.updateTriples_;
  backup += ".backup";
  bool hadUpdates = fs::exists(paths.updateTriples_);
  if (hadUpdates) {
    fs::copy_file(paths.updateTriples_, backup,
                  fs::copy_options::overwrite_existing);
  }
  absl::Cleanup removeBackup = [&backup] {
    std::error_code ec;
    fs::remove(backup, ec);
  };
  try {
    return stepImpl(config, newTurtleFile, paths, forceCompaction, state);
  } catch (...) {
    if (hadUpdates) {
      fs::copy_file(backup, paths.updateTriples_,
                    fs::copy_options::overwrite_existing);
    } else {
      std::error_code ec;
      fs::remove(paths.updateTriples_, ec);
    }
    throw;
  }
}

// _____________________________________________________________________________
std::vector<char> BlobDiffProducer::apply(ql::span<const char> baseBlob,
                                          ql::span<const char> diffFile) {
  return applyBlobDiff(baseBlob, diffFile);
}

// _____________________________________________________________________________
std::string BlobDiffProducer::inspect(
    ql::span<const char> blob, std::optional<ql::span<const char>> diffFile) {
  auto bytes = decompressOrThrow(blob);
  auto layout = BlobLayout::parse(ql::span<const char>{bytes});
  std::string result = absl::StrCat("Layout of the blob (", blob.size(),
                                    " bytes compressed):\n", layout.describe());
  if (diffFile.has_value()) {
    auto diff = readBlobDiffFromFile(diffFile.value());
    absl::StrAppend(&result, "\nThe diff file (", diffFile->size(),
                    " bytes) as a diff to this blob:\n",
                    describeBlobDiff(diff, layout));
  }
  return result;
}

}  // namespace qlever
