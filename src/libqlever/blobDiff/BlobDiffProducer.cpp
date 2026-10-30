// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/BlobDiffProducer.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <cstring>
#include <stdexcept>
#include <variant>

#include "backports/filesystem.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiff.h"
#include "libqlever/blobDiff/BlobDiffQueries.h"
#include "libqlever/blobDiff/TurtleDelta.h"
#include "parser/BlankNodeAdder.h"
#include "util/CryptographicHashUtils.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/json.h"

namespace qlever {

namespace {
namespace fs = ql::filesystem;
using Manager = NamedCachedQueryBlobManager;
using util::copyFileAtomically;
using util::readBinaryFile;
using util::writeFileAtomically;

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

// Serialize the pinned results of `qlever` without a base. The blob is written
// in the new format, so that it keeps the simplification of the geo indices,
// which the next week needs to extend them (see
// `BlobSerializationConfig::alwaysUseNewFormat_`).
std::vector<char> serializeWithoutBase(const Qlever& qlever,
                                       const BlobDiffConfig& config) {
  BlobSerializationConfig serializationConfig;
  serializationConfig.excludedEntryRegexes_ = config.excludedEntryRegexes_;
  serializationConfig.alwaysUseNewFormat_ = true;
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

  // Compute the changes of the data as delta triples, and apply them. The
  // `adder` keeps the new words alive until the delta triples are applied.
  BlankNodeAdder adder{index.getBlankNodeManager()};
  auto delta = blobDiff::computeTripleDelta(paths.currentTurtle_, newTurtleFile,
                                            index, adder);
  result.numInsertedTriples_ = delta.toInsert_.size();
  result.numDeletedTriples_ = delta.toDelete_.size();
  blobDiff::applyTripleDelta(index, std::move(delta));

  // The direct update path does not clear the caches, but
  // `blobDiff::pinAllQueries` does.
  blobDiff::pinAllQueries(qlever, config);

  // The base is the previous blob, loaded into an instance without index.
  auto previousBlob = readBinaryFile(paths.currentBlob_);
  Qlever base{EngineConfig{}, /*skipLoading=*/true};
  base.deserializeVocabAndNamedCacheFromCompressedBlob(previousBlob);

  BlobSerializationConfig serializationConfig;
  serializationConfig.excludedEntryRegexes_ = config.excludedEntryRegexes_;
  size_t maxSegments = forceCompaction ? 1 : config.maxGeoSegments_;
  double maxDeadRatio = forceCompaction ? 0.0 : config.maxDeadShapeRatio_;
  serializationConfig.incremental_ = BlobSerializationConfig::IncrementalBase{
      &base, maxSegments, maxDeadRatio};
  result.blob_ =
      qlever.serializeVocabAndNamedCacheToCompressedBlob(serializationConfig);

  // Compute the diff and verify it. NOTE: `applyBlobDiffToCompressedBase`
  // verifies the checksum of the decompressed result, so the comparison of the
  // compressed bytes only checks that the recompression is deterministic (it
  // is, because the same ZSTD version compresses the same bytes with the same
  // parameters in this process).
  auto baseBytes = decompressOrThrow(previousBlob);
  auto targetBytes = decompressOrThrow(result.blob_);
  result.diffFile_ = blobDiff::computeBlobDiff(
      ql::span<const char>{baseBytes.data(), baseBytes.size()},
      ql::span<const char>{targetBytes.data(), targetBytes.size()},
      config.diffCompressionLevel_);
  auto applied =
      blobDiff::applyBlobDiffToCompressedBase(previousBlob, result.diffFile_);
  if (const auto* error = std::get_if<blobDiff::BlobDiffError>(&applied)) {
    throw std::runtime_error{absl::StrCat(
        "Self-check failed: the diff file cannot be applied to the previous "
        "compressed blob: ",
        error->message_)};
  }
  if (!sameBytes(std::get<std::vector<char>>(applied), result.blob_)) {
    throw std::runtime_error{
        "Self-check failed: applying the diff file to the previous compressed "
        "blob does not yield the new compressed blob byte for byte."};
  }
  result.statistics_ = blobDiff::describeBlobDiff(result.diffFile_);

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
  blobDiff::pinAllQueries(qlever, config);
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
std::string BlobDiffProducer::inspect(
    ql::span<const char> blob, std::optional<ql::span<const char>> diffFile) {
  auto bytes = decompressOrThrow(blob);
  ql::span<const char> span{bytes.data(), bytes.size()};
  auto checksum = blobDiff::blobChecksum(span);
  std::string result =
      absl::StrCat("Blob: ", blob.size(), " bytes compressed, ", bytes.size(),
                   " bytes decompressed, SHA-256 of the decompressed blob ",
                   absl::StrJoin(checksum, "", ad_utility::hexFormatter));
  if (diffFile.has_value()) {
    auto header = blobDiff::readBlobDiffHeader(diffFile.value());
    if (const auto* error = std::get_if<blobDiff::BlobDiffError>(&header)) {
      absl::StrAppend(&result, "\nThe diff file is invalid: ", error->message_);
      return result;
    }
    const auto& validHeader = std::get<blobDiff::BlobDiffHeader>(header);
    std::string_view relation =
        validHeader.targetChecksum_ == checksum ? "yields this blob"
        : validHeader.baseChecksum_ == checksum ? "applies to this blob"
                                                : "is unrelated to this blob";
    absl::StrAppend(&result, "\n", blobDiff::describeBlobDiff(diffFile.value()),
                    "\nThe diff ", relation, ".");
  }
  return result;
}

}  // namespace qlever
