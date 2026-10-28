// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_LIBQLEVER_QLEVERTESTHELPERS_H
#define QLEVER_TEST_LIBQLEVER_QLEVERTESTHELPERS_H

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "engine/UpdateMetadata.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "parser/SparqlParser.h"
#include "util/BlankNodeManager.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/SourceLocation.h"

namespace ad_utility::testing {

// Parse and plan `update` and apply it to `engine` via `Qlever::applyUpdate`,
// returning the metadata. `Qlever::parseQuery`/`parseAndPlanQuery` only accept
// SPARQL queries, not updates (see `SparqlParser::parseQuery` vs.
// `parseUpdate`), so an update has to be parsed separately and then planned via
// `Qlever::bindParsedQuery`.
//
// NOTE: The snapshot that provides the `DeltaTriples` is taken only here,
// independently of the one that the update was planned against. In general
// this is not thread-safe (a concurrent index rebuild between the two calls
// would violate the precondition of `applyUpdate`), but the tests that use
// this helper are single-threaded, and `PlannedQuery` currently does not
// expose the snapshot it was planned against. See the comments in
// `LibQlever.applyUpdate` in `QleverTest.cpp` for details.
inline UpdateMetadata applyUpdateToEngine(qlever::Qlever& engine,
                                          const std::string& update) {
  ad_utility::BlankNodeManager blankNodeManager;
  auto parsedUpdates = SparqlParser::parseUpdate(
      &blankNodeManager, ad_utility::testing::encodedIriManager(), update);
  AD_CORRECTNESS_CHECK(parsedUpdates.size() == 1);
  auto plannedUpdate =
      engine.planQuery(engine.bindParsedQuery(std::move(parsedUpdates[0])));
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto snapshot = engine.indexAndViewsSnapshot();
  return snapshot->index_.deltaTriplesManager().modify<UpdateMetadata>(
      [&](DeltaTriples& deltaTriples) {
        return engine.applyUpdate(plannedUpdate, handle, deltaTriples);
      });
}

// Write `turtleContents` to a turtle file, build an index from it, and return
// the corresponding `IndexBuilderConfig`. If a vocabulary `type` is given, it
// is used for the index, otherwise the default one.
//
// The base name of both the turtle file and the index is derived from the name
// of the currently running test and the optional `suffix`, so that concurrently
// running tests do not interfere with each other, and a single test can build
// several distinct indexes. All files of an index with that base name that are
// left over from a previous run (in particular persisted updates, which would
// otherwise be applied to the new index) are deleted first. The turtle file is
// deleted again before this returns, the files of the index itself remain on
// disk.
//
// NOTE 1: An index that cannot be built throws, which `gtest` reports as a
// failure of the running test. This is deliberately not an `EXPECT_NO_THROW`,
// which would let the test continue with a nonexistent index.
//
// NOTE 2: `serializeVocabAndNamedCacheToCompressedBlob` currently requires the
// `InMemoryUncompressed` vocabulary unless vocabulary entries are excluded (see
// `Vocabulary::writeAsZeroCopyBlob`), so tests of blobs have to pass it as the
// `type`.
inline qlever::IndexBuilderConfig buildTestIndex(
    std::string_view turtleContents,
    std::optional<ad_utility::VocabularyType> type = std::nullopt,
    std::string_view suffix = "") {
  std::string basename = absl::StrCat(gtestCurrentTestName(), suffix);
  ql::ranges::for_each(IndexImpl::allIndexFiles(basename),
                       [](const auto& f) { ad_utility::deleteFile(f); });
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  ad_utility::makeOfstream(sourceFilename) << turtleContents;
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  qlever::IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, qlever::Filetype::Turtle, std::nullopt});
  config.baseName_ = std::move(basename);
  if (type.has_value()) {
    config.vocabType_ = type.value();
  }
  qlever::Qlever::buildIndex(config);
  return config;
}

// Same as `buildTestIndex` above, but return an `EngineConfig` for the index.
inline qlever::EngineConfig buildTestEngineConfig(
    std::string_view turtleContents,
    std::optional<ad_utility::VocabularyType> type = std::nullopt,
    std::string_view suffix = "") {
  return qlever::EngineConfig{buildTestIndex(turtleContents, type, suffix)};
}

// Return the decompressed `compressedBlob` (see
// `NamedCachedQueryBlobManager::tryToDecompressBlob`), and fail the test if it
// cannot be decompressed.
inline std::vector<char, qlever::NamedCachedQueryBlobManager::BlobAllocator>
decompressOrFail(ql::span<const char> compressedBlob,
                 ql::pmr::polymorphic_allocator<char> allocator = {},
                 ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  using Manager = qlever::NamedCachedQueryBlobManager;
  auto trace = generateLocationTrace(l);
  auto result = Manager::tryToDecompressBlob(compressedBlob, allocator);
  if (const auto* error = std::get_if<Manager::BlobError>(&result)) {
    ADD_FAILURE() << error->message_;
    return {};
  }
  return std::get<std::vector<char, Manager::BlobAllocator>>(std::move(result));
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_LIBQLEVER_QLEVERTESTHELPERS_H
