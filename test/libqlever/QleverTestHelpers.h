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
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "engine/UpdateMetadata.h"
#include "index/DeltaTriples.h"
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

// Write the `turtleContents` to a turtle file, build an index from it with the
// given vocabulary `type`, and return the corresponding `IndexBuilderConfig`.
// The basename of the index is derived from the name of the currently running
// test, so that concurrently running tests do not interfere with each other.
// Updates that were persisted for that basename by a previous run of the test
// are deleted, so that they are not applied to the new index. The turtle input
// file is deleted again immediately after the index was built.
//
// NOTE: The default vocabulary type is the in-memory, uncompressed one,
// because `serializeVocabAndNamedCacheToCompressedBlob` currently requires it
// unless vocabulary entries are excluded (see
// `Vocabulary::writeAsZeroCopyBlob`).
inline qlever::IndexBuilderConfig buildTestIndex(
    std::string_view turtleContents,
    ad_utility::VocabularyType type =
        ad_utility::VocabularyType::InMemoryUncompressed,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  std::string basename = gtestCurrentTestName();
  ad_utility::deleteFile(absl::StrCat(basename, ".update-triples"), false);
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << turtleContents;
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  qlever::IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, qlever::Filetype::Turtle, std::nullopt});
  config.baseName_ = std::move(basename);
  config.vocabType_ = type;
  EXPECT_NO_THROW(qlever::Qlever::buildIndex(config));
  return config;
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
