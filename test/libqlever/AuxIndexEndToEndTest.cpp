// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "engine/ExecuteUpdate.h"
#include "index/AuxIndex.h"
#include "index/IndexImpl.h"
#include "libqlever/Qlever.h"
#include "parser/SparqlParser.h"
#include "util/File.h"

using namespace qlever;

namespace {

// Build an index from `turtleContents` and return an `EngineConfig` for it. Any
// auxiliary index of a previous run is removed first, else the freshly built
// index would pick it up.
EngineConfig buildTestIndex(std::string_view turtleContents) {
  std::string basename = gtestCurrentTestName();
  for (size_t generation : AuxIndex::generationsOnDisk(basename)) {
    AuxIndex::deleteFromDisk(AuxIndex::makeBasename(basename, generation));
  }
  std::string filename = absl::StrCat(basename, ".ttl");
  ad_utility::makeOfstream(filename) << turtleContents;
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };

  IndexBuilderConfig config;
  config.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  config.baseName_ = basename;
  Qlever::buildIndex(config);
  return EngineConfig{config};
}

// Remove all auxiliary indices of the index with the given base name.
void deleteAuxIndices(const std::string& basename) {
  for (size_t generation : AuxIndex::generationsOnDisk(basename)) {
    AuxIndex::deleteFromDisk(AuxIndex::makeBasename(basename, generation));
  }
}

// Execute the given SPARQL update. `Qlever::query` only runs queries, so the
// update is planned like a query (`ParsedQuery` represents both) and then
// executed against the delta triples, exactly like `Server::processUpdate`
// does.
void executeUpdate(Qlever& engine, const std::string& update) {
  auto indexAndViews = engine.indexAndViewsSnapshot();
  auto& index = indexAndViews->index_;
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  index.deltaTriplesManager().modify<void>([&](DeltaTriples& deltaTriples) {
    auto parsedUpdates = SparqlParser::parseUpdate(
        index.getBlankNodeManager(), &index.getImpl().encodedIriManager(),
        update, {});
    for (auto& parsedUpdate : parsedUpdates) {
      auto plannedUpdate =
          engine.planQuery(engine.bindParsedQuery(std::move(parsedUpdate)),
                           handle, std::nullopt);
      ExecuteUpdate::executeUpdate(index, plannedUpdate.parsedQuery(),
                                   plannedUpdate.queryExecutionTree(),
                                   deltaTriples, handle);
    }
  });
}

// Run `SELECT ?s ?o { ?s <p> ?o }` and return the result as TSV.
std::string queryAll(Qlever& engine) {
  return engine.query("SELECT ?s ?o WHERE { ?s <p> ?o } ORDER BY ?s ?o",
                      ad_utility::MediaType::tsv);
}

// The generation of the auxiliary index that `engine` currently uses, or
// `std::nullopt` if it has none.
std::optional<size_t> auxGeneration(const Qlever& engine) {
  const auto* auxIndex =
      engine.indexAndViewsSnapshot()->index_.getImpl().auxIndex();
  return auxIndex == nullptr ? std::nullopt
                             : std::optional{auxIndex->generation()};
}

// ____________________________________________________________________________
TEST(AuxIndexEndToEnd, buildAuxIndexKeepsQueryResults) {
  auto config = buildTestIndex("<a> <p> <A> . <b> <p> <B> . <c> <p> <C> .");
  config.persistUpdates_ = false;
  absl::Cleanup cleanup = [&config] { deleteAuxIndices(config.baseName_); };
  Qlever engine{config};
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();

  // Insert a triple with two new words and delete an existing one.
  executeUpdate(engine, "INSERT DATA { <d> <p> <D> }");
  executeUpdate(engine, "DELETE DATA { <a> <p> <A> }");
  auto expected = queryAll(engine);
  EXPECT_THAT(expected, ::testing::HasSubstr("<d>"));
  EXPECT_THAT(expected, ::testing::Not(::testing::HasSubstr("<a>")));
  ASSERT_EQ(auxGeneration(engine), std::nullopt);

  // Move the updates into an auxiliary index. The query result must not change.
  engine.buildAuxIndex(handle);
  EXPECT_EQ(auxGeneration(engine), 0);
  EXPECT_EQ(queryAll(engine), expected);

  // Further updates are applied on top of the auxiliary index, including the
  // deletion of a triple that the auxiliary index inserted and the re-insertion
  // of a triple that it deleted.
  executeUpdate(engine, "DELETE DATA { <d> <p> <D> }");
  executeUpdate(engine, "INSERT DATA { <a> <p> <A> . <e> <p> <E> }");
  auto expectedAfterSecondUpdate = queryAll(engine);
  EXPECT_THAT(expectedAfterSecondUpdate,
              ::testing::Not(::testing::HasSubstr("<d>")));
  EXPECT_THAT(expectedAfterSecondUpdate, ::testing::HasSubstr("<a>"));
  EXPECT_THAT(expectedAfterSecondUpdate, ::testing::HasSubstr("<e>"));

  // A rebuild merges them into a new generation, again without changing the
  // result.
  engine.buildAuxIndex(handle);
  EXPECT_EQ(auxGeneration(engine), 1);
  EXPECT_EQ(queryAll(engine), expectedAfterSecondUpdate);
}

// ____________________________________________________________________________
TEST(AuxIndexEndToEnd, auxIndexAndPersistedUpdatesSurviveARestart) {
  auto config = buildTestIndex("<a> <p> <A> . <b> <p> <B> .");
  config.persistUpdates_ = true;
  absl::Cleanup cleanup = [&config] {
    deleteAuxIndices(config.baseName_);
    auto updateFile = absl::StrCat(config.baseName_, UPDATE_TRIPLES_SUFFIX);
    if (ql::filesystem::exists(updateFile)) {
      ad_utility::deleteFile(updateFile);
    }
  };

  std::string expected;
  {
    Qlever engine{config};
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    executeUpdate(engine, "INSERT DATA { <c> <p> <C> }");
    engine.buildAuxIndex(handle);
    // An update that arrives *after* the build stays in RAM and is persisted
    // with the generation of the auxiliary index that it refers to.
    executeUpdate(engine, "INSERT DATA { <d> <p> <D> }");
    expected = queryAll(engine);
    EXPECT_THAT(expected, ::testing::HasSubstr("<c>"));
    EXPECT_THAT(expected, ::testing::HasSubstr("<d>"));
  }

  // A new instance loads the auxiliary index and replays the persisted updates
  // on top of it.
  {
    Qlever engine{config};
    EXPECT_EQ(auxGeneration(engine), 0);
    EXPECT_EQ(queryAll(engine), expected);
  }
}

// ____________________________________________________________________________
TEST(AuxIndexEndToEnd, buildAuxIndexWithoutUpdates) {
  auto config = buildTestIndex("<a> <p> <A> .");
  config.persistUpdates_ = false;
  absl::Cleanup cleanup = [&config] { deleteAuxIndices(config.baseName_); };
  Qlever engine{config};
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();

  auto expected = queryAll(engine);
  engine.buildAuxIndex(handle);
  EXPECT_EQ(auxGeneration(engine), 0);
  EXPECT_EQ(queryAll(engine), expected);
}

}  // namespace
