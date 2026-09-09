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

#include <memory>
#include <string>
#include <utility>

#include "../util/IndexTestHelpers.h"
#include "engine/UpdateMetadata.h"
#include "index/DeltaTriples.h"
#include "libqlever/Qlever.h"
#include "parser/SparqlParser.h"
#include "util/BlankNodeManager.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"

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

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_LIBQLEVER_QLEVERTESTHELPERS_H
