// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/TurtleDelta.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <range/v3/range/conversion.hpp>
#include <string>
#include <string_view>

#include "backports/algorithm.h"
#include "index/DeltaTriples.h"
#include "index/TripleComponentConversions.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/CancellationHandle.h"
#include "util/FilesystemHelpers.h"

namespace qlever::blobDiff {

namespace detail {
// _____________________________________________________________________________
std::vector<TurtleTriple> parseTurtleFile(const ql::filesystem::path& path,
                                          const Index& index) {
  auto bytes = util::readBinaryFile(path);
  std::string_view content{bytes.data(), bytes.size()};
  return RdfStringParser<TurtleParser<Tokenizer>>::parseTriplesFromString(
      content, &index.encodedIriManager(), path.string());
}

// _____________________________________________________________________________
std::vector<IdTriple<0>> toIdTriples(std::vector<TurtleTriple> triples,
                                     const Index& index,
                                     BlankNodeAdder& adder) {
  auto toId = [&index, &adder](TripleComponent&& component) {
    return toValueId(adder.resolveParsedComponent(std::move(component)),
                     index.getImpl(), adder.localVocab_);
  };
  // NOTE: The default graph of the triples in the index is the `Id` of the
  // default graph IRI in the vocabulary (as in `ExecuteUpdate`), which is not
  // the same as the one in `qlever::specialIds()`.
  Id defaultGraph = toId(TripleComponent{
      ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)});
  auto toIdTriple = [&toId, defaultGraph](TurtleTriple& triple) {
    return IdTriple<0>{std::array{
        toId(std::move(triple.subject_)), toId(std::move(triple.predicate_)),
        toId(std::move(triple.object_)), defaultGraph}};
  };
  auto result = ::ranges::to_vector(triples | ql::views::transform(toIdTriple));
  ql::ranges::sort(result);
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}
}  // namespace detail

// _____________________________________________________________________________
TripleDelta computeTripleDelta(const ql::filesystem::path& oldTurtleFile,
                               const ql::filesystem::path& newTurtleFile,
                               const Index& index, BlankNodeAdder& adder) {
  auto parseToIdTriples = [&index,
                           &adder](const ql::filesystem::path& turtleFile) {
    return detail::toIdTriples(detail::parseTurtleFile(turtleFile, index),
                               index, adder);
  };
  auto oldIdTriples = parseToIdTriples(oldTurtleFile);
  auto newIdTriples = parseToIdTriples(newTurtleFile);
  TripleDelta result;
  ql::ranges::set_difference(newIdTriples, oldIdTriples,
                             std::back_inserter(result.toInsert_));
  ql::ranges::set_difference(oldIdTriples, newIdTriples,
                             std::back_inserter(result.toDelete_));
  return result;
}

// _____________________________________________________________________________
void applyTripleDelta(Index& index, TripleDelta delta) {
  // The deletions come first, as in `ExecuteUpdate::executeUpdate`. The sets
  // are disjoint by construction.
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  index.deltaTriplesManager().modify<void>([&](DeltaTriples& deltaTriples) {
    if (!delta.toDelete_.empty()) {
      deltaTriples.deleteTriples<DeltaTriples::Consolidate::No>(
          handle, std::move(delta.toDelete_));
    }
    if (!delta.toInsert_.empty()) {
      deltaTriples.insertTriples<DeltaTriples::Consolidate::No>(
          handle, std::move(delta.toInsert_));
    }
    deltaTriples.consolidateAll();
  });
}

}  // namespace qlever::blobDiff
