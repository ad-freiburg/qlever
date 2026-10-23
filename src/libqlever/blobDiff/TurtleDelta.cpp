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
#include <string>

#include "backports/algorithm.h"
#include "index/DeltaTriples.h"
#include "index/TripleComponentConversions.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/CancellationHandle.h"
#include "util/FilesystemHelpers.h"

namespace qlever::blobDiff {

namespace {
namespace fs = ql::filesystem;

// Parse the Turtle file `path` using the settings of `index`.
std::vector<TurtleTriple> parseTurtleFile(const fs::path& path,
                                          const Index& index) {
  auto bytes = util::readBinaryFile(path);
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
}  // namespace

// _____________________________________________________________________________
TripleDelta computeTripleDelta(const ql::filesystem::path& oldTurtleFile,
                               const ql::filesystem::path& newTurtleFile,
                               const Index& index, BlankNodeAdder& adder) {
  auto oldTriples = parseTurtleFile(oldTurtleFile, index);
  auto newTriples = parseTurtleFile(newTurtleFile, index);
  auto oldIdTriples = toIdTriples(oldTriples, index, adder);
  auto newIdTriples = toIdTriples(newTriples, index, adder);
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
