// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/TurtleDelta.h"

#include <absl/strings/str_cat.h>

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/enumerate.hpp>
#include <stdexcept>
#include <string>

#include "backports/algorithm.h"
#include "engine/ExecuteUpdate.h"
#include "index/IndexImpl.h"
#include "index/TripleComponentConversions.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/FilesystemHelpers.h"

namespace qlever::blobDiff {

namespace detail {
// _____________________________________________________________________________
std::vector<TurtleTriple> parseTurtleFile(const ql::filesystem::path& path,
                                          const Index& index,
                                          const RdfParserSettings& settings) {
  auto triples =
      RdfStringParser<TurtleParser<Tokenizer>>::parseTriplesFromString(
          util::readBinaryFile<qlever::parser::ByteBlock>(path),
          &index.encodedIriManager(), settings, path.string());
  // The RDF parsers represent blank nodes (both labeled and anonymous ones) as
  // plain strings, see `BlankNodeAdder::resolveParsedComponent`.
  for (const auto& [i, triple] : ::ranges::views::enumerate(triples)) {
    if (triple.subject_.isString() || triple.predicate_.isString() ||
        triple.object_.isString()) {
      throw std::runtime_error(absl::StrCat(
          "The triple number ", i + 1, " of \"", path.string(),
          "\" contains a blank node, which is not supported when computing "
          "the difference between two Turtle files"));
    }
  }
  return triples;
}

// _____________________________________________________________________________
std::vector<IdTriple<0>> toIdTriples(std::vector<TurtleTriple> triples,
                                     const Index& index,
                                     LocalVocab& localVocab) {
  auto toId = [&index, &localVocab](TripleComponent&& component) {
    AD_CONTRACT_CHECK(!component.isString(),
                      "Blank nodes are not supported by `toIdTriples`");
    return toValueId(std::move(component), index.getImpl(), localVocab);
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
  ExecuteUpdate::sortAndRemoveDuplicates(result);
  return result;
}
}  // namespace detail

// _____________________________________________________________________________
TripleDelta computeTripleDelta(const ql::filesystem::path& oldTurtleFile,
                               const ql::filesystem::path& newTurtleFile,
                               std::shared_ptr<const Index> index,
                               const RdfParserSettings& settings) {
  AD_CONTRACT_CHECK(index != nullptr);
  LocalVocab localVocab{index->getImpl().allocator()};
  auto parseToIdTriples =
      [&index, &settings, &localVocab](const ql::filesystem::path& turtleFile) {
        return detail::toIdTriples(
            detail::parseTurtleFile(turtleFile, *index, settings), *index,
            localVocab);
      };
  auto oldIdTriples = parseToIdTriples(oldTurtleFile);
  auto newIdTriples = parseToIdTriples(newTurtleFile);
  auto toInsert = ExecuteUpdate::setMinus(newIdTriples, oldIdTriples);
  auto toDelete = ExecuteUpdate::setMinus(oldIdTriples, newIdTriples);
  return TripleDelta{std::move(index), std::move(localVocab),
                     std::move(toInsert), std::move(toDelete)};
}

}  // namespace qlever::blobDiff
