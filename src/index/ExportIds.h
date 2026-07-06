// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_EXPORTIDS_H
#define QLEVER_SRC_INDEX_EXPORTIDS_H

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocalVocab.h"
#include "parser/LiteralOrIri.h"
#include "util/CompilerExtensions.h"
#include "util/Exception.h"
#include "util/ValueIdentity.h"

namespace ql::exportIds {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using LiteralOrIriView = ad_utility::triple_component::LiteralOrIriView;
using Iri = ad_utility::triple_component::Iri;
using IriView = ad_utility::triple_component::IriView;
using Literal = ad_utility::triple_component::Literal;

// Convert the `id` to a `Literal`. Datatypes are always stripped, so for
// literals (this includes IDs that directly store their value, like Doubles)
// the datatype is always empty. If 'onlyReturnLiteralsWithXsdString' is
// false, IRIs are converted to literals without a datatype, which is
// equivalent to the behavior of the SPARQL STR(...) function. If
// 'onlyReturnLiteralsWithXsdString' is true, all IRIs and literals with
// non-`xsd:string` datatypes (including encoded IDs) return `std::nullopt`.
// These semantics are useful for the string expressions in
// StringExpressions.cpp.
std::optional<Literal> idToLiteral(
    const IndexImpl& index, Id id, const LocalVocab& localVocab,
    bool onlyReturnLiteralsWithXsdString = false);

// Same as the previous function, but only handles the datatypes for which the
// value is encoded directly in the ID. For other datatypes an exception is
// thrown.
// If `onlyReturnLiteralsWithXsdString` is `true`, returns `std::nullopt`.
// If `onlyReturnLiteralsWithXsdString` is `false`, removes datatypes from
// literals (e.g. the integer `42` is converted to the plain literal `"42"`).
std::optional<Literal> idToLiteralForEncodedValue(
    Id id, bool onlyReturnLiteralsWithXsdString = false);

// A helper function for the `idToLiteral` function. Checks and processes
// a LiteralOrIri based on the given parameters.
std::optional<Literal> handleIriOrLiteral(LiteralOrIri word,
                                          bool onlyReturnLiteralsWithXsdString);

// The function resolves a given `ValueId` to a `LiteralOrIri` object. Unlike
// `idToLiteral` no further processing is applied to the string content.
std::optional<LiteralOrIri> idToLiteralOrIri(const IndexImpl& index, Id id,
                                             const LocalVocab& localVocab,
                                             bool skipEncodedValues = false);

// Helper for the `idToLiteralOrIri` function: Retrieves a string literal from
// a value encoded in the given ValueId.
std::optional<LiteralOrIri> idToLiteralOrIriForEncodedValue(Id id);

// Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
// a word in the vocabulary.
LiteralOrIri getLiteralOrIriFromWordVocabIndex(const IndexImpl& index, Id id);

// Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
// a word in the text index.
std::optional<LiteralOrIri> getLiteralOrIriFromTextRecordIndex(
    const IndexImpl& index, Id id);

// Helper for the `idToLiteral` function: get only literals from the
// `LiteralOrIri` object.
std::optional<Literal> getLiteralOrNullopt(
    std::optional<LiteralOrIri> litOrIri);

// Replaces the first character '<' and the last character '>' with double
// quotes '"' to convert an IRI to a Literal, ensuring only the angle brackets
// are replaced.
std::string replaceAnglesByQuotes(std::string iriString);

// Return the blank-node string representation if `iri` is a blank-node IRI,
// otherwise std::nullopt.
template <typename IriType>
std::optional<std::string_view> blankNodeIriToString(
    const IriType& iri AD_LIFETIMEBOUND);

// Acts as a helper to retrieve a LiteralOrIri object from an Id, where the Id
// is of type `VocabIndex`, `LocalVocabIndex`, or `EncodedVal`. This function
// should only be called with suitable `Datatype` Ids, otherwise `AD_FAIL()` is
// called.
LiteralOrIri getLiteralOrIriFromVocabIndex(const IndexImpl& index, Id id,
                                           const LocalVocab& localVocab);

// Convert an ID whose value is encoded in the ID bits to (string, XSD-type).
// Returns std::nullopt for Undefined. Throws for non-encoded-value datatypes.
std::optional<std::pair<std::string, const char*>>
idToStringAndTypeForEncodedValue(Id id);

// Convert an `EncodedVal` ID to a `LiteralOrIri` by looking up the encoded
// IRI via the `EncodedIriManager` in the index.
LiteralOrIri encodedIdToLiteralOrIri(Id id, const IndexImpl& index);

// Format a `LiteralOrIri` as a (string, XSD-type) pair applying the template
// options and `escapeFunction`. Returns `std::nullopt` when
// `returnOnlyLiterals` is true and `word` is not a literal.
CPP_template(bool removeQuotesAndAngleBrackets = false,
             bool returnOnlyLiterals = false,
             typename LiteralOrIriType = LiteralOrIri,
             typename EscapeFunction = ql::identity)(
    requires ad_utility::SameAsAny<LiteralOrIriType, LiteralOrIri,
                                   LiteralOrIriView>) std::
    optional<std::pair<std::string, const char*>> literalOrIriToStringAndType(
        const LiteralOrIriType& word,
        EscapeFunction&& escapeFunction = EscapeFunction{}) {
  if constexpr (returnOnlyLiterals) {
    if (!word.isLiteral()) {
      return std::nullopt;
    }
  }
  if (word.isIri()) {
    if (auto blankNodeString = blankNodeIriToString(word.getIri())) {
      return std::pair{std::string{blankNodeString.value()}, nullptr};
    }
  }
  if constexpr (removeQuotesAndAngleBrackets) {
    // TODO<joka921> Can we get rid of the string copying here?
    return std::pair{
        escapeFunction(std::string{asStringViewUnsafe(word.getContent())}),
        nullptr};
  }
  // TODO<ms2144>: we unconditionally always materialize a string here, which
  // is wasteful and should be mitigated in the future.
  return std::pair{escapeFunction(std::string{word.toStringRepresentation()}),
                   nullptr};
}

// Convert the `id` to a human-readable string. The `index` is used to resolve
// `Id`s with datatype `VocabIndex` or `TextRecordIndex`. The `localVocab` is
// used to resolve `Id`s with datatype `LocalVocabIndex`. The `escapeFunction`
// is applied to the resulting string if it is not of a numeric type.
//
// Return value: If the `Id` encodes a numeric value (integer, double, etc.)
// then the `string` (first element of the pair) will be the number as a
// string without quotation marks, and the second element of the pair will
// contain the corresponding XSD-datatype as an URI. For all other values and
// datatypes, the second element of the pair will be empty and the first
// element will have the format `"stringContent"^^datatypeUri`. If the `id`
// holds the `Undefined` value, then `std::nullopt` is returned.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
std::optional<std::pair<std::string, const char*>> idToStringAndType(
    const Index& index, Id id, const LocalVocab& localVocab,
    EscapeFunction&& escapeFunction = EscapeFunction{}) {
  using enum Datatype;
  auto datatype = id.getDatatype();
  if constexpr (returnOnlyLiterals) {
    if (!(datatype == VocabIndex || datatype == LocalVocabIndex)) {
      return std::nullopt;
    }
  }

  auto formatLiteralOrIri = [&escapeFunction](const auto& word) {
    return literalOrIriToStringAndType<removeQuotesAndAngleBrackets,
                                       returnOnlyLiterals>(word,
                                                           escapeFunction);
  };

  switch (id.getDatatype()) {
    case WordVocabIndex: {
      std::string_view entity = index.indexToString(id.getWordVocabIndex());
      return std::pair{escapeFunction(std::string{entity}), nullptr};
    }
    case VocabIndex:
    case LocalVocabIndex:
      return formatLiteralOrIri(
          getLiteralOrIriFromVocabIndex(index.getImpl(), id, localVocab));
    case EncodedVal:
      return formatLiteralOrIri(encodedIdToLiteralOrIri(id, index.getImpl()));
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return idToStringAndTypeForEncodedValue(id);
  }
}

// Batch variant of `idToStringAndType`.
// Precondition: `ids` is sorted by `ValueId` (as `ConstructBatchEvaluator` does
// before calling vocabulary lookups). Because the datatype tag occupies the 4
// most significant bits of a `ValueId`, all `VocabIndex` IDs  therefore form a
// single contiguous block that is already sorted by vocabulary position. We
// resolve that block in one sequential `lookupBatch` call for I/O-local access
// to the on-disk vocabulary. All other IDs are resolved immediately, since
// their values are either encoded in the id bits or stored in the in-memory
// `LocalVocab`.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
std::vector<std::optional<std::pair<std::string, const char*>>>
idsToStringAndType(const Index& index, ql::span<const Id> ids,
                   const LocalVocab& localVocab,
                   EscapeFunction&& escapeFunction = EscapeFunction{}) {
  std::vector<std::optional<std::pair<std::string, const char*>>> results(
      ids.size());

  // Locate the single contiguous block of `VocabIndex` IDs.
  auto isVocab = [](const Id& id) {
    return id.getDatatype() == Datatype::VocabIndex;
  };
  auto vocabBegin = ql::ranges::find_if(ids, isVocab);
  auto vocabEnd = ql::ranges::find_if_not(vocabBegin, ids.end(), isVocab);

  // Resolve every non-`VocabIndex` ID immediately via in-memory lookups. These
  // are exactly the IDs outside `[vocabBegin, vocabEnd)`, i.e. the ranges
  // before and after the block.
  auto resolveInMemory = [&](auto first, auto last) {
    for (auto it = first; it != last; ++it) {
      results[it - ids.begin()] =
          idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
              index, *it, localVocab, escapeFunction);
    }
  };
  resolveInMemory(ids.begin(), vocabBegin);
  resolveInMemory(vocabEnd, ids.end());

  // Resolve the `VocabIndex` block in a single batch call. `lookupBatch`
  // expects raw `size_t` vocabulary indices and returns a span of
  // `string_view`s backed by memory kept alive by the returned shared_ptr.
  if (vocabBegin != vocabEnd) {
    auto rawIndices =
        ::ranges::to_vector(ql::ranges::subrange(vocabBegin, vocabEnd) |
                            ql::views::transform([](const Id& id) -> size_t {
                              return id.getVocabIndex().get();
                            }));

    auto batchResult = index.getImpl().getVocab().lookupBatch(rawIndices);

    // The block occupies the same positions in `results` as it does in `ids`.
    auto blockOffset = vocabBegin - ids.begin();
    auto resultsBlock = ql::ranges::subrange(
        results.begin() + blockOffset,
        results.begin() + blockOffset + (vocabEnd - vocabBegin));

    // Pair each looked-up `string_view` with its target slot in `results`.
    for (auto&& [sv, slot] : ::ranges::views::zip(*batchResult, resultsBlock)) {
      slot = literalOrIriToStringAndType<removeQuotesAndAngleBrackets,
                                         returnOnlyLiterals>(
          LiteralOrIriView::fromStringRepresentation(sv), escapeFunction);
    }
  }

  return results;
}

}  // namespace ql::exportIds

#endif  // QLEVER_SRC_INDEX_EXPORTIDS_H
