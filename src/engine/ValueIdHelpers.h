// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_VALUEIDTOSTRING_H
#define QLEVER_SRC_ENGINE_VALUEIDTOSTRING_H

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "engine/LocalVocab.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "parser/LiteralOrIri.h"
#include "util/ValueIdentity.h"

namespace ql::valueId {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using Iri = ad_utility::triple_component::Iri;
using Literal = ad_utility::triple_component::Literal;

// Convert the `id` to a 'LiteralOrIri. Datatypes are always stripped, so for
// literals (this includes IDs that directly store their value, like Doubles)
// the datatype is always empty. If 'onlyReturnLiteralsWithXsdString' is
// false, IRIs are converted to literals without a datatype, which is
// equivalent to the behavior of the SPARQL STR(...) function. If
// 'onlyReturnLiteralsWithXsdString' is true, all IRIs and literals with
// non'-xsd:string' datatypes (including encoded IDs) return 'std::nullopt'.
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

// Checks if a LiteralOrIri is either a plain literal (without datatype)
// or a literal with the `xsd:string` datatype.
bool isPlainLiteralOrLiteralWithXsdString(const LiteralOrIri& word);

// Replaces the first character '<' and the last character '>' with double
// quotes '"' to convert an IRI to a Literal, ensuring only the angle brackets
// are replaced.
std::string replaceAnglesByQuotes(std::string iriString);

// Return the blank-node string representation if `iri` is a blank-node IRI,
// otherwise std::nullopt.
std::optional<std::string> blankNodeIriToString(const Iri& iri);

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

// _____________________________________________________________________________
LiteralOrIri encodedIdToLiteralOrIri(Id id, const IndexImpl& index);

// Convert an `Id` to (string, XSD-type). The `index` is used for `VocabIndex` /
// `TextRecordIndex` lookups; `localVocab` for `LocalVocabIndex` lookups.
// Template parameters:
//   removeQuotesAndAngleBrackets — strip leading/trailing delimiters
//   returnOnlyLiterals           — return nullopt for IRIs
//   EscapeFunction               — applied to the result string
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

  auto handleIriOrLiteral = [&escapeFunction](const LiteralOrIri& word)
      -> std::optional<std::pair<std::string, const char*>> {
    if constexpr (returnOnlyLiterals) {
      if (!word.isLiteral()) {
        return std::nullopt;
      }
    }
    if (word.isIri()) {
      if (auto blankNodeString = blankNodeIriToString(word.getIri())) {
        return std::pair{std::move(blankNodeString.value()), nullptr};
      }
    }
    if constexpr (removeQuotesAndAngleBrackets) {
      // TODO<joka921> Can we get rid of the string copying here?
      return std::pair{
          escapeFunction(std::string{asStringViewUnsafe(word.getContent())}),
          nullptr};
    }
    return std::pair{escapeFunction(word.toStringRepresentation()), nullptr};
  };

  switch (id.getDatatype()) {
    case WordVocabIndex: {
      std::string_view entity = index.indexToString(id.getWordVocabIndex());
      return std::pair{escapeFunction(std::string{entity}), nullptr};
    }
    case VocabIndex:
    case LocalVocabIndex:
      return handleIriOrLiteral(
          getLiteralOrIriFromVocabIndex(index.getImpl(), id, localVocab));
    case EncodedVal:
      return handleIriOrLiteral(encodedIdToLiteralOrIri(id, index.getImpl()));
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return idToStringAndTypeForEncodedValue(id);
  }
}

// Batch variant of idToStringAndType.
// Precondition: `ids` is sorted by `ValueId` (as `ConstructBatchEvaluator`
// does before calling vocabulary lookups). Under this precondition, all
// `VocabIndex` IDs form a single contiguous block that is already sorted by
// vocabulary position, so they can be resolved last and in sequential order
// for I/O-local access to the on-disk vocabulary. All other IDs are resolved
// immediately since their values are either encoded in the id bits or stored
// in the in-memory `LocalVocab`.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
std::vector<std::optional<std::pair<std::string, const char*>>>
idsToStringAndType(const Index& index, ql::span<const Id> ids,
                   const LocalVocab& localVocab,
                   EscapeFunction&& escapeFunction = EscapeFunction{}) {
  std::vector<std::optional<std::pair<std::string, const char*>>> results(
      ids.size());

  // `ids` is sorted by `ValueId`. Because the datatype tag occupies the 4 most
  // significant bits of a `ValueId`, all `VocabIndex` IDs form a single
  // contiguous block and are already sorted by vocabulary position within that
  // block. We resolve all other datatypes immediately (in-memory lookups) and
  // record where the `VocabIndex` block begins.
  size_t vocabBegin = ids.size();
  for (size_t i = 0; i < ids.size(); ++i) {
    if (ids[i].getDatatype() != Datatype::VocabIndex) {
      results[i] =
          idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
              index, ids[i], localVocab, escapeFunction);
    } else if (vocabBegin == ids.size()) {
      vocabBegin = i;
    }
  }

  // Resolve the contiguous `VocabIndex` block in sorted (vocabulary-position)
  // order, giving sequential I/O access to the on-disk vocabulary file.
  for (size_t i = vocabBegin;
       i < ids.size() && ids[i].getDatatype() == Datatype::VocabIndex; ++i) {
    results[i] =
        idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
            index, ids[i], localVocab, escapeFunction);
  }

  return results;
}

}  // namespace ql::valueId

#endif  // QLEVER_SRC_ENGINE_VALUEIDTOSTRING_H
