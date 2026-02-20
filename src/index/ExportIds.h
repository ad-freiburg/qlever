// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_INDEX_EXPORTIDS_H
#define QLEVER_SRC_INDEX_EXPORTIDS_H

#include <optional>
#include <string>
#include <utility>

#include "backports/functional.h"
#include "engine/LocalVocab.h"
#include "global/Id.h"
#include "parser/LiteralOrIri.h"

// Forward declarations.
class Index;
class IndexImpl;

// Utility class for converting `Id`s (ValueIds) to human-readable strings,
// literals, and IRIs. All functions only depend on the Index/Vocabulary and
// the local vocabulary, so they can be part of the `index` library without
// pulling in the full query-execution machinery.
class ExportIds {
 public:
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;

  // Return the corresponding blank node string representation for the export if
  // this iri is a blank node iri. Otherwise, return std::nullopt.
  static std::optional<std::string> blankNodeIriToString(
      const ad_utility::triple_component::Iri& iri);

  // Convert the `id` to a human-readable string. The `index` is used to
  // resolve `Id`s with datatype `VocabIndex` or `TextRecordIndex`. The
  // `localVocab` is used to resolve `Id`s with datatype `LocalVocabIndex`. The
  // `escapeFunction` is applied to the resulting string if it is not of a
  // numeric type.
  //
  // Return value: If the `Id` encodes a numeric value (integer, double, etc.)
  // then the `string` (first element of the pair) will be the number as a
  // string without quotation marks, and the second element of the pair will
  // contain the corresponding XSD-datatype as an URI. For all other values and
  // datatypes, the second element of the pair will be empty and the first
  // element will have the format `"stringContent"^^datatypeUri`. If the `id`
  // holds the `Undefined` value, then `std::nullopt` is returned.
  //
  // Note: This function currently has to be public because the
  // `Variable::evaluate` function calls it for evaluating CONSTRUCT queries.
  //
  // TODO<joka921> Make it private again as soon as the evaluation of construct
  // queries is completely performed inside this module.
  template <bool removeQuotesAndAngleBrackets = false,
            bool returnOnlyLiterals = false,
            typename EscapeFunction = ql::identity>
  static std::optional<std::pair<std::string, const char*>> idToStringAndType(
      const Index& index, Id id, const LocalVocab& localVocab,
      EscapeFunction&& escapeFunction = EscapeFunction{});

  // Same as the previous function, but only handles the datatypes for which
  // the value is encoded directly in the ID. For other datatypes an exception
  // is thrown.
  static std::optional<std::pair<std::string, const char*>>
  idToStringAndTypeForEncodedValue(Id id);

  // Convert the `id` to a `LiteralOrIri`. Datatypes are always stripped, so
  // for literals (this includes IDs that directly store their value, like
  // Doubles) the datatype is always empty. If `onlyReturnLiteralsWithXsdString`
  // is false, IRIs are converted to literals without a datatype, which is
  // equivalent to the behavior of the SPARQL STR(...) function. If
  // `onlyReturnLiteralsWithXsdString` is true, all IRIs and literals with
  // non-`xsd:string` datatypes (including encoded IDs) return `std::nullopt`.
  // These semantics are useful for the string expressions in
  // StringExpressions.cpp.
  static std::optional<Literal> idToLiteral(
      const IndexImpl& index, Id id, const LocalVocab& localVocab,
      bool onlyReturnLiteralsWithXsdString = false);

  // Same as the previous function, but only handles the datatypes for which
  // the value is encoded directly in the ID. For other datatypes an exception
  // is thrown.
  // If `onlyReturnLiteralsWithXsdString` is `true`, returns `std::nullopt`.
  // If `onlyReturnLiteralsWithXsdString` is `false`, removes datatypes from
  // literals (e.g. the integer `42` is converted to the plain literal `"42"`).
  static std::optional<Literal> idToLiteralForEncodedValue(
      Id id, bool onlyReturnLiteralsWithXsdString = false);

  // A helper function for the `idToLiteral` function. Checks and processes
  // a LiteralOrIri based on the given parameters.
  static std::optional<Literal> handleIriOrLiteral(
      LiteralOrIri word, bool onlyReturnLiteralsWithXsdString);

  // The function resolves a given `ValueId` to a `LiteralOrIri` object. Unlike
  // `idToLiteral` no further processing is applied to the string content.
  static std::optional<LiteralOrIri> idToLiteralOrIri(
      const IndexImpl& index, Id id, const LocalVocab& localVocab,
      bool skipEncodedValues = false);

  // Helper for the `idToLiteralOrIri` function: Retrieves a string literal
  // from a value encoded in the given ValueId.
  static std::optional<LiteralOrIri> idToLiteralOrIriForEncodedValue(Id id);

  // Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
  // a word in the vocabulary.
  static std::optional<LiteralOrIri> getLiteralOrIriFromWordVocabIndex(
      const IndexImpl& index, Id id);

  // Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
  // a word in the text index.
  static std::optional<LiteralOrIri> getLiteralOrIriFromTextRecordIndex(
      const IndexImpl& index, Id id);

  // Helper for the `idToLiteral` function: get only literals from the
  // `LiteralOrIri` object.
  static std::optional<Literal> getLiteralOrNullopt(
      std::optional<LiteralOrIri> litOrIri);

  // Checks if a LiteralOrIri is either a plain literal (without datatype)
  // or a literal with the `xsd:string` datatype.
  static bool isPlainLiteralOrLiteralWithXsdString(const LiteralOrIri& word);

  // Replaces the first character '<' and the last character '>' with double
  // quotes '"' to convert an IRI to a Literal, ensuring only the angle
  // brackets are replaced.
  static std::string replaceAnglesByQuotes(std::string iriString);

  // Acts as a helper to retrieve a `LiteralOrIri` object from an `Id`, where
  // the `Id` is of type `VocabIndex` or `LocalVocabIndex`. This function
  // should only be called with suitable `Datatype` Ids, otherwise `AD_FAIL()`
  // is called.
  static LiteralOrIri getLiteralOrIriFromVocabIndex(
      const IndexImpl& index, Id id, const LocalVocab& localVocab);
};

#endif  // QLEVER_SRC_INDEX_EXPORTIDS_H
