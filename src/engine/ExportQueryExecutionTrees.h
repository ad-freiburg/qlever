// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_EXPORTQUERYEXECUTIONTREES_H
#define QLEVER_SRC_ENGINE_EXPORTQUERYEXECUTIONTREES_H

#include <functional>

#include "engine/QueryExecutionTree.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

// Class for computing the result of an already parsed and planned query and
// exporting it in different formats (TSV, CSV, Turtle, JSON, Binary).
//
// TODO<joka921> Also implement a streaming JSON serializer to reduce the RAM
// consumption of large JSON exports and to make this interface even simpler.
class ExportQueryExecutionTrees {
 public:
  using MediaType = ad_utility::MediaType;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;

  // Compute the result of the given `parsedQuery` (created by the
  // `SparqlParser`) for which the `QueryExecutionTree` has been previously
  // created by the `QueryPlanner`. The result is converted into a sequence of
  // bytes that represents the result of the computed query in the format
  // specified by the `mediaType`. Supported formats for this function are CSV,
  // TSV, Turtle, Binary, SparqlJSON, QLeverJSON. Note that the Binary format
  // can only be used with SELECT queries and the Turtle format can only be used
  // with CONSTRUCT queries. Invalid `mediaType`s and invalid combinations of
  // `mediaType` and the query type will throw. The result is returned as a
  // `generator` that lazily computes the serialized result in large chunks of
  // bytes.
  using ComputeResultReturnType =
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
      cppcoro::generator<std::string>;
#else
      void;
#endif
  static ComputeResultReturnType computeResult(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      MediaType mediaType, const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle, STREAMABLE_YIELDER_ARG_DECL);

  // Return the corresponding blank node string representation for the export if
  // this iri is a blank node iri. Otherwise, return std::nullopt.
  static std::optional<std::string> blankNodeIriToString(
      const ad_utility::triple_component::Iri& iri);

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

  // Same as the previous function, but only handles the datatypes for which the
  // value is encoded directly in the ID. For other datatypes an exception is
  // thrown.
  static std::optional<std::pair<std::string, const char*>>
  idToStringAndTypeForEncodedValue(Id id);

  // Convert the `id` to a 'LiteralOrIri. Datatypes are always stripped, so for
  // literals (this includes IDs that directly store their value, like Doubles)
  // the datatype is always empty. If 'onlyReturnLiteralsWithXsdString' is
  // false, IRIs are converted to literals without a datatype, which is
  // equivalent to the behavior of the SPARQL STR(...) function. If
  // 'onlyReturnLiteralsWithXsdString' is true, all IRIs and literals with
  // non'-xsd:string' datatypes (including encoded IDs) return 'std::nullopt'.
  // These semantics are useful for the string expressions in
  // StringExpressions.cpp.
  static std::optional<Literal> idToLiteral(
      const Index& index, Id id, const LocalVocab& localVocab,
      bool onlyReturnLiteralsWithXsdString = false);

  // Same as the previous function, but only handles the datatypes for which the
  // value is encoded directly in the ID. For other datatypes an exception is
  // thrown.
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
      const Index& index, Id id, const LocalVocab& localVocab,
      bool skipEncodedValues = false);

  // Helper for the `idToLiteralOrIri` function: Retrieves a string literal from
  // a value encoded in the given ValueId.
  static std::optional<LiteralOrIri> idToLiteralOrIriForEncodedValue(Id id);

  // Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
  // a word in the vocabulary.
  static std::optional<LiteralOrIri> getLiteralOrIriFromWordVocabIndex(
      const Index& index, Id id);

  // Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
  // a word in the text index.
  static std::optional<LiteralOrIri> getLiteralOrIriFromTextRecordIndex(
      const Index& index, Id id);

  // Helper for the `idToLiteral` function: get only literals from the
  // `LiteralOrIri` object.
  static std::optional<Literal> getLiteralOrNullopt(
      std::optional<LiteralOrIri> litOrIri);

  // Checks if a LiteralOrIri is either a plain literal (without datatype)
  // or a literal with the `xsd:string` datatype.
  static bool isPlainLiteralOrLiteralWithXsdString(const LiteralOrIri& word);

  // Replaces the first character '<' and the last character '>' with double
  // quotes '"' to convert an IRI to a Literal, ensuring only the angle brackets
  // are replaced.
  static std::string replaceAnglesByQuotes(std::string iriString);

  // Acts as a helper to retrieve an LiteralOrIri object
  // from an Id, where the Id is of type `VocabIndex` or `LocalVocabIndex`.
  // This function should only be called with suitable `Datatype` Id's,
  // otherwise `AD_FAIL()` is called.
  static LiteralOrIri getLiteralOrIriFromVocabIndex(
      const Index& index, Id id, const LocalVocab& localVocab);

  // Convert a `stream_generator` to an "ordinary" `InputRange<string>` that
  // yields exactly the same chunks as the `stream_generator`. Exceptions that
  // happen during the creation of the first chunk (default chunk size is 1MB)
  // will be immediately thrown when calling this function. Exceptions that
  // happen later will be caught and their exception message will be  yielded by
  // the resulting `generator<string>` together with a message, that explains,
  // that there is no good mechanism for handling errors during a chunked HTTP
  // response transfer.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static ad_utility::InputRangeTypeErased<std::string>
  convertStreamGeneratorForChunkedTransfer(
      STREAMABLE_GENERATOR_TYPE streamGenerator);
#endif

 private:
  // Make sure that the offset is not applied again when exporting the
  // result (it is already applied by the root operation in the query
  // execution tree). Note that we don't need this for the limit because
  // applying a fixed limit is idempotent. This only works because the query
  // planner does the exact same `supportsLimit()` check.
  static void compensateForLimitOffsetClause(
      LimitOffsetClause& limitOffsetClause, const QueryExecutionTree& qet);

  // Generate the bindings of the result of a SELECT or CONSTRUCT query in the
  // `application/qlever-results+json` format.
  //
  // NOTE: This calls `selectQueryResultBindingsToQLeverJSON` or
  // `constructQueryResultBindingsToQLeverJSON` for the bindings and adds the
  // remaining (meta) fields needed for the `application/qlever-results+json`
  // format.
  static STREAMABLE_GENERATOR_TYPE computeResultAsQLeverJSON(
      const ParsedQuery& query, const QueryExecutionTree& qet,
      const LimitOffsetClause& limitOffset,
      const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle, STREAMABLE_YIELDER_ARG_DECL);

  // Generate the bindings of the result of a SELECT query in the
  // `application/ qlever+json` format.
  static ad_utility::InputRangeTypeErased<std::string>
  selectQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

  // Generate the bindings of the result of a CONSTRUCT query in the
  // `application/ qlever+json` format.
  static ad_utility::InputRangeTypeErased<std::string>
  constructQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

  // Helper function that generates the individual bindings for the
  // `application/ qlever+json` format.
  static auto idTableToQLeverJSONBindings(
      const QueryExecutionTree& qet, LimitOffsetClause limitAndOffset,
      const QueryExecutionTree::ColumnIndicesAndTypes columns,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

  // Helper function that generates the result of a CONSTRUCT query as
  // `StringTriple`s.
  static auto constructQueryResultToTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
      uint64_t& resultSize, CancellationHandle cancellationHandle);

  // Helper function that generates the result of a CONSTRUCT query as a
  // CSV or TSV stream.
  template <MediaType format>
  static STREAMABLE_GENERATOR_TYPE constructQueryResultToStream(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
      CancellationHandle cancellationHandle, STREAMABLE_YIELDER_ARG_DECL);

  // Generate the result of a SELECT query as a CSV or TSV or binary stream.
  template <MediaType format>
  static STREAMABLE_GENERATOR_TYPE selectQueryResultToStream(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle,
      const ad_utility::Timer& requestTimer, STREAMABLE_YIELDER_ARG_DECL);

  // Public for testing.
 public:
  struct TableConstRefWithVocab {
    std::reference_wrapper<const IdTable> idTable_;
    std::reference_wrapper<const LocalVocab> localVocab_;

    const IdTable& idTable() const { return idTable_.get(); }

    const LocalVocab& localVocab() const { return localVocab_.get(); }
  };
  // Helper type that contains an `IdTable` and a view with related indices to
  // access the `IdTable` with.
  struct TableWithRange {
    TableConstRefWithVocab tableWithVocab_;
    ql::ranges::iota_view<uint64_t, uint64_t> view_;
  };

 private:
  // Yield all `IdTables` provided by the given `result`.
  static ad_utility::InputRangeTypeErased<TableConstRefWithVocab> getIdTables(
      const Result& result);

  // Generate the result in "blocks" and, when iterating over the generator
  // from beginning to end, return the total number of rows in the result
  // in `totalResultSize`.
  //
  // Blocks, where all rows are before OFFSET, are requested (and hence
  // computed), but skipped.
  //
  // Blocks, where at least one row is after OFFSET but before the effective
  // export limit (minimum of the LIMIT and the value of the `send` parameter),
  // are requested and yielded (together with the corresponding `LocalVocab`
  // and the range from that `IdTable` that belongs to the result).
  //
  // Blocks after the effective export limit until the LIMIT are requested, and
  // counted towards the `totalResultSize`, but not yielded.
  //
  // Blocks after the LIMIT are not even requested.
 public:
  static ad_utility::InputRangeTypeErased<TableWithRange> getRowIndices(
      LimitOffsetClause limitOffset, const Result& result,
      uint64_t& resutSizeTotal, uint64_t resultSizeMultiplicator = 1);

 private:
  FRIEND_TEST(ExportQueryExecutionTrees, getIdTablesReturnsSingletonIterator);
  FRIEND_TEST(ExportQueryExecutionTrees, getIdTablesMirrorsGenerator);
  FRIEND_TEST(ExportQueryExecutionTrees, ensureCorrectSlicingOfSingleIdTable);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenFirstIsSkipped);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenLastIsSkipped);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenFirstAndSecondArePartial);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenFirstAndLastArePartial);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureGeneratorIsNotConsumedWhenNotRequired);
  FRIEND_TEST(ExportQueryExecutionTrees, verifyQleverJsonContainsValidMetadata);
  FRIEND_TEST(ExportQueryExecutionTrees, compensateForLimitOffsetClause);
};

#endif  // QLEVER_SRC_ENGINE_EXPORTQUERYEXECUTIONTREES_H
