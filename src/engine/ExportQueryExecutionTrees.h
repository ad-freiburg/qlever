//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/QueryExecutionTree.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/json.h"

// Class for computing the result of an already parsed and planned query and
// exporting it in different formats (TSV, CSV, Turtle, JSON, Binary).
//
// TODO<joka921> Also implement a streaming JSON serializer to reduce the RAM
// consumption of large JSON exports and to make this interface even simpler.
class ExportQueryExecutionTrees {
 public:
  using MediaType = ad_utility::MediaType;
  using CancellationHandle = ad_utility::SharedCancellationHandle;

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
  static cppcoro::generator<std::string> computeResult(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      MediaType mediaType, const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle);

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
            typename EscapeFunction = std::identity>
  static std::optional<std::pair<std::string, const char*>> idToStringAndType(
      const Index& index, Id id, const LocalVocab& localVocab,
      EscapeFunction&& escapeFunction = EscapeFunction{});

  // Same as the previous function, but only handles the datatypes for which the
  // value is encoded directly in the ID. For other datatypes an exception is
  // thrown.
  static std::optional<std::pair<std::string, const char*>>
  idToStringAndTypeForEncodedValue(Id id);

  // Acts as a helper to retrieve an LiteralOrIri object
  // from an Id, where the Id is of type `VocabIndex` or `LocalVocabIndex`.
  // This function should only be called with suitable `Datatype` Id's,
  // otherwise `AD_FAIL()` is called.
  static ad_utility::triple_component::LiteralOrIri
  getLiteralOrIriFromVocabIndex(const Index& index, Id id,
                                const LocalVocab& localVocab);

  // Convert a `stream_generator` to an "ordinary" `generator<string>` that
  // yields exactly the same chunks as the `stream_generator`. Exceptions that
  // happen during the creation of the first chunk (default chunk size is 1MB)
  // will be immediately thrown when calling this function. Exceptions that
  // happen later will be caught and their exception message will be  yielded by
  // the resulting `generator<string>` together with a message, that explains,
  // that there is no good mechanism for handling errors during a chunked HTTP
  // response transfer.
  static cppcoro::generator<std::string>
  convertStreamGeneratorForChunkedTransfer(
      ad_utility::streams::stream_generator streamGenerator);

 private:
  // Similar to `computeResult` but returns a stream in
  // QLeverJSON-format.
  static ad_utility::streams::stream_generator computeResultAsQLeverJSON(
      const ParsedQuery& query, const QueryExecutionTree& qet,
      const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle);

  // ___________________________________________________________________________
  static cppcoro::generator<std::string> selectQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result,
      CancellationHandle cancellationHandle);
  /**
   * @brief Convert an `IdTable` (typically from a query result) to a JSON
   * array In the `QLeverJSON` format. This function is called by
   *  `computeQueryResultAsQLeverJSON` to obtain the "actual" query results
   * (without the meta data)
   * @param qet The `QueryExecutionTree` of the query.
   * @param limitAndOffset at most <limit> entries are written, starting at
   * <from>
   * @param columns each pair of <columnInIdTable, correspondingType> tells
   * us which columns are to be serialized in which order
   * @param resultTable The query result in the ID space. If it is `nullptr`,
   *        then the query result will be obtained via `qet->getResult()`.
   * @return a 2D-Json array corresponding to the IdTable given the arguments
   */
  static cppcoro::generator<std::string> idTableToQLeverJSONBindings(
      const QueryExecutionTree& qet, const LimitOffsetClause& limitAndOffset,
      const QueryExecutionTree::ColumnIndicesAndTypes columns,
      std::shared_ptr<const Result> result,
      CancellationHandle cancellationHandle);

  // ___________________________________________________________________________
  static cppcoro::generator<std::string>
  constructQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> res, CancellationHandle cancellationHandle);

  // Generate an RDF graph for a CONSTRUCT query.
  static cppcoro::generator<QueryExecutionTree::StringTriple>
  constructQueryResultToTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> res,
      CancellationHandle cancellationHandle);

  // ___________________________________________________________________________
  template <MediaType format>
  static ad_utility::streams::stream_generator constructQueryResultToStream(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset,
      std::shared_ptr<const Result> resultTable,
      CancellationHandle cancellationHandle);

  // _____________________________________________________________________________
  template <MediaType format>
  static ad_utility::streams::stream_generator selectQueryResultToStream(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle);

  // Public for testing.
 public:
  struct TableConstRefWithVocab {
    const IdTable& idTable_;
    const LocalVocab& localVocab_;
  };
  // Helper type that contains an `IdTable` and a view with related indices to
  // access the `IdTable` with.
  struct TableWithRange {
    TableConstRefWithVocab tableWithVocab_;
    std::ranges::iota_view<uint64_t, uint64_t> view_;
  };

 private:
  // Yield all `IdTables` provided by the given `result`.
  static cppcoro::generator<ExportQueryExecutionTrees::TableConstRefWithVocab>
  getIdTables(const Result& result);

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
  static cppcoro::generator<TableWithRange> getRowIndices(
      LimitOffsetClause limitOffset, const Result& result,
      uint64_t& resutSizeTotal);

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
};
