//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/QueryExecutionTree.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/json.h"

// This class contains all the functionality to convert a query that has already
// been parsed (by the SPARQL parser) and planned (by the query planner) into
// a serialized result. In particular, it creates TSV, CSV, Turtle, JSON (SPARQL
// conforming and QLever's flavor) and binary results).
// This class has only two static public functions, one for the JSON
// output format (which returns a `nlohmann::json` object) and one for the other
// result formats (tsv, csv, turtle, binary) which returns a
// `streamable_generator`.
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
  // TSV, Turtle, Binary. Note that the Binary format can only be used with
  // SELECT queries and the Turtle format can only be used with CONSTRUCT
  // queries. Invalid `mediaType`s and invalid combinations of `mediaType` and
  // the query type will throw. The result is returned as a `generator` that
  // lazily computes the serialized result in large chunks of bytes.
  static cppcoro::generator<std::string> computeResultAsStream(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      MediaType mediaType, CancellationHandle cancellationHandle);

  // Compute the result of the given `parsedQuery` (created by the
  // `SparqlParser`) for which the `QueryExecutionTree` has been previously
  // created by the `QueryPlanner`. The result is converted to the format
  // specified by the `mediaType`. Supported formats for this function are
  // `SparqlJSON` and `QLeverJSON`. Note that the SparqlJSON format can only be
  // used with SELECT queries. Invalid `mediaType`s and invalid combinations of
  // `mediaType` and the query type will throw. The result is returned as a
  // single JSON object that is fully materialized before the function returns.
  // The `requestTimer` is used to report timing statistics on the query. It
  // must have already run during the query planning to produce the expected
  // results.
  static nlohmann::json computeResultAsJSON(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      const ad_utility::Timer& requestTimer, MediaType mediaType,
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

 private:
  // TODO<joka921> The following functions are all internally called by the
  // two public functions above. All the code has been inside QLever for a long
  // time (previously in the classes `QueryExecutionTree` and `Server`. Clean
  // up their interfaces (are all these functions needed, or can some of them
  // be merged).

  // Similar to `queryToJSON`, but always returns the `QLeverJSON` format.
  static nlohmann::json computeQueryResultAsQLeverJSON(
      const ParsedQuery& query, const QueryExecutionTree& qet,
      const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle);
  // Similar to `queryToJSON`, but always returns the `SparqlJSON` format.
  static nlohmann::json computeSelectQueryResultAsSparqlJSON(
      const ParsedQuery& query, const QueryExecutionTree& qet,
      CancellationHandle cancellationHandle);

  // ___________________________________________________________________________
  static nlohmann::json selectQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> resultTable,
      CancellationHandle cancellationHandle);

  /**
   * @brief Convert an `IdTable` (typically from a query result) to a JSON array
   *   In the `QLeverJSON` format. This function is called by
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
  static nlohmann::json idTableToQLeverJSONArray(
      const QueryExecutionTree& qet, const LimitOffsetClause& limitAndOffset,
      const QueryExecutionTree::ColumnIndicesAndTypes& columns,
      std::shared_ptr<const Result> resultTable,
      CancellationHandle cancellationHandle);

  // ___________________________________________________________________________
  static nlohmann::json constructQueryResultBindingsToQLeverJSON(
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
  static nlohmann::json selectQueryResultToSparqlJSON(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> resultTable,
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

  // Helper type that contains an `IdTable` and a view with related indices to
  // access the `IdTable` with.
  struct TableWithRange {
    const IdTable& idTable_;
    std::ranges::iota_view<uint64_t, uint64_t> view_;
  };

  // Yield all `IdTables` provided by the given `result`.
  static cppcoro::generator<const IdTable&> getIdTables(const Result& result);

  // Return a range that contains the indices of the rows that have to be
  // exported from the `idTable` given the `LimitOffsetClause`. It takes into
  // account the LIMIT, the OFFSET, and the actual size of the `idTable`
  static cppcoro::generator<TableWithRange> getRowIndices(
      LimitOffsetClause limitOffset, const Result& result);

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
};
