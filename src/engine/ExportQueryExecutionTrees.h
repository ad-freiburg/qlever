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
#include "engine/QueryExportTypes.h"
#include "index/ExportIds.h"
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
      const ad_utility::sparql_types::Triples& constructClauseTriples,
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
      const LimitOffsetClause& limitOffset, const Result& result,
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
