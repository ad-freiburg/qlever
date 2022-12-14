//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <cstdlib>

#include "engine/QueryExecutionTree.h"
#include "nlohmann/json.hpp"
#include "util/http/MediaTypes.h"

#pragma once

// This class contains all the functionality to convert a query that has already
// been parsed (by the SPARQL parser) and planned (by the query planner) into
// a serialized result. In particular, it creates tsv, csv, turtle, json (sparql
// conforming and QLever's flavor) and binary results). The queries are first
// computed or This class has only two static public functions, one for the JSON
// output format (which returns a `nlohmann::json` object) and one for the other
// result formats (tsv, csv, turtle, binary) which returns a
// `streamable_generator`.
// TODO<joka921> Also implement a streaming JSON serializer to reduce the RAM
// consumption of large JSON exports and to make this interface even simpler.
class ExportQueryExecutionTrees {
 public:
  using MediaType = ad_utility::MediaType;

  // Convert a `ParsedQuery` (created by the `SparqlParser`) and a
  // `QueryExecutionTree` (created by the `QueryPlanner` using the
  // `ParsedQuery`) into a sequence of bytes that represents the result of the
  // computed query in the format specified by the `mediaType`. Supported
  // formats for this function are CSV, TSV, Turtle, Binary. Note that the
  // Binary format can only be used with SELECT queries and the Turtle format
  // can only be used with CONSTRUCT queries. Invalid `mediaType`s and invalid
  // combinations of `mediaType` and the query type will throw. The result is
  // returned as a `stream_generator` that lazily computes the serialized result
  // in large chunks of bytes.
  static ad_utility::streams::stream_generator queryToStreamableGenerator(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      MediaType mediaType);

  // Convert a `ParsedQuery` (created by the `SparqlParser`) and a
  // `QueryExecutionTree` (created by the `QueryPlanner` using the
  // `ParsedQuery`) into a sequence of bytes that represents the result of the
  // computed query in the format specified by the `mediaType`. Supported
  // formats for this function are `SparqlJSON` and `QLeverJSON`. Note that the
  // SparqlJSON format can only be used with SELECT queries. Invalid
  // `mediaType`s and invalid combinations of `mediaType` and the query type
  // will throw. The result is returned as a single JSON object that is fully
  // materialized before the function returns. The `requestTimer` is used to
  // report timing statistics on the query. It must have already run during the
  // query planning to produce the expected results. If `maxSend` is smaller
  // than the size of the query result, then only the first `maxSend` rows are
  // returned.
  static nlohmann::json queryToJSON(const ParsedQuery& parsedQuery,
                                    const QueryExecutionTree& qet,
                                    ad_utility::Timer& requestTimer,
                                    size_t maxSend, MediaType mediaType);

 private:
  // TODO<joka921> The following functions are all internally called by the
  // two public functions above. All the code has been inside QLever for a long
  // time (previously in the classes `QueryExecutionTree` and `Server`. Clean
  // up their interfaces (are all these functions needed, or can some of them
  // be merged) and write unit tests for them, potentially based on the W3C
  // tests for the output formats. But this needs further cleanups first and
  // this encapsulation is a valuable first step.

  // Similar to `queryToJSON`, but always returns the `QLeverJSON` format.
  static nlohmann::json queryToQLeverJSON(const ParsedQuery& query,
                                          const QueryExecutionTree& qet,
                                          ad_utility::Timer& requestTimer,
                                          size_t maxSend);

  // ___________________________________________________________________________
  static nlohmann::json selectQueryToQLeverJSONArray(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause, size_t limit,
      size_t offset, shared_ptr<const ResultTable> resultTable);

  /**
   * @brief Convert an `IdTable` (typically from a query result) to a json array
   *        In the `QLeverJSON` format. This function is called by
   * `queryToQLeverJSON` to obtain the "actual" query results (without the meta
   * data)
   * @param qet The `QueryExecutionTree` of the query.
   * @param from the first <from> entries of the idTable are skipped
   * @param limit at most <limit> entries are written, starting at <from>
   * @param columns each pair of <columnInIdTable, correspondingType> tells
   * us which columns are to be serialized in which order
   * @param resultTable The query result in the ID space. If it is `nullptr`,
   *        then the query result will be obtained via `qet->getResult()`.
   * @return a 2D-Json array corresponding to the IdTable given the arguments
   */
  static nlohmann::json idTableToQLeverJSONArray(
      const QueryExecutionTree& qet, size_t from, size_t limit,
      const QueryExecutionTree::ColumnIndicesAndTypes& columns,
      std::shared_ptr<const ResultTable> resultTable = nullptr);

  // ___________________________________________________________________________
  static nlohmann::json constructQueryToQLeverJSONArray(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> res);

  // Similar to `queryToJSON`, but always returns the `SparqlJSON` format.
  static nlohmann::json queryToSparqlJSON(const ParsedQuery& query,
                                          const QueryExecutionTree& qet,
                                          ad_utility::Timer& requestTimer,
                                          size_t maxSend);

  // Generate an RDF graph for a CONSTRUCT query.
  static cppcoro::generator<QueryExecutionTree::StringTriple>
  constructQueryToTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> res);

  // ___________________________________________________________________________
  static ad_utility::streams::stream_generator constructQueryToTurtle(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> resultTable);

  // ___________________________________________________________________________
  static nlohmann::json selectQueryToSparqlJSON(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause, size_t limit,
      size_t offset, shared_ptr<const ResultTable> resultTable);

  // ___________________________________________________________________________
  template <MediaType format>
  static ad_utility::streams::stream_generator writeRdfGraphSeparatedValues(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> resultTable);

  // _____________________________________________________________________________
  template <MediaType format>
  static ad_utility::streams::stream_generator generateResults(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause, size_t limit,
      size_t offset);

  // ___________________________________________________________________________
  static ad_utility::streams::stream_generator composeTurtleResponse(
      const ParsedQuery& query, const QueryExecutionTree& qet);

  // ___________________________________________________________________________
  [[nodiscard]] static std::optional<std::pair<std::string, const char*>>
  idToStringAndType(const QueryExecutionTree& qet, Id id,
                    const ResultTable& resultTable);
};
