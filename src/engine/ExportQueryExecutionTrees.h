//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <cstdlib>

#include "engine/QueryExecutionTree.h"
#include "nlohmann/json.hpp"

#pragma once

class ExportQueryExecutionTrees {
 public:
  /**
   * @brief Convert an IdTable (typically from a query result) to a json array
   * @param data the IdTable from which we read
   * @param from the first <from> entries of the idTable are skipped
   * @param limit at most <limit> entries are written, starting at <from>
   * @param columns each pair of <columnInIdTable, correspondingType> tells
   * us which columns are to be serialized in which order
   * @return a 2D-Json array corresponding to the IdTable given the arguments
   */
  static nlohmann::json writeQLeverJsonTable(
      const QueryExecutionTree& qet, size_t from, size_t limit,
      const QueryExecutionTree::ColumnIndicesAndTypes& columns,
      std::shared_ptr<const ResultTable> resultTable = nullptr);

  // Generate an RDF graph for a CONSTRUCT query.
  static cppcoro::generator<QueryExecutionTree::StringTriple> generateRdfGraph(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> res);

  // _____________________________________________________________________________
  static ad_utility::streams::stream_generator writeRdfGraphTurtle(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> resultTable);

  // _____________________________________________________________________________
  static nlohmann::json writeRdfGraphJson(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
      size_t offset, std::shared_ptr<const ResultTable> res);

  // _____________________________________________________________________________
  static nlohmann::json writeResultAsSparqlJson(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause, size_t limit,
      size_t offset, shared_ptr<const ResultTable> resultTable);

 private:
  [[nodiscard]] static std::optional<std::pair<std::string, const char*>>
  idToStringAndType(const QueryExecutionTree& qet, Id id,
                    const ResultTable& resultTable);
};
