// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_CONSTRUCTQUERYEXPORTCONTEXT_H
#define QLEVER_SRC_PARSER_DATA_CONSTRUCTQUERYEXPORTCONTEXT_H

#include "engine/Result.h"
#include "engine/VariableToColumnMap.h"
#include "rdfTypes/Variable.h"

// Forward declarations to avoid cyclic dependencies
namespace qlever {
class Index;
class LocalVocab;
}  // namespace qlever
enum struct PositionInTriple : int { SUBJECT, PREDICATE, OBJECT };

// All the data that is needed to evaluate an element in a construct query.
struct ConstructQueryExportContext {
  // idx of row of result table for WHERE-clause
  const size_t resultTableRowIndex_;
  IdTableView<0> idTable_;
  const qlever::LocalVocab& localVocab_;
  const qlever::VariableToColumnMap& _variableColumns;
  const qlever::Index& _qecIndex;
  const size_t _rowOffset;
};
#endif  // QLEVER_SRC_PARSER_DATA_CONSTRUCTQUERYEXPORTCONTEXT_H
