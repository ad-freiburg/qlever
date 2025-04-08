//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_GRAPHPATTERN_H
#define QLEVER_SRC_PARSER_GRAPHPATTERN_H

#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

#include "parser/TripleComponent.h"
#include "parser/data/SparqlFilter.h"

namespace parsedQuery {

// Forward declarations.
struct GraphPatternOperation;

// Struct that store meta information about a text limit operation.
// It is used to store the variables that are used in the operation and the IDs
// of the operations that must be completed before applying the text limit
// operation.
struct TextLimitMetaObject {
  std::vector<Variable> entityVars_;
  std::vector<Variable> scoreVars_;
  uint64_t idsOfMustBeFinishedOperations_;
};

// Groups triplets and filters. Represents a node in a tree (as graph patterns
// are recursive).
class GraphPattern {
 public:
  // The constructor has to be implemented in the .cpp file because of the
  // circular dependencies of `GraphPattern` and `GraphPatternOperation`.
  GraphPattern();
  GraphPattern(GraphPattern&& other);
  GraphPattern(const GraphPattern& other);
  GraphPattern& operator=(const GraphPattern& other);
  GraphPattern& operator=(GraphPattern&& other) noexcept;
  ~GraphPattern();
  // Traverse the graph pattern tree and assigns a unique ID to every graph
  // pattern.

  // Modify query to take care of language filter. `variable` is the variable,
  // `languageInQuotes` is the language. Return `true` if it could successfully
  // be applied, false otherwise.
  bool addLanguageFilter(const Variable& variable,
                         const std::string& languageInQuotes);

  bool _optional;

  // Filters always apply to the complete GraphPattern, no matter where
  // they appear. For VALUES and Triples, the order matters, so they
  // become children.
  std::vector<SparqlFilter> _filters;
  std::vector<GraphPatternOperation> _graphPatterns;

  // Hashmap that stores for each text variable the corresponding
  // TextLimitMetaObject
  ad_utility::HashMap<Variable, TextLimitMetaObject> textLimits_;
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_GRAPHPATTERN_H
