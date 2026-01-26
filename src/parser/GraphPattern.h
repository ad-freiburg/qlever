//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_GRAPHPATTERN_H
#define QLEVER_SRC_PARSER_GRAPHPATTERN_H

#include <cstddef>
#include <string>
#include <vector>

#include "parser/data/SparqlFilter.h"
#include "rdfTypes/Variable.h"
#include "util/HashSet.h"

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
  GraphPattern(GraphPattern&& other) noexcept;
  GraphPattern(const GraphPattern& other);
  GraphPattern& operator=(const GraphPattern& other);
  GraphPattern& operator=(GraphPattern&& other) noexcept;
  ~GraphPattern();

  // Modify the query to take care of language filter by using a special
  // predicate that only returns matching literals if applicable. `variable` is
  // the variable to filter on, `langTags` represent a whitelist of languages,
  // indicating that the desired literals have to be of any of the specified
  // languages. Return `true` if it could successfully be applied, false
  // otherwise.
  bool addLanguageFilter(const Variable& variable,
                         const ad_utility::HashSet<std::string>& langTags);

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
