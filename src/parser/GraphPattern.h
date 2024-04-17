//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

#include "parser/data/SparqlFilter.h"

namespace parsedQuery {

// Forward declarations.
struct GraphPatternOperation;

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
  // `languageInQuotes` is the language.
  void addLanguageFilter(const Variable& variable,
                         const std::string& languageInQuotes);

  bool _optional;

  // Filters always apply to the complete GraphPattern, no matter where
  // they appear. For VALUES and Triples, the order matters, so they
  // become children.
  std::vector<SparqlFilter> _filters;
  std::vector<GraphPatternOperation> _graphPatterns;
};
}  // namespace parsedQuery
