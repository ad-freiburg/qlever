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
  void toString(std::ostringstream& os, int indentation = 0) const;
  // Traverse the graph pattern tree and assigns a unique ID to every graph
  // pattern.
  void recomputeIds(size_t* id_count = nullptr);

  // Modify query to take care of language filter. `variable` is the variable,
  // `languageInQuotes` is the language.
  void addLanguageFilter(const std::string& variable,
                         const std::string& languageInQuotes);

  bool _optional;

  // An ID that is only used by the QueryPlanner.
  // TODO<joka921> This should not be part of this class.
  size_t _id = size_t(-1);

  // Filters always apply to the complete GraphPattern, no matter where
  // they appear. For VALUES and Triples, the order matters, so they
  // become children.
  std::vector<SparqlFilter> _filters;
  std::vector<GraphPatternOperation> _graphPatterns;
};
}  // namespace parsedQuery
