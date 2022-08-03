// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <initializer_list>
#include <string>
#include <vector>

#include "data/Variable.h"

using std::string;
using std::vector;

class PropertyPath {
 public:
  enum class Operation {
    SEQUENCE,
    ALTERNATIVE,
    TRANSITIVE,
    TRANSITIVE_MIN,  // e.g. +
    TRANSITIVE_MAX,  // e.g. *n or ?
    INVERSE,
    IRI
  };

  PropertyPath()
      : _operation(Operation::IRI),
        _limit(0),
        _iri(),
        _children(),
        _can_be_null(false) {}
  explicit PropertyPath(Operation op)
      : _operation(op), _limit(0), _iri(), _children(), _can_be_null(false) {}
  PropertyPath(Operation op, uint16_t limit, std::string iri,
               std::initializer_list<PropertyPath> children);

  static PropertyPath fromIri(std::string iri) {
    PropertyPath p(PropertyPath::Operation::IRI);
    p._iri = std::move(iri);
    return p;
  }

  static PropertyPath fromVariable(Variable var) {
    PropertyPath p(PropertyPath::Operation::IRI);
    p._iri = std::move(var.name());
    return p;
  }

  static PropertyPath makeWithChildren(std::vector<PropertyPath> children,
                                       PropertyPath::Operation op) {
    PropertyPath p(std::move(op));
    p._children = std::move(children);
    return p;
  }

  static PropertyPath makeAlternative(std::vector<PropertyPath> children) {
    if (children.size() == 1) {
      return std::move(children.front());
    } else {
      return makeWithChildren(std::move(children), Operation::ALTERNATIVE);
    }
  }

  static PropertyPath makeSequence(std::vector<PropertyPath> children) {
    if (children.size() == 1) {
      return std::move(children.front());
    } else {
      return makeWithChildren(std::move(children), Operation::SEQUENCE);
    }
  }

  static PropertyPath makeInverse(PropertyPath child) {
    return makeWithChildren({std::move(child)}, Operation::INVERSE);
  }

  static PropertyPath makeWithChildLimit(PropertyPath child,
                                         uint_fast16_t limit,
                                         PropertyPath::Operation op) {
    PropertyPath p = makeWithChildren({std::move(child)}, op);
    p._limit = limit;
    return p;
  }

  static PropertyPath makeTransitiveMin(PropertyPath child,
                                        uint_fast16_t limit) {
    return makeWithChildLimit(std::move(child), limit,
                              Operation::TRANSITIVE_MIN);
  }

  static PropertyPath makeTransitiveMax(PropertyPath child,
                                        uint_fast16_t limit) {
    return makeWithChildLimit(std::move(child), limit,
                              Operation::TRANSITIVE_MAX);
  }

  static PropertyPath makeTransitive(PropertyPath child) {
    return makeWithChildren({std::move(child)}, Operation::TRANSITIVE);
  }

  bool operator==(const PropertyPath& other) const = default;

  void writeToStream(std::ostream& out) const;
  [[nodiscard]] std::string asString() const;

  void computeCanBeNull();

  Operation _operation;
  // For the limited transitive operations
  uint_fast16_t _limit;

  // In case of an iri
  std::string _iri;

  std::vector<PropertyPath> _children;

  /**
   * True iff this property path is either a transitive path with minimum length
   * of 0, or if all of this transitive path's children can be null.
   */
  bool _can_be_null;
};
