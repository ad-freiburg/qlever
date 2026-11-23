// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_PARSEDTRIPLETERM_H
#define QLEVER_SRC_PARSER_PARSEDTRIPLETERM_H

#include <memory>
#include <utility>

class TripleComponent;

// The representation of an RDF 1.2 triple term.
// Since there is a cyclic dependency between `TripleComponent` and
// `ParsedTripleTerm` we need to use a pointer indirection to break this
// dependency.
//
// Although we store the contents via `shared_ptr`, the implicitly generated
// copy and move operations are safe because the contents are immutable and this
// class only grants const access to them. Copies share ownership; moves
// transfer ownership. Equality and hashing compare contents rather than
// allocation addresses.
class ParsedTripleTerm {
  struct Components;
  std::shared_ptr<const Components> components_;
  bool isEqual(const ParsedTripleTerm& other) const;

 public:
  ParsedTripleTerm(TripleComponent subject, TripleComponent predicate,
                   TripleComponent object);
  // The accessors require a term that was not moved from (a moved-from term
  // can be assigned again).
  const TripleComponent& subject() const;
  const TripleComponent& predicate() const;
  const TripleComponent& object() const;
  friend bool operator==(const ParsedTripleTerm& a, const ParsedTripleTerm& b) {
    return a.isEqual(b);
  }
#ifdef QLEVER_CPP_17
  friend bool operator!=(const ParsedTripleTerm& a, const ParsedTripleTerm& b) {
    return !(a == b);
  }
#endif
  template <typename H>
  friend H AbslHashValue(H h, const ParsedTripleTerm& term) {
    return H::combine(std::move(h), term.subject(), term.predicate(),
                      term.object());
  }
};

#endif  // QLEVER_SRC_PARSER_PARSEDTRIPLETERM_H
