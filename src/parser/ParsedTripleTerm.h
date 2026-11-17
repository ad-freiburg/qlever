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

// Immutable, recursively owned parsed contents. `TripleComponent` contains a
// `ParsedTripleTerm`, whose `Components` contain three `TripleComponent`s.
// Pointer indirection breaks this recursive type dependency and allows
// `Components` to be defined out of line.
//
// The implicitly generated copy and move operations are safe because the
// contents are immutable and this class only grants const access to them.
// Copies share ownership; moves transfer ownership. Equality and hashing
// compare contents rather than allocation addresses.
//
// The RDF parser enforces the permitted component kinds. This syntactic type
// does not assign IDs or define SPARQL value equality.
class ParsedTripleTerm {
  struct Components;
  std::shared_ptr<const Components> components_;

 public:
  ParsedTripleTerm(TripleComponent subject, TripleComponent predicate,
                   TripleComponent object);
  // Access requires a non-moved-from term. Copies share immutable contents;
  // moves transfer ownership, and a moved-from term can be assigned again.
  const TripleComponent& subject() const;
  const TripleComponent& predicate() const;
  const TripleComponent& object() const;
  friend bool operator==(const ParsedTripleTerm& a, const ParsedTripleTerm& b);
  friend bool operator!=(const ParsedTripleTerm& a, const ParsedTripleTerm& b) {
    return !(a == b);
  }
  template <typename H>
  friend H AbslHashValue(H h, const ParsedTripleTerm& term) {
    return H::combine(std::move(h), term.subject(), term.predicate(),
                      term.object());
  }
};

#endif  // QLEVER_SRC_PARSER_PARSEDTRIPLETERM_H
