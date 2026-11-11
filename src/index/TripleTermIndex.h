// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_TRIPLETERMINDEX_H
#define QLEVER_SRC_INDEX_TRIPLETERMINDEX_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "global/Id.h"
#include "global/TripleTermIndex.h"

namespace qlever::tripleTermPrototype {

// First in-memory prototype: components must already be final, global `Id`s
// (after vocabulary merging, never partial IDs of one batch). This is not an
// RDF parser or an on-disk index. The supported scope is non-nested, IRI-only
// terms, so every component must be an IRI `Id`: a `VocabIndex` or an IRI that
// is encoded directly into the `Id` (`EncodedVal`). Other datatypes, including
// nested term references and query-local `LocalVocabIndex` entries, are
// rejected.
class TripleTermIndexBuilder {
 public:
  using Triple = std::array<Id, 3>;              // Subject, predicate, object.
  using Match = std::pair<Id, TripleTermIndex>;  // Subject, term ID.

 private:
  std::map<Triple, TripleTermIndex> ids_;
  std::vector<Triple> terms_;
  // Separate P,O,I,S permutation; these records are never asserted triples.
  using PoisKey = std::tuple<Id, Id, TripleTermIndex>;
  std::map<PoisKey, Id> pois_;

  static bool isSupportedComponent(Id id) {
    auto datatype = id.getDatatype();
    return datatype == Datatype::VocabIndex || datatype == Datatype::EncodedVal;
  }

 public:
  // Reuse IDs for identical structures. Assign a new ID before adding its
  // permutation entry. Roll back on allocation failure to preserve consistency.
  TripleTermIndex intern(const Triple& triple) {
    for (Id component : triple) {
      if (!isSupportedComponent(component)) {
        throw std::invalid_argument(
            "Triple-term prototype components must be IRIs (VocabIndex or "
            "EncodedVal)");
      }
    }
    auto [entry, inserted] =
        ids_.try_emplace(triple, TripleTermIndex::make(terms_.size()));
    if (!inserted) {
      return entry->second;
    }
    try {
      terms_.push_back(triple);
      try {
        pois_.emplace(PoisKey{triple[1], triple[2], entry->second}, triple[0]);
      } catch (...) {
        terms_.pop_back();
        throw;
      }
    } catch (...) {
      ids_.erase(entry);
      throw;
    }
    return entry->second;
  }

  // Return a copy so later insertion cannot invalidate a caller's result.
  Triple resolve(TripleTermIndex id) const { return terms_.at(id.get()); }

  Id internId(const Triple& triple) {
    return Id::makeFromTripleTermIndex(intern(triple));
  }

  Triple resolve(Id id) const {
    if (id.getDatatype() != Datatype::TripleTermIndex) {
      throw std::invalid_argument("Expected a triple-term reference");
    }
    return resolve(id.getTripleTermIndex());
  }

  size_t size() const { return terms_.size(); }

  // All terms, in the order of their `TripleTermIndex`. Interning them in this
  // order into an empty builder reproduces the same IDs.
  const std::vector<Triple>& terms() const { return terms_; }

  // Match (?subject, predicate, object), returning both the variable binding
  // and the term ID needed to join with an outer statement.
  std::vector<Match> scan(Id predicate, Id object) const {
    std::vector<Match> result;
    auto it =
        pois_.lower_bound(PoisKey{predicate, object, TripleTermIndex::make(0)});
    for (; it != pois_.end(); ++it) {
      const auto& [p, o, id] = it->first;
      if (p != predicate || o != object) {
        break;
      }
      result.emplace_back(it->second, id);
    }
    return result;
  }

  // Runtime bindings for ordinary result columns and joins: subject, term ID.
  std::vector<std::array<Id, 2>> scanIds(Id predicate, Id object) const {
    std::vector<std::array<Id, 2>> result;
    for (const auto& [subject, term] : scan(predicate, object)) {
      result.push_back({subject, Id::makeFromTripleTermIndex(term)});
    }
    return result;
  }
};
}  // namespace qlever::tripleTermPrototype

#endif  // QLEVER_SRC_INDEX_TRIPLETERMINDEX_H
