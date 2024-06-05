//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//           Johannes Kalmbach <johannes.kalmbach@gmail.com>

#pragma once

#include <vector>

#include "PropertyPath.h"
#include "TripleComponent.h"
#include "global/Id.h"
#include "parser/data/Variable.h"

inline bool isVariable(const string& elem) { return elem.starts_with("?"); }
inline bool isVariable(const TripleComponent& elem) {
  return elem.isVariable();
}

inline bool isVariable(const PropertyPath& elem) {
  return elem._operation == PropertyPath::Operation::IRI &&
         isVariable(elem._iri);
}

// Data container for parsed triples from the where clause.
// It is templated on the predicate type, see the instantiations below.
template <typename Predicate>
class SparqlTripleBase {
 public:
  using AdditionalScanColumns = std::vector<std::pair<ColumnIndex, Variable>>;
  SparqlTripleBase(TripleComponent s, Predicate p, TripleComponent o,
                   AdditionalScanColumns additionalScanColumns = {})
      : s_(std::move(s)),
        p_(std::move(p)),
        o_(std::move(o)),
        additionalScanColumns_(std::move(additionalScanColumns)) {}

  bool operator==(const SparqlTripleBase& other) const = default;
  TripleComponent s_;
  Predicate p_;
  TripleComponent o_;
  // The additional columns (e.g. patterns) that are to be attached when
  // performing an index scan using this triple.
  // TODO<joka921> On this level we should not store `ColumnIndex`, but the
  // special predicate IRIs that are to be attached here.
  std::vector<std::pair<ColumnIndex, Variable>> additionalScanColumns_;
};

// A triple where the predicate is a `TripleComponent`, so a fixed entity or a
// variable, but not a property path.
class SparqlTripleSimple : public SparqlTripleBase<TripleComponent> {
  using Base = SparqlTripleBase<TripleComponent>;
  using Base::Base;
};

// A triple where the predicate is a `PropertyPath` (which technically still
// might be a variable or fixed entity in the current implementation).
class SparqlTriple : public SparqlTripleBase<PropertyPath> {
 public:
  using Base = SparqlTripleBase<PropertyPath>;
  using Base::Base;

  // ___________________________________________________________________________
  SparqlTriple(TripleComponent s, const std::string& iri, TripleComponent o)
      : Base{std::move(s), PropertyPath::fromIri(iri), std::move(o)} {}

  // ___________________________________________________________________________
  [[nodiscard]] string asString() const;

  // Convert to a simple triple. Fails with an exception if the predicate
  // actually is a property path.
  SparqlTripleSimple getSimple() const {
    AD_CONTRACT_CHECK(p_.isIri());
    TripleComponent p =
        isVariable(p_._iri)
            ? TripleComponent{Variable{p_._iri}}
            : TripleComponent(TripleComponent::Iri::fromIriref(p_._iri));
    return {s_, p, o_, additionalScanColumns_};
  }

  // Constructs SparqlTriple from a simple triple. Fails with an exception if
  // the predicate is neither a variable nor an iri.
  static SparqlTriple fromSimple(const SparqlTripleSimple& triple) {
    AD_CONTRACT_CHECK(triple.p_.isVariable() || triple.p_.isIri());
    PropertyPath p = triple.p_.isVariable()
                         ? PropertyPath::fromVariable(triple.p_.getVariable())
                         : PropertyPath::fromIri(
                               triple.p_.getIri().toStringRepresentation());
    return {triple.s_, p, triple.o_};
  }
};
