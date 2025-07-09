//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//           Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_PARSER_SPARQLTRIPLE_H
#define QLEVER_SRC_PARSER_SPARQLTRIPLE_H

#include <utility>
#include <vector>

#include "global/Id.h"
#include "parser/PropertyPath.h"
#include "parser/TripleComponent.h"
#include "parser/data/Variable.h"

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

class SparqlTripleSimpleWithGraph : public SparqlTripleSimple {
 public:
  using Graph = std::variant<std::monostate, TripleComponent::Iri, Variable>;

  SparqlTripleSimpleWithGraph(TripleComponent s, TripleComponent p,
                              TripleComponent o, Graph g,
                              AdditionalScanColumns additionalScanColumns = {})
      : SparqlTripleSimple(std::move(s), std::move(p), std::move(o),
                           std::move(additionalScanColumns)),
        g_{std::move(g)} {}
  Graph g_;

  bool operator==(const SparqlTripleSimpleWithGraph&) const = default;
};

// A triple where the predicate is a `PropertyPath` or a `Variable`.
class SparqlTriple
    : public SparqlTripleBase<ad_utility::sparql_types::VarOrPath> {
 public:
  using Base = SparqlTripleBase<ad_utility::sparql_types::VarOrPath>;
  using Base::Base;

  // TODO<RobinTF> make this constructor accept a type-safe IRI instead of a
  // string
  // ___________________________________________________________________________
  SparqlTriple(TripleComponent s, const std::string& iri, TripleComponent o)
      : Base{std::move(s),
             PropertyPath::fromIri(TripleComponent::Iri::fromIriref(iri)),
             std::move(o)} {}

  // ___________________________________________________________________________
  [[nodiscard]] string asString() const;

  // Convert to a simple triple. Fails with an exception if the predicate
  // actually is a property path.
  SparqlTripleSimple getSimple() const {
    bool holdsVariable = std::holds_alternative<Variable>(p_);
    auto predicate = getSimplePredicate();
    AD_CONTRACT_CHECK(holdsVariable || predicate.has_value());
    TripleComponent p =
        holdsVariable
            ? TripleComponent{std::get<Variable>(p_)}
            : TripleComponent(ad_utility::triple_component::Iri::fromIriref(
                  predicate.value()));
    return {s_, p, o_, additionalScanColumns_};
  }

  // Constructs SparqlTriple from a simple triple. Fails with an exception if
  // the predicate is neither a variable nor an iri.
  static SparqlTriple fromSimple(const SparqlTripleSimple& triple) {
    AD_CONTRACT_CHECK(triple.p_.isVariable() || triple.p_.isIri());
    if (triple.p_.isVariable()) {
      return {triple.s_, triple.p_.getVariable(), triple.o_};
    }
    return {triple.s_, PropertyPath::fromIri(triple.p_.getIri()), triple.o_};
  }

  // If the predicate of the triple is a simple IRI (neither a variable nor a
  // complex property path), return it. Else return `nullopt`. Note: the
  // lifetime of the return value is bound to the lifetime of the triple as the
  // optional stores a `string_view`.
  std::optional<std::string_view> getSimplePredicate() const {
    if (!std::holds_alternative<PropertyPath>(p_)) {
      return std::nullopt;
    }
    const auto& path = std::get<PropertyPath>(p_);
    return path.isIri()
               ? std::optional<std::string_view>{path.getIri()
                                                     .toStringRepresentation()}
               : std::nullopt;
  }

  // If the predicate of the triples is a variable, return it. Note:
  // the lifetime of the return value is bound to the lifetime of the triple,
  // as the optional stores a reference.
  boost::optional<const Variable&> getPredicateVariable() const {
    return std::holds_alternative<Variable>(p_)
               ? boost::optional<const Variable&>{std::get<Variable>(p_)}
               : boost::none;
  }

  // Return true iff the predicate is a variable that is equal to the argument.
  bool predicateIs(const Variable& variable) const {
    auto ptr = std::get_if<Variable>(&p_);
    return (ptr != nullptr && *ptr == variable);
  }
};

#endif  // QLEVER_SRC_PARSER_SPARQLTRIPLE_H
