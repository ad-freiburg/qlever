// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#pragma once

#include <string>
#include <vector>

// Full IRI constants (with angle brackets) for OWL/RDFS predicates and
// classes referenced in rule definitions. Used for semi-naive dependency
// tracking.
namespace reasoner_iris {
inline constexpr std::string_view RDF_TYPE =
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
inline constexpr std::string_view RDFS_SUB_CLASS_OF =
    "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
inline constexpr std::string_view RDFS_SUB_PROPERTY_OF =
    "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
inline constexpr std::string_view RDFS_DOMAIN =
    "<http://www.w3.org/2000/01/rdf-schema#domain>";
inline constexpr std::string_view RDFS_RANGE =
    "<http://www.w3.org/2000/01/rdf-schema#range>";
inline constexpr std::string_view OWL_EQUIVALENT_CLASS =
    "<http://www.w3.org/2002/07/owl#equivalentClass>";
inline constexpr std::string_view OWL_EQUIVALENT_PROPERTY =
    "<http://www.w3.org/2002/07/owl#equivalentProperty>";
inline constexpr std::string_view OWL_INVERSE_OF =
    "<http://www.w3.org/2002/07/owl#inverseOf>";
inline constexpr std::string_view OWL_SAME_AS =
    "<http://www.w3.org/2002/07/owl#sameAs>";

// Sentinel value for rules whose CONSTRUCT head uses a variable predicate
// (e.g., prp-spo1, prp-inv1/2, prp-symp, prp-trp). When present in
// `outputPredicates`, it signals that a wildcard activation is needed.
// When present in `inputPredicates`, it signals that the rule can be
// triggered by any newly-active predicate.
inline constexpr std::string_view WILDCARD = "*";
}  // namespace reasoner_iris

// A single OWL 2 RL / RDFS reasoning rule expressed as a SPARQL CONSTRUCT
// query.
struct ReasonerRule {
  // W3C rule identifier (e.g., "cax-sco", "scm-sco", "prp-dom").
  std::string name;
  // Human-readable description.
  std::string description;
  // Complete SPARQL CONSTRUCT query string (with PREFIX declarations).
  std::string constructQuery;
  // Predicate IRIs (with angle brackets) referenced in the WHERE clause.
  // Used for semi-naive evaluation: the rule is re-scheduled when any of
  // these predicates gains new triples in a previous round. Use WILDCARD
  // ("*") to indicate the rule can be triggered by any predicate.
  std::vector<std::string> inputPredicates;
  // Predicate IRIs produced in the CONSTRUCT head. Use WILDCARD ("*") for
  // rules with a variable predicate in the head (e.g., prp-spo1).
  std::vector<std::string> outputPredicates;
};

// Returns all OWL 2 RL / RDFS reasoning rules in execution order.
// Schema-level rules (class/property hierarchy) are placed first so that
// the fully-materialized schema is available to instance-level rules within
// the same round, minimising the total number of rounds required.
//
// The vector is constructed exactly once (function-local static) to avoid
// rebuilding 14 heap-allocated ReasonerRule objects on every call.
inline const std::vector<ReasonerRule>& getAllReasonerRules() {
  using namespace reasoner_iris;
  static const std::vector<ReasonerRule> kRules = {
      // ── Schema-level rules ──────────────────────────────────────────────

      // cax-eqc  (OWL 2 RL Table 7)
      // owl:equivalentClass ↔ rdfs:subClassOf (bidirectional in one CONSTRUCT).
      {
          "cax-eqc",
          "EquivalentClass expands to mutual subClassOf",
          R"SPARQL(PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?c1 rdfs:subClassOf ?c2 . ?c2 rdfs:subClassOf ?c1 }
WHERE      { ?c1 owl:equivalentClass ?c2 })SPARQL",
          {std::string(OWL_EQUIVALENT_CLASS)},
          {std::string(RDFS_SUB_CLASS_OF)},
      },

      // prp-eqp  (OWL 2 RL Table 5)
      // owl:equivalentProperty ↔ rdfs:subPropertyOf (bidirectional).
      {
          "prp-eqp",
          "EquivalentProperty expands to mutual subPropertyOf",
          R"SPARQL(PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?p1 rdfs:subPropertyOf ?p2 . ?p2 rdfs:subPropertyOf ?p1 }
WHERE      { ?p1 owl:equivalentProperty ?p2 })SPARQL",
          {std::string(OWL_EQUIVALENT_PROPERTY)},
          {std::string(RDFS_SUB_PROPERTY_OF)},
      },

      // scm-sco  (OWL 2 RL Table 9)
      // rdfs:subClassOf full transitive closure via SPARQL property path (+).
      // Using a property path computes the closure in a single query instead
      // of iterating one hop per round.
      {
          "scm-sco",
          "SubClassOf transitivity (full closure via property path)",
          R"SPARQL(PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?a rdfs:subClassOf ?b }
WHERE      { ?a rdfs:subClassOf+ ?b })SPARQL",
          {std::string(RDFS_SUB_CLASS_OF)},
          {std::string(RDFS_SUB_CLASS_OF)},
      },

      // scm-spo  (OWL 2 RL Table 9)
      // rdfs:subPropertyOf full transitive closure via SPARQL property path.
      {
          "scm-spo",
          "SubPropertyOf transitivity (full closure via property path)",
          R"SPARQL(PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?a rdfs:subPropertyOf ?b }
WHERE      { ?a rdfs:subPropertyOf+ ?b })SPARQL",
          {std::string(RDFS_SUB_PROPERTY_OF)},
          {std::string(RDFS_SUB_PROPERTY_OF)},
      },

      // eq-sym  (OWL 2 RL Table 4)
      // owl:sameAs symmetry.
      {
          "eq-sym",
          "sameAs symmetry",
          R"SPARQL(PREFIX owl: <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?y owl:sameAs ?x }
WHERE      { ?x owl:sameAs ?y })SPARQL",
          {std::string(OWL_SAME_AS)},
          {std::string(OWL_SAME_AS)},
      },

      // eq-trans  (OWL 2 RL Table 4)
      // owl:sameAs transitivity via property path (full closure).
      {
          "eq-trans",
          "sameAs transitivity (full closure via property path)",
          R"SPARQL(PREFIX owl: <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?x owl:sameAs ?z }
WHERE      { ?x owl:sameAs+ ?z })SPARQL",
          {std::string(OWL_SAME_AS)},
          {std::string(OWL_SAME_AS)},
      },

      // ── Instance-level rules ─────────────────────────────────────────────

      // cax-sco  (OWL 2 RL Table 7)
      // rdf:type propagation through rdfs:subClassOf.
      {
          "cax-sco",
          "Type propagation through subClassOf",
          R"SPARQL(PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?x rdf:type ?c2 }
WHERE      { ?x rdf:type ?c1 . ?c1 rdfs:subClassOf ?c2 })SPARQL",
          {std::string(RDF_TYPE), std::string(RDFS_SUB_CLASS_OF)},
          {std::string(RDF_TYPE)},
      },

      // prp-spo1  (OWL 2 RL Table 5)
      // Triple propagation through rdfs:subPropertyOf. The output predicate is
      // a variable (?p2), so we use WILDCARD in outputPredicates.
      {
          "prp-spo1",
          "Triple propagation through subPropertyOf",
          R"SPARQL(PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?x ?p2 ?y }
WHERE      { ?p1 rdfs:subPropertyOf ?p2 . ?x ?p1 ?y })SPARQL",
          {std::string(RDFS_SUB_PROPERTY_OF), std::string(WILDCARD)},
          {std::string(WILDCARD)},
      },

      // prp-dom  (OWL 2 RL Table 5)
      // rdf:type inference from rdfs:domain.
      {
          "prp-dom",
          "Type inference from rdfs:domain",
          R"SPARQL(PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?x rdf:type ?c }
WHERE      { ?p rdfs:domain ?c . ?x ?p ?y })SPARQL",
          {std::string(RDFS_DOMAIN), std::string(WILDCARD)},
          {std::string(RDF_TYPE)},
      },

      // prp-rng  (OWL 2 RL Table 5)
      // rdf:type inference from rdfs:range.
      {
          "prp-rng",
          "Type inference from rdfs:range",
          R"SPARQL(PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
CONSTRUCT { ?y rdf:type ?c }
WHERE      { ?p rdfs:range ?c . ?x ?p ?y })SPARQL",
          {std::string(RDFS_RANGE), std::string(WILDCARD)},
          {std::string(RDF_TYPE)},
      },

      // prp-inv1  (OWL 2 RL Table 5)
      // Inverse property: given ?p1 inverseOf ?p2, ?x ?p1 ?y → ?y ?p2 ?x.
      {
          "prp-inv1",
          "Inverse property (direction 1: p1 triples become p2)",
          R"SPARQL(PREFIX owl: <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?y ?p2 ?x }
WHERE      { ?p1 owl:inverseOf ?p2 . ?x ?p1 ?y })SPARQL",
          {std::string(OWL_INVERSE_OF), std::string(WILDCARD)},
          {std::string(WILDCARD)},
      },

      // prp-inv2  (OWL 2 RL Table 5)
      // Inverse property: given ?p1 inverseOf ?p2, ?x ?p2 ?y → ?y ?p1 ?x.
      {
          "prp-inv2",
          "Inverse property (direction 2: p2 triples become p1)",
          R"SPARQL(PREFIX owl: <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?y ?p1 ?x }
WHERE      { ?p1 owl:inverseOf ?p2 . ?x ?p2 ?y })SPARQL",
          {std::string(OWL_INVERSE_OF), std::string(WILDCARD)},
          {std::string(WILDCARD)},
      },

      // prp-symp  (OWL 2 RL Table 5)
      // Symmetric property: ?p rdf:type owl:SymmetricProperty, ?x ?p ?y → ?y ?p
      // ?x.
      {
          "prp-symp",
          "Symmetric property",
          R"SPARQL(PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?y ?p ?x }
WHERE      { ?p rdf:type owl:SymmetricProperty . ?x ?p ?y })SPARQL",
          {std::string(RDF_TYPE), std::string(WILDCARD)},
          {std::string(WILDCARD)},
      },

      // prp-trp  (OWL 2 RL Table 5)
      // Transitive property (iterative join — cannot use property path because
      // the predicate is a variable).
      {
          "prp-trp",
          "Transitive property (iterative)",
          R"SPARQL(PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
CONSTRUCT { ?x ?p ?z }
WHERE      { ?p rdf:type owl:TransitiveProperty . ?x ?p ?y . ?y ?p ?z })SPARQL",
          {std::string(RDF_TYPE), std::string(WILDCARD)},
          {std::string(WILDCARD)},
      },
  };
  return kRules;
}
