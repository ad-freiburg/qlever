// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_IRI_H
#define QLEVER_SRC_PARSER_DATA_IRI_H

#include <string>

#include "backports/three_way_comparison.h"

// TODO: replace usages of this class with `ad_utility::triple_component::Iri`
// Stores the QLever internal string representation of an IRI, as returned by
// `ad_utility::triple_component::Iri::toStringRepresentation()`. This is not
// raw parser input: prefixes and base IRIs have already been resolved, and the
// IRI has already been normalized. The representation includes angle brackets
// and may have QLever's internal language-tag prefix, for example
// `@en@<example>`.
class Iri {
 private:
  std::string stringRepresentation_;

 public:
  // `str` must be a valid QLever internal IRI string representation.
  explicit Iri(std::string str);

  // ___________________________________________________________________________
  // Used for testing
  const std::string& iri() const { return stringRepresentation_; }

  // ___________________________________________________________________________
  const std::string& toStringRepresentation() const {
    return stringRepresentation_;
  }

  // ___________________________________________________________________________
  // TODO: Replace this by `toStringRepresentation` at the call sites where
  // SPARQL output is not actually required.
  std::string toSparql() const { return stringRepresentation_; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Iri, stringRepresentation_);
};

#endif  // QLEVER_SRC_PARSER_DATA_IRI_H
