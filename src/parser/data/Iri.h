// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_IRI_H
#define QLEVER_SRC_PARSER_DATA_IRI_H

#include <string>

#include "backports/three_way_comparison.h"
#include "parser/data/ConstructQueryExportContext.h"

// TODO: replace usages of this class with `ad_utility::triple_component::Iri`
class Iri {
  std::string _string;

 public:
  explicit Iri(std::string str);

  // ___________________________________________________________________________
  // Used for testing
  [[nodiscard]] const std::string& iri() const { return _string; }

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      [[maybe_unused]] const ConstructQueryExportContext& context,
      [[maybe_unused]] PositionInTriple role) const {
    return _string;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _string; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Iri, _string)
};

#endif  // QLEVER_SRC_PARSER_DATA_IRI_H
