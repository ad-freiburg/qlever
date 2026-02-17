// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTemplatePreprocessor.h"

#include <absl/strings/str_cat.h>

#include "engine/ConstructQueryEvaluator.h"
#include "util/Algorithm.h"
#include "util/TypeTraits.h"

namespace qlever::constructExport {
// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessTerm(
    const GraphTerm& term, PositionInTriple role,
    const VariableToColumnMap& variableColumns) {
  return std::visit(
      [&role,
       &variableColumns](const auto& t) -> std::optional<PreprocessedTerm> {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, Iri>) {
          return PrecomputedConstant{ConstructQueryEvaluator::evaluate(t)};

        } else if constexpr (std::is_same_v<T, Literal>) {
          auto opt = ConstructQueryEvaluator::evaluate(t, role);
          if (opt) {
            return PrecomputedConstant(std::move(*opt));
          }
          return std::nullopt;

        } else if constexpr (std::is_same_v<T, Variable>) {
          if (auto opt = ad_utility::findOptional(variableColumns, t)) {
            size_t columnIndex = opt->columnIndex_;
            return PrecomputedVariable{columnIndex};
          }
          return std::nullopt;

        } else if constexpr (std::is_same_v<T, BlankNode>) {
          return PrecomputedBlankNode{t.isGenerated() ? "_:g" : "_:u",
                                      absl::StrCat("_", t.label())};

        } else {
          static_assert(ad_utility::alwaysFalse<T>);
        }
      },
      term);
}

// _____________________________________________________________________________
PreprocessedConstructTemplate ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples,
    const VariableToColumnMap& variableColumns) {
  PreprocessedConstructTemplate result;

  ad_utility::HashSet<size_t> uniqueColumnsSet;

  for (const auto& triple : templateTriples) {
    PreprocessedTriple preprocessedTriple;
    bool valid = true;

    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);
      auto preprocessed = preprocessTerm(triple[pos], role, variableColumns);
      if (!preprocessed) {
        valid = false;
        break;
      }
      preprocessedTriple[pos] = std::move(*preprocessed);
    }

    if (valid) {
      // only now add the column indices of `PrecomputedVariable`s to the
      // `uniqueColumnsSet`, since only now we can be sure that the triple is
      // indeed valid.
      for (const auto& term : preprocessedTriple) {
        if (auto* var = std::get_if<PrecomputedVariable>(&term)) {
          uniqueColumnsSet.insert(var->columnIndex_);
        }
      }
      result.preprocessedTriples_.push_back(std::move(preprocessedTriple));
    }
  }

  result.uniqueVariableColumns_.assign(uniqueColumnsSet.begin(),
                                       uniqueColumnsSet.end());
  return result;
}

}  // namespace qlever::constructExport
