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
PreprocessedTerm ConstructTemplatePreprocessor::preprocessTerm(
    const GraphTerm& term, PositionInTriple role,
    const VariableToColumnMap& variableColumns) {
  return std::visit(
      [&role, &variableColumns](const auto& t) -> PreprocessedTerm {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, Iri>) {
          return PrecomputedConstant{ConstructQueryEvaluator::evaluate(t)};
        } else if constexpr (std::is_same_v<T, Literal>) {
          auto value = ConstructQueryEvaluator::evaluate(t, role);
          return PrecomputedConstant{value.value_or("")};
        } else if constexpr (std::is_same_v<T, Variable>) {
          std::optional<size_t> columnIndex;
          if (auto opt = ad_utility::findOptional(variableColumns, t)) {
            columnIndex = opt->columnIndex_;
          }
          return PrecomputedVariable{columnIndex};
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
  result.preprocessedTriples_.resize(templateTriples.size());

  ad_utility::HashSet<size_t> uniqueColumnsSet;

  for (auto&& [triple, preprocessedTriple] :
       ::ranges::views::zip(templateTriples, result.preprocessedTriples_)) {
    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);
      auto preprocessed = preprocessTerm(triple[pos], role, variableColumns);
      if (auto* var = std::get_if<PrecomputedVariable>(&preprocessed)) {
        if (var->columnIndex_.has_value()) {
          uniqueColumnsSet.insert(*var->columnIndex_);
        }
      }
      preprocessedTriple[pos] = std::move(preprocessed);
    }
  }

  result.uniqueVariableColumns_.assign(uniqueColumnsSet.begin(),
                                       uniqueColumnsSet.end());
  return result;
}

}  // namespace qlever::constructExport
