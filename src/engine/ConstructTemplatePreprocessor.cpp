// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTemplatePreprocessor.h"

#include <absl/strings/str_cat.h>

#include "engine/ConstructQueryEvaluator.h"
#include "util/HashMap.h"
#include "util/TypeTraits.h"

// _____________________________________________________________________________
std::vector<PreprocessedTriple> ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples,
    const VariableToColumnMap& variableColumns) {
  std::vector<PreprocessedTriple> result(templateTriples.size());

  for (size_t tripleIdx = 0; tripleIdx < templateTriples.size(); ++tripleIdx) {
    const auto& triple = templateTriples[tripleIdx];

    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);

      result[tripleIdx][pos] = std::visit(
          [&role, &variableColumns](const auto& term) -> PreprocessedTerm {
            using T = std::decay_t<decltype(term)>;

            if constexpr (std::is_same_v<T, Iri>) {
              return PrecomputedConstant{
                  ConstructQueryEvaluator::evaluate(term)};
            } else if constexpr (std::is_same_v<T, Literal>) {
              auto value = ConstructQueryEvaluator::evaluate(term, role);
              return PrecomputedConstant{value.value_or("")};
            } else if constexpr (std::is_same_v<T, Variable>) {
              std::optional<size_t> columnIndex;
              if (auto opt = ad_utility::getOptionalFromHashMap(variableColumns,
                                                                term)) {
                columnIndex = opt->columnIndex_;
              }
              return PrecomputedVariable{columnIndex};
            } else if constexpr (std::is_same_v<T, BlankNode>) {
              return PrecomputedBlankNode{term.isGenerated() ? "_:g" : "_:u",
                                          absl::StrCat("_", term.label())};
            } else {
              static_assert(ad_utility::alwaysFalse<T>);
            }
          },
          triple[pos]);
    }
  }

  return result;
}
