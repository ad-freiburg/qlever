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

using PreprocessedConstructTemplate =
    qlever::constructExport::PreprocessedConstructTemplate;
using PreprocessedTerm = qlever::constructExport::PreprocessedTerm;
using PrecomputedConstant = qlever::constructExport::PrecomputedConstant;
using PrecomputedVariable = qlever::constructExport::PrecomputedVariable;
using PrecomputedBlankNode = qlever::constructExport::PrecomputedBlankNode;
using qlever::constructExport::NUM_TRIPLE_POSITIONS;

// _____________________________________________________________________________
PreprocessedConstructTemplate ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples,
    const VariableToColumnMap& variableColumns) {
  PreprocessedConstructTemplate result;
  result.preprocessedTriples_.resize(templateTriples.size());

  // unique set of variables contained in the construct template. Each variable
  // is identified by its column idx into the `IdTable`.
  ad_utility::HashSet<size_t> uniqueVariableColumnSet;

  for (size_t tripleIdx = 0; tripleIdx < templateTriples.size(); ++tripleIdx) {
    const auto& triple = templateTriples[tripleIdx];

    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);

      result.preprocessedTriples_[tripleIdx][pos] = std::visit(
          [&role, &variableColumns,
           &uniqueVariableColumnSet](const auto& term) -> PreprocessedTerm {
            using T = std::decay_t<decltype(term)>;
            if constexpr (std::is_same_v<T, Iri>) {
              return PrecomputedConstant{
                  ConstructQueryEvaluator::evaluate(term)};

            } else if constexpr (std::is_same_v<T, Literal>) {
              auto value = ConstructQueryEvaluator::evaluate(term, role);
              return PrecomputedConstant{value.value_or("")};

            } else if constexpr (std::is_same_v<T, Variable>) {
              std::optional<size_t> columnIndex;
              if (auto opt = ad_utility::findOptionalFromHashMap(
                      variableColumns, term)) {
                columnIndex = opt->columnIndex_;
                uniqueVariableColumnSet.insert(*columnIndex);
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

  result.uniqueVariableColumns_.assign(uniqueVariableColumnSet.begin(),
                                       uniqueVariableColumnSet.end());
  return result;
}
