//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef JOINHELPERS_H
#define JOINHELPERS_H

#include <array>
#include <optional>
#include <vector>

#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "index/CompressedRelation.h"
#include "index/Permutation.h"
#include "util/Exception.h"
#include "util/Generators.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"
#include "util/TypeTraits.h"

namespace qlever::joinHelpers {

static constexpr size_t CHUNK_SIZE = 100'000;

using namespace ad_utility;

using OptionalPermutation = std::optional<std::vector<ColumnIndex>>;

// _____________________________________________________________________________
inline void applyPermutation(IdTable& idTable,
                             const OptionalPermutation& permutation) {
  if (permutation.has_value()) {
    idTable.setColumnSubset(permutation.value());
  }
}

using LazyInputView = InputRangeTypeErased<IdTableAndFirstCol<IdTable>>;

// Convert a `generator<IdTableVocab>` to a `generator<IdTableAndFirstCol>` for
// more efficient access in the join columns below and apply the given
// permutation to each table.
CPP_template(typename Input)(
    requires SameAsAny<Input, Result::Generator, Result::LazyResult>)
    LazyInputView
    convertGenerator(Input gen, OptionalPermutation permutation = {}) {
  auto transformer = [permutation = std::move(permutation)](auto& element) {
    auto& [table, localVocab] = element;
    applyPermutation(table, permutation);
    // Make sure to actually move the table into the wrapper so that the tables
    // live as long as the wrapper.
    return IdTableAndFirstCol{std::move(table), std::move(localVocab)};
  };
  return InputRangeTypeErased{
      CachingTransformInputRange(std::move(gen), std::move(transformer))};
}

using MaterializedInputView = std::array<IdTableAndFirstCol<IdTableView<0>>, 1>;

// Wrap a fully materialized result in a `IdTableAndFirstCol` and an array. It
// then fulfills the concept `view<IdTableAndFirstCol>` which is required by the
// lazy join algorithms. Note: The `convertGenerator` function above
// conceptually does exactly the same for lazy inputs.
inline MaterializedInputView asSingleTableView(
    const Result& result, const std::vector<ColumnIndex>& permutation) {
  return std::array{
      IdTableAndFirstCol{result.idTable().asColumnSubsetView(permutation),
                         result.getCopyOfLocalVocab()}};
}

// Wrap a result either in an array with a single element or in a range wrapping
// the lazy result generator. Note that the lifetime of the view is coupled to
// the lifetime of the result.
inline std::variant<LazyInputView, MaterializedInputView> resultToView(
    const Result& result, const std::vector<ColumnIndex>& permutation) {
  if (result.isFullyMaterialized()) {
    return asSingleTableView(result, permutation);
  }
  return convertGenerator(result.idTables(), permutation);
}

// Type alias for the general InputRangeTypeErasedWithDetails with specific
// types
using GeneratorWithDetails =
    InputRangeTypeErasedWithDetails<IdTableAndFirstCol<IdTable>,
                                    CompressedRelationReader::LazyScanMetadata>;

// Convert a `generator<IdTable` to a `generator<IdTableAndFirstCol>` for more
// efficient access in the join columns below.
inline GeneratorWithDetails convertGenerator(
    Permutation::IdTableGenerator gen) {
  // Store the generator in a wrapper so we can access its details after moving
  auto generatorStorage =
      std::make_shared<Permutation::IdTableGenerator>(std::move(gen));

  // Create the range with a pointer to the generator's details
  auto range = InputRangeTypeErased<IdTableAndFirstCol<IdTable>>(
      CachingTransformInputRange(
          *generatorStorage, [generatorStorage](auto& table) {
            (void)generatorStorage;  // Only captured for lifetime reasons.
            // IndexScans don't have a local vocabulary, so we can just use an
            return IdTableAndFirstCol{std::move(table), LocalVocab{}};
          }));

  return GeneratorWithDetails{std::move(range), generatorStorage->details()};
}

// Part of the implementation of `createResult`. This function is called when
// the result should be yielded lazily.
// Action is a lambda that itself runs the join operation in a blocking
// manner. It is passed a special function that is supposed to be the callback
// being passed to the `AddCombinedRowToIdTable` so that the partial results
// can be yielded during execution. This is achieved by spawning a separate
// thread.
CPP_template_2(typename ActionT)(
    requires ad_utility::InvocableWithExactReturnType<
        ActionT, Result::IdTableVocabPair,
        std::function<void(IdTable&, LocalVocab&)>>) Result::LazyResult
    runLazyJoinAndConvertToGenerator(ActionT runLazyJoin,
                                     OptionalPermutation permutation) {
  return generatorFromActionWithCallback<Result::IdTableVocabPair>(
      [runLazyJoin = std::move(runLazyJoin),
       permutation = std::move(permutation)](
          std::function<void(Result::IdTableVocabPair)> callback) {
        auto yieldValue = [&permutation,
                           &callback](Result::IdTableVocabPair value) {
          if (value.idTable_.empty()) {
            return;
          }
          applyPermutation(value.idTable_, permutation);
          callback(std::move(value));
        };

        // The lazy join implementation calls its callback for each but the last
        // block. The last block (which also is the only block in case the
        // result is to be fully materialized)
        auto lastBlock = runLazyJoin(
            [&yieldValue](IdTable& idTable, LocalVocab& localVocab) {
              if (idTable.size() < CHUNK_SIZE) {
                return;
              }
              yieldValue({std::move(idTable), std::move(localVocab)});
            });
        yieldValue(std::move(lastBlock));
      });
}

// Helper function to check if the join of two columns propagate the value
// returned by `Operation::columnOriginatesFromGraphOrUndef`.
inline bool doesJoinProduceGuaranteedGraphValuesOrUndef(
    const std::shared_ptr<QueryExecutionTree>& left,
    const std::shared_ptr<QueryExecutionTree>& right,
    const Variable& variable) {
  auto graphOrUndef = [&variable](const auto& tree) {
    return tree->getRootOperation()->columnOriginatesFromGraphOrUndef(variable);
  };
  auto hasUndef = [&variable](const auto& tree) {
    return tree->getVariableColumns().at(variable).mightContainUndef_ !=
           ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined;
  };
  bool leftInGraph = graphOrUndef(left);
  bool rightInGraph = graphOrUndef(right);
  bool leftUndef = hasUndef(left);
  bool rightUndef = hasUndef(right);
  return (leftInGraph && rightInGraph) || (leftInGraph && !leftUndef) ||
         (rightInGraph && !rightUndef);
}

// Helper function to check if any of the join columns could potentially contain
// undef values.
inline bool joinColumnsAreAlwaysDefined(
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    const std::shared_ptr<QueryExecutionTree>& left,
    const std::shared_ptr<QueryExecutionTree>& right) {
  auto alwaysDefHelper = [](const auto& tree, ColumnIndex index) {
    return tree->getVariableAndInfoByColumnIndex(index)
               .second.mightContainUndef_ ==
           ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined;
  };
  return ql::ranges::all_of(
      joinColumns, [alwaysDefHelper = std::move(alwaysDefHelper), &left,
                    &right](const auto& indices) {
        return alwaysDefHelper(left, indices[0]) &&
               alwaysDefHelper(right, indices[1]);
      });
}

// Helper function that is commonly used to skip sort operations and use an
// alternative algorithm that doesn't require sorting instead.
inline std::shared_ptr<const Result> computeResultSkipChild(
    const std::shared_ptr<Operation>& operation) {
  auto children = operation->getChildren();
  AD_CONTRACT_CHECK(children.size() == 1);
  auto child = children.at(0);
  auto runtimeInfoChildren = child->getRootOperation()->getRuntimeInfoPointer();
  operation->updateRuntimeInformationWhenOptimizedOut({runtimeInfoChildren});

  return child->getResult(true);
}
}  // namespace qlever::joinHelpers

#endif  // JOINHELPERS_H
