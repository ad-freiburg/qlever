//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef JOINHELPERS_H
#define JOINHELPERS_H

#include <array>
#include <optional>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "util/Generators.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"
#include "util/TypeTraits.h"

namespace qlever::joinHelpers {

static constexpr size_t CHUNK_SIZE = 100'000;

using OptionalPermutation = std::optional<std::vector<ColumnIndex>>;

// _____________________________________________________________________________
inline void applyPermutation(IdTable& idTable,
                             const OptionalPermutation& permutation) {
  if (permutation.has_value()) {
    idTable.setColumnSubset(permutation.value());
  }
}

using LazyInputView =
    cppcoro::generator<ad_utility::IdTableAndFirstCol<IdTable>>;

// Convert a `generator<IdTableVocab>` to a `generator<IdTableAndFirstCol>` for
// more efficient access in the join columns below and apply the given
// permutation to each table.
CPP_template(typename Input)(
    requires ad_utility::SameAsAny<Input, Result::Generator,
                                   Result::LazyResult>) LazyInputView
    convertGenerator(Input gen, OptionalPermutation permutation = {}) {
  for (auto& [table, localVocab] : gen) {
    applyPermutation(table, permutation);
    // Make sure to actually move the table into the wrapper so that the tables
    // live as long as the wrapper.
    ad_utility::IdTableAndFirstCol t{std::move(table), std::move(localVocab)};
    co_yield t;
  }
}

using MaterializedInputView =
    std::array<ad_utility::IdTableAndFirstCol<IdTableView<0>>, 1>;

// Wrap a fully materialized result in a `IdTableAndFirstCol` and an array. It
// then fulfills the concept `view<IdTableAndFirstCol>` which is required by the
// lazy join algorithms. Note: The `convertGenerator` function above
// conceptually does exactly the same for lazy inputs.
inline MaterializedInputView asSingleTableView(
    const Result& result, const std::vector<ColumnIndex>& permutation) {
  return std::array{ad_utility::IdTableAndFirstCol{
      result.idTable().asColumnSubsetView(permutation),
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
  return convertGenerator(std::move(result.idTables()), permutation);
}

using GeneratorWithDetails =
    cppcoro::generator<ad_utility::IdTableAndFirstCol<IdTable>,
                       CompressedRelationReader::LazyScanMetadata>;

// Convert a `generator<IdTable` to a `generator<IdTableAndFirstCol>` for more
// efficient access in the join columns below.
inline GeneratorWithDetails convertGenerator(
    Permutation::IdTableGenerator gen) {
  co_await cppcoro::getDetails = gen.details();
  gen.setDetailsPointer(&co_await cppcoro::getDetails);
  for (auto& table : gen) {
    // IndexScans don't have a local vocabulary, so we can just use an empty one
    ad_utility::IdTableAndFirstCol t{std::move(table), LocalVocab{}};
    co_yield t;
  }
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
        std::function<void(IdTable&, LocalVocab&)>>) Result::Generator
    runLazyJoinAndConvertToGenerator(ActionT runLazyJoin,
                                     OptionalPermutation permutation) {
  return ad_utility::generatorFromActionWithCallback<Result::IdTableVocabPair>(
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
}  // namespace qlever::joinHelpers

#endif  // JOINHELPERS_H
