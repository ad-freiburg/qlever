// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_RESULT_H
#define QLEVER_SRC_ENGINE_RESULT_H

#include <ranges>
#include <variant>
#include <vector>

#include "backports/span.h"
#include "engine/LocalVocab.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/InputRangeUtils.h"

// The result of an `Operation`. This is the class QLever uses for all
// intermediate or final results when processing a SPARQL query. The actual data
// is either a table and contained in the member `idTable()` or can be consumed
// through a generator via `idTables()` when it is supposed to be lazily
// evaluated.
class Result {
 public:
  struct IdTableVocabPair {
    IdTable idTable_;
    LocalVocab localVocab_;

    // Explicit constructor to avoid problems with coroutines and temporaries.
    // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for details.
    IdTableVocabPair(IdTable idTable, LocalVocab localVocab)
        : idTable_{std::move(idTable)}, localVocab_{std::move(localVocab)} {}
  };

  // The current implementation of (most of the) lazy results. Will be replaced
  // in the future to make QLever compatible with C++17 again.
  using Generator = cppcoro::generator<IdTableVocabPair>;
  // The lazy result type that is actually stored. It is type-erased and allows
  // explicit conversion from the `Generator` above.
  using LazyResult = ad_utility::InputRangeTypeErased<IdTableVocabPair>;

  // A commonly used LoopControl type for CachingContinuableTransformInputRange
  // generators
  using IdTableLoopControl =
      ad_utility::loopControl::LoopControl<IdTableVocabPair>;

 private:
  // Needs to be mutable in order to be consumable from a const result.
  struct GenContainer {
    mutable LazyResult generator_;
    mutable std::unique_ptr<std::atomic_bool> consumed_ =
        std::make_unique<std::atomic_bool>(false);
    explicit GenContainer(LazyResult generator)
        : generator_{std::move(generator)} {}
    CPP_template(typename Range)(
        requires std::is_constructible_v<
            LazyResult, Range>) explicit GenContainer(Range range)
        : generator_{LazyResult{std::move(range)}} {}
  };

  using LocalVocabPtr = std::shared_ptr<const LocalVocab>;

  struct IdTableSharedLocalVocabPair {
    IdTable idTable_;
    // The local vocabulary of the result.
    LocalVocabPtr localVocab_;
  };
  using Data = std::variant<IdTableSharedLocalVocabPair, GenContainer>;

  // The actual entries.
  Data data_;

  // The column indices by which the result is sorted (primary sort key first).
  // Empty if the result is not sorted on any column.
  std::vector<ColumnIndex> sortedBy_;

  // Note: If additional members and invariants are added to the class (for
  // example information about the datatypes in each column) make sure that
  // 1. The members and invariants remain valid after calling non-const function
  // like `applyLimitOffset`.
  // 2. The generator returned by the `idTables()`method for lazy operations is
  // valid even after the `Result` object from which it was obtained is
  // destroyed.

  // This class is used to enforce the invariant, that the `localVocab_` (which
  // is stored in a shared_ptr) is only shared between instances of the
  // `Result` class (where it is `const`). This gives a provable guarantee
  // that the `localVocab_` is not mutated through some other code that still
  // owns a pointer to the same local vocab.
  class SharedLocalVocabWrapper {
   private:
    // Only the `Result` class is allowed to read or write the stored
    // `shared_ptr`. Other code can obtain a `SharedLocalVocabWrapper` from a
    // `Result` and pass this wrapper into another `Result`, but it
    // can never access the `shared_ptr` directly.
    std::shared_ptr<const LocalVocab> localVocab_;
    explicit SharedLocalVocabWrapper(LocalVocabPtr localVocab)
        : localVocab_{std::move(localVocab)} {}
    friend class Result;

   public:
    // Create a wrapper from a `LocalVocab`. This is safe to call also from
    // external code, as the local vocab is passed by value and not by (shared)
    // pointer, so it is exclusive to this wrapper.
    explicit SharedLocalVocabWrapper(LocalVocab localVocab)
        : localVocab_{
              std::make_shared<const LocalVocab>(std::move(localVocab))} {}
  };

 public:
  // Construct from the given arguments (see above) and check the following
  // invariants: `localVocab` must not be `nullptr` and each entry of `sortedBy`
  // must be a valid column index for the `idTable`. The invariant that the
  // `idTable` is sorted by the columns specified by `sortedBy` is only checked,
  // if expensive checks are enabled, for example by not defining the `NDEBUG`
  // macro.
  // The first overload of the constructor is for local vocabs that are shared
  // with another `Result` via the `getSharedLocalVocab...` methods below.
  // The second overload is for newly created local vocabularies.
  Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
         SharedLocalVocabWrapper localVocab);
  Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
         LocalVocab&& localVocab);
  Result(IdTableVocabPair pair, std::vector<ColumnIndex> sortedBy);
  Result(Generator idTables, std::vector<ColumnIndex> sortedBy);
  Result(LazyResult idTables, std::vector<ColumnIndex> sortedBy);

  // Prevent accidental copying of a result table.
  Result(const Result& other) = delete;
  Result& operator=(const Result& other) = delete;

  // Moving of a result table is OK.
  Result(Result&& other) = default;
  Result& operator=(Result&& other) = default;

  // Wrap the generator stored in `data_` within a new generator that calls
  // `onNewChunk` every time a new `IdTableVocabPair` is yielded by the original
  // generator and passed this new `IdTableVocabPair` along with microsecond
  // precision timing information on how long it took to compute this new chunk.
  // `onGeneratorFinished` is guaranteed to be called eventually as long as the
  // generator is consumed at least partially, with `true` if an exception
  // occurred during consumption or with `false` when the generator is done
  // processing or abandoned and destroyed.
  //
  // Throw an `ad_utility::Exception` if the underlying `data_` member holds the
  // wrong variant.
  void runOnNewChunkComputed(
      std::function<void(const IdTableVocabPair&, std::chrono::microseconds)>
          onNewChunk,
      std::function<void(bool)> onGeneratorFinished);

  // Wrap the generator stored in `data_` within a new generator that aggregates
  // the entries yielded by the generator into a cacheable `IdTable`. Once
  // `fitInCache` returns false, thus indicating that both passed arguments
  // together would be too large to be cached, this cached value is discarded.
  // If this cached value still exists when the generator is fully consumed a
  // new `Result` is created with this value and passed to `storeInCache`.
  //
  // Throw an `ad_utility::Exception` if the underlying `data_` member holds the
  // wrong variant.
  void cacheDuringConsumption(
      std::function<bool(const std::optional<IdTableVocabPair>&,
                         const IdTableVocabPair&)>
          fitInCache,
      std::function<void(Result)> storeInCache);

  // Const access to the underlying `IdTable`. Throw an `ad_utility::Exception`
  // if the underlying `data_` member holds the wrong variant.
  const IdTable& idTable() const;

  // Access to the underlying `IdTable`s. Throw an `ad_utility::Exception`
  // if the underlying `data_` member holds the wrong variant or if the result
  // has previously already been retrieved.
  // Note: The returned `LazyResult` is not coupled to the `Result` object, and
  // thus can be used even after the `Result` object from which it was obtained
  // was destroyed.
  LazyResult idTables() const;

  // Const access to the columns by which the `idTable()` is sorted.
  const std::vector<ColumnIndex>& sortedBy() const { return sortedBy_; }

  // Get the local vocabulary of this result, used for lookup only.
  //
  // NOTE: This is currently used in the following methods (in parentheses
  // the name of the function called with the local vocab as argument):
  //
  // ExportQueryExecutionTrees::idTableToQLeverJSONArray (idToStringAndType)
  // ExportQueryExecutionTrees::selectQueryResultToSparqlJSON (ditto)
  // ExportQueryExecutionTrees::selectQueryResultToStream (ditto)
  // Filter::computeFilterImpl (evaluationContext)
  // Variable::evaluate (idToStringAndType)
  //
  const LocalVocab& localVocab() const {
    AD_CONTRACT_CHECK(isFullyMaterialized());
    return *std::get<IdTableSharedLocalVocabPair>(data_).localVocab_;
  }

  // Get the local vocab as a shared pointer to const. This can be used if one
  // result has the same local vocab as one of its child results.
  SharedLocalVocabWrapper getSharedLocalVocab() const {
    AD_CONTRACT_CHECK(isFullyMaterialized());
    return SharedLocalVocabWrapper{
        std::get<IdTableSharedLocalVocabPair>(data_).localVocab_};
  }

  // Like `getSharedLocalVocabFrom`, but takes more than one result and merges
  // all the corresponding local vocabs.
  static SharedLocalVocabWrapper getMergedLocalVocab(const Result& result1,
                                                     const Result& result2);

  // Overload for more than two `Results`
  CPP_template(typename R)(
      requires ql::ranges::forward_range<R> CPP_and
          std::convertible_to<ql::ranges::range_value_t<R>,
                              const Result&>) static SharedLocalVocabWrapper
      getMergedLocalVocab(R&& subResults) {
    std::vector<const LocalVocab*> vocabs;
    for (const Result& table : subResults) {
      vocabs.push_back(&table.localVocab());
    }
    return SharedLocalVocabWrapper{LocalVocab::merge(vocabs)};
  }

  // Get a (deep) copy of the local vocabulary from the given result. Use this
  // when you want to (potentially) add further words to the local vocabulary
  // (which is not possible with `shareLocalVocabFrom`).
  LocalVocab getCopyOfLocalVocab() const;

  // Return true if `data_` holds an `IdTable`, false otherwise.
  bool isFullyMaterialized() const noexcept;

  // Log the size of this result. We call this at several places in
  // `Server::processQuery`. Ideally, this should only be called in one
  // place, but for now, this method at least makes sure that these log
  // messages look all the same.
  void logResultSize() const;

  // The first rows of the result and its total size (for debugging).
  string asDebugString() const;

  // Apply the `limitOffset` clause by shifting and then resizing the `IdTable`.
  // This also applies if `data_` holds a generator yielding `IdTable`s, where
  // this is applied respectively.
  // `limitTimeCallback` is called whenever an `IdTable` is resized with the
  // number of microseconds it took to perform this operation and the freshly
  // resized `IdTable` as const reference.
  // Note: If  additional members and invariants are added to the class (for
  // example information about the datatypes in each column) make sure that
  // those are still correct after performing this operation.
  void applyLimitOffset(
      const LimitOffsetClause& limitOffset,
      std::function<void(std::chrono::microseconds, const IdTable&)>
          limitTimeCallback);

  // Check if the operation did fulfill its contract and only returns as many
  // elements as requested by the provided `limitOffset`. Throw an
  // `ad_utility::Exception` otherwise. When `data_` holds a generator, this
  // behaviour applies analogously when consuming the generator.
  // This member function provides an alternative to `applyLimitOffset` that
  // resizes the result if the operation doesn't support this on its own.
  void assertThatLimitWasRespected(const LimitOffsetClause& limitOffset);

  // Check that if the `varColMap` guarantees that a column is always defined
  // (i.e. that is contains no single undefined value) that there are indeed no
  // undefined values in the `data_` of this result. Do nothing iff the check is
  // successful. Throw an `ad_utility::Exception` otherwise. When `data_` holds
  // a generator, this behaviour applies analogously when consuming the
  // generator.
  void checkDefinedness(const VariableToColumnMap& varColMap);
};

#endif  // QLEVER_SRC_ENGINE_RESULT_H
