// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <ranges>
#include <variant>
#include <vector>

#include "engine/LocalVocab.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CacheableGenerator.h"

template <typename IdTableType, typename GeneratorType>
class ResultStorage {
  friend class ProtoResult;
  friend class Result;

  using Data = std::variant<IdTableType, GeneratorType>;
  // The actual entries.
  Data data_;

  // The column indices by which the result is sorted (primary sort key first).
  // Empty if the result is not sorted on any column.
  std::vector<ColumnIndex> sortedBy_;

  // The local vocabulary of the result.
  std::shared_ptr<const LocalVocab> localVocab_ =
      std::make_shared<const LocalVocab>();

  ResultStorage(Data data, std::vector<ColumnIndex> sortedBy,
                std::shared_ptr<const LocalVocab> localVocab)
      : data_{std::move(data)},
        sortedBy_{std::move(sortedBy)},
        localVocab_{std::move(localVocab)} {}

  bool isDataEvaluated() const noexcept {
    return std::holds_alternative<IdTableType>(data_);
  }

  IdTableType& idTable() {
    AD_CONTRACT_CHECK(isDataEvaluated());
    return std::get<IdTableType>(data_);
  }

  const IdTableType& idTable() const {
    AD_CONTRACT_CHECK(isDataEvaluated());
    return std::get<IdTableType>(data_);
  }

  GeneratorType& idTables() {
    AD_CONTRACT_CHECK(!isDataEvaluated());
    return std::get<GeneratorType>(data_);
  }

  const GeneratorType& idTables() const {
    AD_CONTRACT_CHECK(!isDataEvaluated());
    return std::get<GeneratorType>(data_);
  }
};

class ProtoResult {
  friend class Result;
  using StorageType = ResultStorage<IdTable, cppcoro::generator<IdTable>>;
  StorageType storage_;

  using LocalVocabPtr = std::shared_ptr<const LocalVocab>;

  // Note: If additional members and invariants are added to the class (for
  // example information about the datatypes in each column) make sure that
  // those remain valid after calling non-const function like
  // `applyLimitOffset`.

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
    friend ProtoResult;
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
  ProtoResult(IdTable idTable, std::vector<ColumnIndex> sortedBy,
              SharedLocalVocabWrapper localVocab);
  ProtoResult(IdTable idTable, std::vector<ColumnIndex> sortedBy,
              LocalVocab&& localVocab);
  ProtoResult(cppcoro::generator<IdTable> idTables,
              std::vector<ColumnIndex> sortedBy,
              SharedLocalVocabWrapper localVocab);
  ProtoResult(cppcoro::generator<IdTable> idTables,
              std::vector<ColumnIndex> sortedBy, LocalVocab&& localVocab);

 public:
  ProtoResult(const ProtoResult& other) = delete;
  ProtoResult& operator=(const ProtoResult& other) = delete;

  ProtoResult(ProtoResult&& other) = default;
  ProtoResult& operator=(ProtoResult&& other) = default;

  // For each column in the result (the entries in the outer `vector`) and for
  // each `Datatype` (the entries of the inner `array`), store the information
  // how many entries of that datatype are stored in the column.
  using DatatypeCountsPerColumn = std::vector<
      std::array<size_t, static_cast<size_t>(Datatype::MaxValue) + 1>>;

  // Apply the `limitOffset` clause by shifting and then resizing the `IdTable`.
  // Note: If additional members and invariants are added to the class (for
  // example information about the datatypes in each column) make sure that
  // those are still correct after performing this operation.
  void applyLimitOffset(
      const LimitOffsetClause& limitOffset,
      std::function<void(std::chrono::milliseconds)> limitTimeCallback);

  void enforceLimitOffset(const LimitOffsetClause& limitOffset);

  // Check that if the `varColMap` guarantees that a column is always defined
  // (i.e. that is contains no single undefined value) that there are indeed no
  // undefined values in the `data_` of this result. Return `true` iff the
  // check is successful.
  void checkDefinedness(const VariableToColumnMap& varColMap);

 private:
  // Get the information, which columns stores how many entries of each
  // datatype.
  static DatatypeCountsPerColumn computeDatatypeCountsPerColumn(
      IdTable& idTable);

  static void validateIdTable(const IdTable& idTable,
                              const std::vector<ColumnIndex>& sortedBy);

 public:
  const IdTable& idTable() const;

  bool isDataEvaluated() const noexcept;
};

// The result of an `Operation`. This is the class QLever uses for all
// intermediate or final results when processing a SPARQL query. The actual data
// is always a table and contained in the member `idTable()`.
class Result {
 private:
  using StorageType = ResultStorage<IdTable, cppcoro::generator<IdTable>>;
  mutable StorageType storage_;

  using LocalVocabPtr = std::shared_ptr<const LocalVocab>;

  using SharedLocalVocabWrapper = ProtoResult::SharedLocalVocabWrapper;

  Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
         LocalVocabPtr localVocab);
  Result(cppcoro::generator<IdTable> idTables,
         std::vector<ColumnIndex> sortedBy, LocalVocabPtr localVocab);

 public:
  // Prevent accidental copying of a result table.
  Result(const Result& other) = delete;
  Result& operator=(const Result& other) = delete;

  // Moving of a result table is OK.
  Result(Result&& other) = default;
  Result& operator=(Result&& other) = default;

  static Result fromProtoResult(ProtoResult protoResult,
                                std::function<bool(const IdTable&)> fitsInCache,
                                std::function<void(Result)> storeInCache);

  // Const access to the underlying `IdTable`.
  const IdTable& idTable() const;

  // Access to the underlying `IdTable`s.
  cppcoro::generator<IdTable>& idTables() const;

  // Const access to the columns by which the `idTable()` is sorted.
  const std::vector<ColumnIndex>& sortedBy() const {
    return storage_.sortedBy_;
  }

  // Get the local vocabulary of this result, used for lookup only.
  //
  // NOTE: This is currently used in the following methods (in parentheses
  // the name of the function called with the local vocab as argument):
  //
  // ExportQueryExecutionTrees::idTableToQLeverJSONArray (idToStringAndType)
  // ExportQueryExecutionTrees::selectQueryResultToSparqlJSON (dito)
  // ExportQueryExecutionTrees::selectQueryResultToStream (dito)
  // Filter::computeFilterImpl (evaluationContext)
  // Variable::evaluate (idToStringAndType)
  //
  const LocalVocab& localVocab() const { return *storage_.localVocab_; }

  // Get the local vocab as a shared pointer to const. This can be used if one
  // result has the same local vocab as one of its child results.
  SharedLocalVocabWrapper getSharedLocalVocab() const {
    return SharedLocalVocabWrapper{storage_.localVocab_};
  }

  // Like `getSharedLocalVocabFrom`, but takes more than one result and merges
  // all the corresponding local vocabs.
  static SharedLocalVocabWrapper getMergedLocalVocab(const Result& result1,
                                                     const Result& result2);

  // Overload for more than two `Results`
  template <std::ranges::forward_range R>
  requires std::convertible_to<std::ranges::range_value_t<R>, const Result&>
  static SharedLocalVocabWrapper getMergedLocalVocab(R&& subResults) {
    std::vector<const LocalVocab*> vocabs;
    for (const Result& table : subResults) {
      vocabs.push_back(std::to_address(table.storage_.localVocab_));
    }
    return SharedLocalVocabWrapper{LocalVocab::merge(vocabs)};
  }

  // Get a (deep) copy of the local vocabulary from the given result. Use this
  // when you want to (potentially) add further words to the local vocabulary
  // (which is not possible with `shareLocalVocabFrom`).
  LocalVocab getCopyOfLocalVocab() const;

  bool isDataEvaluated() const noexcept;

  // Log the size of this result. We call this at several places in
  // `Server::processQuery`. Ideally, this should only be called in one
  // place, but for now, this method at least makes sure that these log
  // messages look all the same.
  void logResultSize() const;

  // The first rows of the result and its total size (for debugging).
  string asDebugString() const;
};
