// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <ranges>
#include <vector>

#include "engine/LocalVocab.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/Log.h"

using std::vector;

// The result of an `Operation`. This is the class QLever uses for all
// intermediate or final results when processing a SPARQL query. The actual data
// is always a table and contained in the member `idTable()`.
//
// TODO: I would find it more appropriate to simply call this class `Result`.
// Otherwise, it's not clear from the names what the difference between a
// `ResultTable` and an `IdTable` is.
class ResultTable {
 private:
  // The actual entries.
  IdTable _idTable;

  // The column indices by which the result is sorted (primary sort key first).
  // Empty if the result is not sorted on any column.
  std::vector<ColumnIndex> _sortedBy;

  using LocalVocabPtr = std::shared_ptr<const LocalVocab>;
  // The local vocabulary of the result.
  LocalVocabPtr localVocab_ = std::make_shared<const LocalVocab>();

  // Note: If additional members and invariants are added to the class (for
  // example information about the datatypes in each column) make sure that
  // those remain valid after calling non-const function like
  // `applyLimitOffset`.

  // This class is used to enforce the invariant, that the `localVocab_` (which
  // is stored in a shared_ptr) is only shared between instances of the
  // `ResultTable` class (where it is `const`). This gives a provable guarantee
  // that the `localVocab_` is not mutated through some other code that still
  // owns a pointer to the same local vocab.
  class SharedLocalVocabWrapper {
   private:
    // Only the `ResultTable` class is allowed to read or write the stored
    // `shared_ptr`. Other code can obtain a `SharedLocalVocabWrapper` from a
    // `ResultTable` and pass this wrapper into another `ResultTable`, but it
    // can never access the `shared_ptr` directly.
    std::shared_ptr<const LocalVocab> localVocab_ =
        std::make_shared<const LocalVocab>();
    explicit SharedLocalVocabWrapper(LocalVocabPtr localVocab)
        : localVocab_{std::move(localVocab)} {}
    friend class ResultTable;

   public:
    // Create a wrapper from a `LocalVocab`. This is safe to call also from
    // external code, as the local vocab is passed by value and not by (shared)
    // pointer, so it is exclusive to this wrapper.
    explicit SharedLocalVocabWrapper(LocalVocab localVocab)
        : localVocab_{
              std::make_shared<const LocalVocab>(std::move(localVocab))} {}
  };

  // For each column in the result (the entries in the outer `vector`) and for
  // each `Datatype` (the entries of the inner `array`), store the information
  // how many entries of that datatype are stored in the column.
  using DatatypeCountsPerColumn = std::vector<
      std::array<size_t, static_cast<size_t>(Datatype::MaxValue) + 1>>;
  std::optional<DatatypeCountsPerColumn> datatypeCountsPerColumn_;

 public:
  // Construct from the given arguments (see above) and check the following
  // invariants: `localVocab` must not be `nullptr` and each entry of `sortedBy`
  // must be a valid column index for the `idTable`. The invariant that the
  // `idTable` is sorted by the columns specified by `sortedBy` is only checked,
  // if expensive checks are enabled, for example by not defining the `NDEBUG`
  // macro.
  // The first overload of the constructor is for local vocabs that are shared
  // with another `ResultTable` via the `getSharedLocalVocab...` methods below.
  // The second overload is for newly created local vocabularies.
  ResultTable(IdTable idTable, std::vector<ColumnIndex> sortedBy,
              SharedLocalVocabWrapper localVocab);
  ResultTable(IdTable idTable, std::vector<ColumnIndex> sortedBy,
              LocalVocab&& localVocab);

  // Prevent accidental copying of a result table.
  ResultTable(const ResultTable& other) = delete;
  ResultTable& operator=(const ResultTable& other) = delete;

  // Moving of a result table is OK.
  ResultTable(ResultTable&& other) = default;
  ResultTable& operator=(ResultTable&& other) = default;

  // Default destructor.
  virtual ~ResultTable() = default;

  // Get the number of rows of this result.
  size_t size() const { return _idTable.size(); }

  // Get the number of columns of this result.
  size_t width() const { return _idTable.numColumns(); }

  // Const access to the underlying `IdTable`.
  const IdTable& idTable() const { return _idTable; }

  // Const access to the columns by which the `idTable()` is sorted.
  const std::vector<ColumnIndex>& sortedBy() const { return _sortedBy; }

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
  const LocalVocab& localVocab() const { return *localVocab_; }

  // Get the local vocab as a shared pointer to const. This can be used if one
  // result has the same local vocab as one of its child results.
  SharedLocalVocabWrapper getSharedLocalVocab() const {
    return SharedLocalVocabWrapper{localVocab_};
  }

  // Like `getSharedLocalVocabFrom`, but takes more than one result and assumes
  // that exactly one of the local vocabularies is empty and gets the shared
  // local vocab from the non-empty one (if all are empty, arbitrarily share
  // with the first one).
  //
  // TODO: Eventually, we want to be able to merge two non-empty local
  // vocabularies, but that requires more work since we have to rewrite IDs then
  // (from the previous separate local vocabularies to the new merged one).
  static SharedLocalVocabWrapper getSharedLocalVocabFromNonEmptyOf(
      const ResultTable& resultTable1, const ResultTable& resultTable2);

  // Overload for more than two `ResultTables`
  template <std::ranges::forward_range R>
  requires std::convertible_to<std::ranges::range_value_t<R>,
                               const ResultTable&>
  static SharedLocalVocabWrapper getSharedLocalVocabFromNonEmptyOf(
      R&& subResults) {
    AD_CONTRACT_CHECK(!std::ranges::empty(subResults));
    auto hasNonEmptyVocab = [](const ResultTable& tbl) {
      return !tbl.localVocab_->empty();
    };
    auto numNonEmptyVocabs =
        std::ranges::count_if(subResults, hasNonEmptyVocab);
    if (numNonEmptyVocabs > 1) {
      throw std::runtime_error(
          "Merging of more than one non-empty local vocabularies is currently "
          "not supported, please contact the developers");
    }
    // The static casts in the following are needed to make this code work for
    // types that are implicitly convertible to `const ResultTable&`, in
    // particular `std::reference_wrapper<const ResultTable>`.
    if (numNonEmptyVocabs == 0) {
      return SharedLocalVocabWrapper{
          static_cast<const ResultTable&>(*subResults.begin()).localVocab_};
    } else {
      return SharedLocalVocabWrapper{
          static_cast<const ResultTable&>(
              *std::ranges::find_if(subResults, hasNonEmptyVocab))
              .localVocab_};
    }
  }

  // Get a (deep) copy of the local vocabulary from the given result. Use this
  // when you want to (potentially) add further words to the local vocabulary
  // (which is not possible with `shareLocalVocabFrom`).
  LocalVocab getCopyOfLocalVocab() const;

  // Log the size of this result. We call this at several places in
  // `Server::processQuery`. Ideally, this should only be called in one
  // place, but for now, this method at least makes sure that these log
  // messages look all the same.
  void logResultSize() const {
    LOG(INFO) << "Result has size " << size() << " x " << width() << std::endl;
  }

  // The first rows of the result and its total size (for debugging).
  string asDebugString() const;

  // Apply the `limitOffset` clause by shifting and then resizing the `IdTable`.
  // Note: If additional members and invariants are added to the class (for
  // example information about the datatypes in each column) make sure that
  // those are still correct after performing this operation.
  void applyLimitOffset(const LimitOffsetClause& limitOffset);

  // Get the information, which columns stores how many entries of each
  // datatype. This information is computed on the first call to this function
  // `O(num-entries-in-table)` and then cached for subsequent usages.
  const DatatypeCountsPerColumn& getOrComputeDatatypeCountsPerColumn();

  // Check that if the `varColMap` guarantees that a column is always defined
  // (i.e. that is contains no single undefined value) that there are indeed no
  // undefined values in the `_idTable` of this result. Return `true` iff the
  // check is succesful.
  bool checkDefinedness(const VariableToColumnMap& varColMap);
};
