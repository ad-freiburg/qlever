// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <vector>

#include "engine/LocalVocab.h"
#include "engine/ResultType.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"

using std::array;
using std::condition_variable;
using std::lock_guard;
using std::mutex;
using std::unique_lock;
using std::vector;

// The result of an `Operation`. This is the class QLever uses for all
// intermediate or final results when processing a SPARQL query. The actual data
// is always a table and contained in the member `_idTable`.
//
// TODO: I would find it more appropriate to simply call this class `Result`.
// Otherwise, it's not clear from the names what the difference between a
// `ResultTable` and an `IdTable` is.
class ResultTable {
 public:
  // The status of the result.
  enum Status { IN_PROGRESS = 0, FINISHED = 1, ABORTED = 2 };

 public:
  // TODO: I think that none of these member variables should be public. They
  // probably are for historical reasons. I already added a `localVocab`
  // method.

  // The actual entries.
  IdTable _idTable;

  // The semantics of `_resultTypes` is not not used anymore in the QLever code,
  // see the comment in `ResultType.h`. We still need the declaration though
  // because it is checked, at various places, that this vector has the same
  // size as the number of columns of the table.
  //
  // TODO: Refactor this code according to the comment in `ResultType.h`.
  using ResultType = qlever::ResultType;
  vector<ResultType> _resultTypes;

  // The column indices by which the result is sorted (primary sort key first).
  // Empty if the result is not sorted on any column.
  vector<size_t> _sortedBy;

 private:
  // The local vocabulary of the result.
  std::shared_ptr<LocalVocab> localVocab_ = std::make_shared<LocalVocab>();

 public:
  // Construct with given allocator.
  explicit ResultTable(ad_utility::AllocatorWithLimit<Id> allocator)
      : _idTable(std::move(allocator)) {}

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

  // Share the local vocabulary with the given other result. After this is
  // called, the (shared) local vocabulary is set to read only (because adding
  // to it further would affect earlier results, which might be cached, which
  // can give all sort of unexpected behavior or even wrong results).
  //
  // NOTE: In order to make a deep copy, use LocalVocab::clone(); see
  // `GroupBy.cpp` for an example.
  void shareLocalVocabFrom(const ResultTable& resultTable);

  // Like `shareLocalVocabFrom`, but takes *two* results and assumes that one of
  // their local vocabularies is empty and shares the the result with the
  // non-empty one (if both are empty, arbitrarily share with the first one).
  //
  // TODO: Eventually, we want to be able to merge two non-empty local
  // vocabularies, but that requires more work since we have to rewrite IDs then
  // (from the previous separate local vocabularies to the new merged one).
  void shareLocalVocabFromNonEmptyOf(const ResultTable& resultTable1,
                                     const ResultTable& resultTable2);

  // Get a (deep) copy of the local vocabulary from the given result. Use this
  // when you want to (potentially) add further words to the local vocabulary
  // (which is not possible with `shareLocalVocabFrom`).
  void getCopyOfLocalVocabFrom(const ResultTable& resultTable);

  // Get the local vocabulary of this result, used for lookup only.
  //
  // NOTE: This is currently used in the following methods (in parentheses
  // the name of the function called with the local vocab as argument):
  //
  // ExportQueryExecutionTrees::idTableToQLeverJSONArray (idToStringAndType)
  // ExportQueryExecutionTrees::selectQueryResultToSparqlJSON (dito)
  // ExportQueryExecutionTrees::selectQueryResultToCsvTsvOrBinary (dito)
  // Filter::computeFilterImpl (evaluationContext)
  // Variable::evaluate (idToStringAndType)
  //
  const LocalVocab& localVocab() const { return *localVocab_; }

  // The non-const version of the above.
  //
  // NOTE: This is currently used in the following methods:
  //
  // Values::writeValues (toValueId)
  // Bind::computeExpressionBind (evaluationContext)
  // Bind::computeExpressionBind (constantExpressionResultToId)
  // GroupBy::processGroup (constantExpressionResultToId)
  // GroupBy::doGroupBy (evaluationContext)
  // GroupByTest::doGroupBy [that could be done differently there]
  //
  // TODO: I added the `NonConst` to the name to emphasize that this is not the
  // ideal interface. But good enough for now.
  LocalVocab& localVocabNonConst() { return *localVocab_; }

  // Log the size of this result. We call this at several places in
  // `Server::processQuery`. Ideally, this should only be called in one
  // place, but for now, this method at least makes sure that these log
  // messages look all the same.
  void logResultSize() const {
    LOG(INFO) << "Result has size " << size() << " x " << width() << std::endl;
  }

  // The first rows of the result and its total size (for debugging).
  string asDebugString() const;

  // Get the result type for the given column if provided (default:
  // ResultType::KB).
  ResultType getResultType(size_t col) const {
    return col < _resultTypes.size() ? _resultTypes[col] : ResultType::KB;
  }
};
