// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <vector>

#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
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
  std::vector<size_t> _sortedBy;

  // The local vocabulary of the result.
  std::shared_ptr<const LocalVocab> localVocab_ =
      std::make_shared<const LocalVocab>();

 public:
  // Construct from the given arguments (see above) and check the following
  // invariants: `localVocab` must not be `nullptr` and each entry of `sortedBy`
  // must be a valid column index for the `idTable`. The invariant that the
  // `idTable` is sorted by the columns specified by `sortedBy` is only checked,
  // if expensive checks are enabled, for example by not defining the `NDEBUG`
  // macro.
  ResultTable(IdTable idTable, std::vector<size_t> sortedBy,
              std::shared_ptr<const LocalVocab> localVocab);

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
  const std::vector<size_t>& sortedBy() const { return _sortedBy; }

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

  // Get the local vocab as a shared pointer to const. This can be used if one
  // result has the same local vocab as one of its child results.
  std::shared_ptr<const LocalVocab> getSharedLocalVocab() const {
    return localVocab_;
  }

  // Like `getSharedLocalVocabFrom`, but takes *two* results and assumes that
  // one of the local vocabularies is empty and gets the shared local vocab from
  // the non-empty one (if both are empty, arbitrarily share with the first
  // one).
  //
  // TODO: Eventually, we want to be able to merge two non-empty local
  // vocabularies, but that requires more work since we have to rewrite IDs then
  // (from the previous separate local vocabularies to the new merged one).
  static std::shared_ptr<const LocalVocab> getSharedLocalVocabFromNonEmptyOf(
      const ResultTable& resultTable1, const ResultTable& resultTable2);

  // Get a (deep) copy of the local vocabulary from the given result. Use this
  // when you want to (potentially) add further words to the local vocabulary
  // (which is not possible with `shareLocalVocabFrom`).
  std::shared_ptr<LocalVocab> getCopyOfLocalVocab() const;

  // Log the size of this result. We call this at several places in
  // `Server::processQuery`. Ideally, this should only be called in one
  // place, but for now, this method at least makes sure that these log
  // messages look all the same.
  void logResultSize() const {
    LOG(INFO) << "Result has size " << size() << " x " << width() << std::endl;
  }

  // The first rows of the result and its total size (for debugging).
  string asDebugString() const;
};
