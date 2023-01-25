// Copyright 2015 - 2022, University of Freiburg
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
  // probably are for historical reasons. I already added a `getLocalVocab`
  // method.

  // The actual entries.
  IdTable _idTable;

  // The entries in `_resultTypes` used to be significant in an old version of
  // the QLever code, but they longer are (because reality is more complicated
  // than "one type per column). It is still important for the correctness of
  // the code, however, that this vector has the same size as the number of
  // columns of the table.
  //
  // TODO: Properly keep track of result types again. In particular, efficiency
  // should benefit in the common use case where all entries in a column have a
  // certain type or types.
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

  // Set local vocabulary. This only copies a shared pointer. In order to make a
  // deep copy, use LocalVocab::clone(); see `GroupBy.cpp` for an example.
  void setLocalVocab(const std::shared_ptr<LocalVocab>& localVocab) {
    localVocab_ = localVocab;
  }

  // Get the local vocabulary of this result (not `const` because we may want to
  // augment it while constructing the result).
  std::shared_ptr<LocalVocab> getLocalVocab() const { return localVocab_; }

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
