// Copyright 2015 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
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

class ResultTable {
 public:
  enum Status { IN_PROGRESS = 0, FINISHED = 1, ABORTED = 2 };
  using ResultType = qlever::ResultType;

  /**
   * @brief This vector contains a list of column indices by which the result
   *        is sorted. This vector may be empty if the result is not sorted
   *        on any column. The primary sort column is _sortedBy[0]
   *        (if it exists).
   */
  vector<size_t> _sortedBy;

  IdTable _idTable;

  vector<ResultType> _resultTypes;

  std::shared_ptr<LocalVocab> _localVocab;

  explicit ResultTable(ad_utility::AllocatorWithLimit<Id> allocator);

  ResultTable(const ResultTable& other) = delete;

  ResultTable(ResultTable&& other) = default;

  ResultTable& operator=(const ResultTable& other) = delete;

  ResultTable& operator=(ResultTable&& other) = default;

  virtual ~ResultTable();

  size_t size() const;
  size_t width() const { return _idTable.numColumns(); }

  // Log to INFO the size of this result.
  //
  // NOTE: Due to the current sub-optimal design of `Server::processQuery`, we
  // need the same message in multiple places and so instead of duplicating the
  // message, we should have a method for it.
  void logResultSize() const {
    LOG(INFO) << "Result has size " << size() << " x " << width() << std::endl;
  }

  void clear();

  string asDebugString() const;

  ResultType getResultType(size_t col) const {
    if (col < _resultTypes.size()) {
      return _resultTypes[col];
    }
    return ResultType::KB;
  }

 private:
};
