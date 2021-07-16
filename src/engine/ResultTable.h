// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <vector>

#include "../global/Id.h"
#include "../util/Exception.h"
#include "IdTable.h"

using std::array;
using std::condition_variable;
using std::lock_guard;
using std::mutex;
using std::unique_lock;
using std::vector;

class ResultTable {
 public:
  enum Status { IN_PROGRESS = 0, FINISHED = 1, ABORTED = 2 };

  /**
   * @brief Describes the type of a columns data
   */
  enum class ResultType {
    // An entry in the knowledgebase
    KB,
    // An unsigned integer (size_t)
    VERBATIM,
    // A byte offset in the text index
    TEXT,
    // A 32 bit float, stored in the first 4 bytes of the entry. The last four
    // bytes have to be zero.
    FLOAT,
    // An entry in the ResultTable's _localVocab
    LOCAL_VOCAB
  };

  /**
   * @brief This vector contains a list of column indices by which the result
   *        is sorted. This vector may be empty if the result is not sorted
   *        on any column. The primary sort column is _sortedBy[0]
   *        (if it exists).
   */
  vector<size_t> _sortedBy;

  IdTable _data;

  vector<ResultType> _resultTypes;

  // This vector is used to store generated strings (such as the GROUP_CONCAT
  // results) which are used in the output with the ResultType::STRING type.
  // A std::shared_ptr is used to allow for quickly passing the vocabulary
  // from one result to the next, as any operation that occurs after one
  // having added entries to the _localVocab needs to ensure its ResultTable
  // has the same _localVocab. As currently entries in the _localVocab are not
  // being moved or deleted having a single copy used by several operations
  // should not lead to any references to the _localVocab being invalidated
  // due to later use.
  // WARNING: Currently only operations that can run after a GroupBy copy
  //          the _localVocab of a subresult.
  std::shared_ptr<vector<string>> _localVocab;

  ResultTable(ad_utility::AllocatorWithLimit<Id> allocator);

  ResultTable(const ResultTable& other) = delete;

  ResultTable(ResultTable&& other) = delete;

  ResultTable& operator=(ResultTable other) = delete;

  ResultTable& operator=(ResultTable&& other) = delete;

  virtual ~ResultTable();

  std::optional<std::string> idToOptionalString(Id id) const {
    if (id < _localVocab->size()) {
      return (*_localVocab)[id];
    } else if (id == ID_NO_VALUE) {
      return std::nullopt;
    }
    return std::nullopt;
  }

  size_t size() const;
  size_t width() const { return _data.cols(); }

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
