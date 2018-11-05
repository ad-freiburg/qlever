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

using std::array;
using std::condition_variable;
using std::lock_guard;
using std::mutex;
using std::unique_lock;
using std::vector;

class ResultTable {
 public:
  enum Status { FINISHED = 0, OTHER = 1 };

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

  size_t _nofColumns;
  // A value >= _nofColumns indicates unsorted data
  vector<size_t> _sortedBy;

  vector<vector<Id>> _varSizeData;
  void* _fixedSizeData;

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

  ResultTable();

  ResultTable(const ResultTable& other) = delete;

  ResultTable(ResultTable&& other) = delete;

  ResultTable& operator=(ResultTable other) = delete;

  ResultTable& operator=(ResultTable&& other) = delete;

  virtual ~ResultTable();

  void finish() {
    lock_guard<mutex> lk(_cond_var_m);
    _status = ResultTable::FINISHED;
    _cond_var.notify_all();
  }

  bool isFinished() const {
    lock_guard<mutex> lk(_cond_var_m);
    bool tmp = _status == ResultTable::FINISHED;
    return tmp;
  }

  void awaitFinished() const {
    unique_lock<mutex> lk(_cond_var_m);
    _cond_var.wait(lk, [&] { return _status == ResultTable::FINISHED; });
  }

  std::optional<std::string> idToOptionalString(Id id) const {
    if (id < _localVocab->size()) {
      return (*_localVocab)[id];
    } else if (id == ID_NO_VALUE) {
      return std::nullopt;
    }
    return std::nullopt;
  }

  size_t size() const;

  void clear();

  string asDebugString() const;

  const vector<vector<Id>> getDataAsVarSize() const {
    if (_varSizeData.size() > 0) {
      return _varSizeData;
    }

    vector<vector<Id>> res;
    if (_nofColumns == 1) {
      const auto& data = *static_cast<vector<array<Id, 1>>*>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(vector<Id>{{data[i][0]}});
      }
    } else if (_nofColumns == 2) {
      const auto& data = *static_cast<vector<array<Id, 2>>*>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(vector<Id>{{data[i][0], data[i][1]}});
      }
    } else if (_nofColumns == 3) {
      const auto& data = *static_cast<vector<array<Id, 3>>*>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(vector<Id>{{data[i][0], data[i][1], data[i][2]}});
      }
    } else if (_nofColumns == 4) {
      const auto& data = *static_cast<vector<array<Id, 4>>*>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(
            vector<Id>{{data[i][0], data[i][1], data[i][2], data[i][3]}});
      }
    } else if (_nofColumns == 5) {
      const auto& data = *static_cast<vector<array<Id, 5>>*>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(vector<Id>{
            {data[i][0], data[i][1], data[i][2], data[i][3], data[i][4]}});
      }
    }
    return res;
  }

  ResultType getResultType(unsigned int col) const {
    if (col < _resultTypes.size()) {
      return _resultTypes[col];
    }
    return ResultType::KB;
  }

 private:
  // See this SO answer for why mutable is ok here
  // https://stackoverflow.com/questions/3239905/c-mutex-and-const-correctness
  mutable condition_variable _cond_var;
  mutable mutex _cond_var_m;
  Status _status;
};
