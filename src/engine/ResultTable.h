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

#include "engine/IdTable.h"
#include "engine/ResultType.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Exception.h"
#include "util/HashMap.h"

using std::array;
using std::condition_variable;
using std::lock_guard;
using std::mutex;
using std::unique_lock;
using std::vector;

// The local vocabulary for a particular result table. It maps the IDs that are
// not part of the normal vocabulary
//
//
// It contains a map from
// (local vocab) ids
class LocalVocab {
 public:
  // Create a new, empty local vocabulary.
  LocalVocab() {}

  // Prevent accidental copying of a local vocabulary.
  // TODO: Needed in SparqlExpressionTestHelpers.h:91.
  // LocalVocab(const LocalVocab&) = delete;

  // Get ID of a word in the local vocabulary. If the word was already
  // contained, return the already existing ID. If the word was not yet
  // contained, add it, and return the new ID.
  [[maybe_unused]] Id getIdAndAddIfNotContained(const std::string& word);

  // Start the construction of a local vocabulary. This is currently allowed
  // only once, when the vocabulary is still empty.
  void startConstructionPhase();

  // Signal that the construction of the local vocabulary is done. This call
  // will clear the `wordsToIdsMap_` (to save space) and afterwards,
  // `getIdAndAddIfNotContained` can no longer be called.
  void endConstructionPhase();

  // The number of words in the vocabulary.
  size_t size() const { return words_.size(); }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return words_.empty(); }

  // Return a const reference to the i-th word.
  const std::string& operator[](size_t i) const { return words_[i]; }

 private:
  // The words of the local vocabulary. The index of a word in the `std::vector`
  // corresponds to its ID in the local vocabulary.
  std::vector<string> words_;

  // Remember which words are already in the vocabulary and with which ID. This
  // map is only used during the construction of a local vocabulary and can (and
  // should) be cleared when the construction is done (to save space).
  ad_utility::HashMap<std::string, Id> wordsToIdsMap_;

  // Indicator whether the vocabulary is still under construction (only then can
  // `getIdAndAddIfNotContained` be called) or done.
  bool constructionHasFinished_ = false;
};

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

  std::optional<std::string> indexToOptionalString(LocalVocabIndex idx) const {
    if (idx.get() < _localVocab->size()) {
      return (*_localVocab)[idx.get()];
    }
    return std::nullopt;
  }

  size_t size() const;
  size_t width() const { return _idTable.cols(); }

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
