// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::wcoj {

// _____________________________________________________________________________
// Sorted Trie Iterator interface for Leapfrog Triejoin.
//
// Lifetime: implementations that borrow their key range (e.g. via `ql::span`)
// must not outlive the underlying storage; callers keep the owning vector
// alive for as long as any iterator over it is used.
class LeapfrogIterator {
 private:
  ql::span<const Id> sortedKeys_;
  size_t currentIndex_ = 0;

 public:
  explicit LeapfrogIterator(ql::span<const Id> sortedKeys)
      : sortedKeys_(sortedKeys), currentIndex_(0) {}

  [[nodiscard]] bool atEnd() const noexcept {
    return currentIndex_ >= sortedKeys_.size();
  }

  [[nodiscard]] Id key() const noexcept { return sortedKeys_[currentIndex_]; }

  void next() noexcept { currentIndex_++; }

  // The index of the current key in the underlying range.
  [[nodiscard]] size_t position() const noexcept { return currentIndex_; }

  // Fast forward to the first key >= targetKey using binary search
  void seek(Id targetKey) noexcept {
    if (atEnd() || key() >= targetKey) {
      return;
    }
    auto it = std::lower_bound(sortedKeys_.begin() + currentIndex_,
                               sortedKeys_.end(), targetKey);
    currentIndex_ = std::distance(sortedKeys_.begin(), it);
  }

  // Move past all entries that are equal to the current key. Must not be
  // called when `atEnd()`.
  void skipCurrentKey() noexcept {
    auto it = std::upper_bound(sortedKeys_.begin() + currentIndex_,
                               sortedKeys_.end(), key());
    currentIndex_ = std::distance(sortedKeys_.begin(), it);
  }
};

// The positions `[begin_, end_)` of one key in the range of a
// `LeapfrogIterator`.
struct KeyRun {
  size_t begin_;
  size_t end_;
  [[nodiscard]] size_t size() const noexcept { return end_ - begin_; }
};

// _____________________________________________________________________________
// Leapfrog-style simultaneous K-way intersection over sorted Id lists:
// advances the iterator holding the smallest key towards the largest key, so
// no intermediate result tables are materialized (only the output vector).
class LeapfrogJoin {
 public:
  // Call `onMatch(key, runs)` for each key that is contained in all
  // `iterators`, in ascending order of the keys. `runs[i]` holds the positions
  // of all entries that are equal to `key` in the range of `iterators[i]`, so
  // duplicate keys are reported once, with their multiplicities.
  template <size_t N, typename OnMatch>
  static void forEachCommonKey(std::array<LeapfrogIterator, N>& iterators,
                               OnMatch&& onMatch) {
    static_assert(N > 0);
    if (ql::ranges::any_of(iterators, &LeapfrogIterator::atEnd)) {
      return;
    }
    while (true) {
      Id maxKey = iterators[0].key();
      for (const auto& it : iterators) {
        maxKey = std::max(maxKey, it.key());
      }
      // Leapfrog every iterator to `maxKey`. If one of them overshoots, the
      // next round starts from its new key.
      bool allEqual = true;
      for (auto& it : iterators) {
        it.seek(maxKey);
        if (it.atEnd()) {
          return;
        }
        allEqual = allEqual && it.key() == maxKey;
      }
      if (!allEqual) {
        continue;
      }
      std::array<KeyRun, N> runs;
      for (size_t i = 0; i < N; ++i) {
        size_t begin = iterators[i].position();
        iterators[i].skipCurrentKey();
        runs[i] = KeyRun{begin, iterators[i].position()};
      }
      onMatch(maxKey, std::as_const(runs));
      if (ql::ranges::any_of(iterators, &LeapfrogIterator::atEnd)) {
        return;
      }
    }
  }

  static std::vector<Id> intersect(std::vector<LeapfrogIterator>& iterators) {
    std::vector<Id> result;
    if (iterators.empty()) {
      return result;
    }

    for (const auto& it : iterators) {
      if (it.atEnd()) {
        return result;
      }
    }

    // Index of the iterator holding the smallest current key. The match test
    // `key[p] == maxKey` is only valid when p is the minimum: together with
    // maxKey being the maximum, equality implies all keys are equal.
    size_t k = iterators.size();
    auto indexOfMinKey = [&iterators, k]() {
      size_t minIdx = 0;
      for (size_t i = 1; i < k; ++i) {
        if (iterators[i].key() < iterators[minIdx].key()) {
          minIdx = i;
        }
      }
      return minIdx;
    };
    size_t p = indexOfMinKey();
    Id maxKey = iterators[0].key();
    for (size_t i = 1; i < k; ++i) {
      if (iterators[i].key() > maxKey) {
        maxKey = iterators[i].key();
      }
    }

    while (true) {
      Id currentKey = iterators[p].key();

      if (currentKey == maxKey) {
        // Smallest and largest keys agree, so all iterators match here.
        result.push_back(currentKey);
        iterators[p].next();
        if (iterators[p].atEnd()) {
          break;
        }
        maxKey = iterators[p].key();
      } else {
        // Leapfrog forward to maxKey
        iterators[p].seek(maxKey);
        if (iterators[p].atEnd()) {
          break;
        }
        maxKey = iterators[p].key();
      }

      // Re-establish the minimum holder; round-robin order is not valid
      // because the updated iterator is no longer ordered relative to p.
      p = indexOfMinKey();
    }

    return result;
  }
};

}  // namespace ql::engine::wcoj
