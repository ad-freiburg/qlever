// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <absl/numeric/bits.h>

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <vector>

namespace ql::index::vocab {

// _____________________________________________________________________________
// Succinct Vocabulary Trie:
// Encodes in-memory vocabulary prefix structures as succinct bit vectors.
// rank1() answers in O(1) time from per-word cumulative POPCNT checkpoints
// plus a single masked POPCNT on the partial word.
class SuccinctVocabularyTrie {
 private:
  std::vector<uint64_t> topologyBits_;  // Louds-style tree topology
  std::vector<char> labels_;            // Character transition labels
  size_t totalNodes_ = 0;
  // rankCheckpoints_[i] holds the number of 1-bits in topologyBits_[0..i);
  // rebuilt whenever the topology is set.
  std::vector<size_t> rankCheckpoints_{0};

  void rebuildRankCheckpoints() {
    rankCheckpoints_.resize(topologyBits_.size() + 1);
    rankCheckpoints_[0] = 0;
    for (size_t i = 0; i < topologyBits_.size(); ++i) {
      rankCheckpoints_[i + 1] =
          rankCheckpoints_[i] + absl::popcount(topologyBits_[i]);
    }
  }

 public:
  SuccinctVocabularyTrie() = default;

  // Rank1 (number of 1-bits below bitIndex): O(1) via the checkpoints plus
  // one POPCNT on the partially covered word.
  [[nodiscard]] size_t rank1(size_t bitIndex) const noexcept {
    size_t fullWords = std::min(bitIndex / 64, topologyBits_.size());
    size_t count = rankCheckpoints_[fullWords];
    size_t remainder = bitIndex % 64;
    if (remainder > 0 && fullWords < topologyBits_.size()) {
      uint64_t mask = (1ULL << remainder) - 1;
      count += absl::popcount(topologyBits_[fullWords] & mask);
    }

    return count;
  }

  // Test and benchmark setup only: installs a mock topology and rebuilds the
  // rank checkpoints, so the rank1() invariant always holds afterwards.
  void setMockTopology(const std::vector<uint64_t>& bits,
                       const std::vector<char>& labels) {
    topologyBits_ = bits;
    labels_ = labels;
    totalNodes_ = labels.size();
    rebuildRankCheckpoints();
  }

  [[nodiscard]] size_t totalNodes() const noexcept { return totalNodes_; }
  [[nodiscard]] size_t bitVectorSize() const noexcept {
    return topologyBits_.size() * 64;
  }
};

}  // namespace ql::index::vocab
