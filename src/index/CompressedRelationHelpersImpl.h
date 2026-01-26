// Copyright 2025 The QLever Authors, in particular:
//
// 2021 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "index/CompressedRelation.h"
#include "util/ExceptionHandling.h"

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_

namespace compressedRelationHelpers {

static constexpr size_t c1Idx = 1;
static constexpr size_t c2Idx = 2;

// Compares two rows based on the second, third and fourth column only (it
// ignores the first column as well as any payload columns).
struct ComparatorForConstCol0 {
  bool operator()(const auto& a, const auto& b) const {
    return std::tie(a[c1Idx], a[c2Idx], a[ADDITIONAL_COLUMN_GRAPH_ID]) <
           std::tie(b[c1Idx], b[c2Idx], b[ADDITIONAL_COLUMN_GRAPH_ID]);
  }
};

// Helper function to make a row from `IdTable` easier to compare. This ties
// the cells of the given row with the indices 0, 1 and 2.
inline auto tieFirstThreeColumns = [](const auto& row) {
  return std::tie(row[0], row[1], row[2]);
};

// Collect elements of type `T` in batches of size 100'000 and apply the
// `Function` to each batch. For the last batch (which might be smaller)  the
// function is applied in the destructor.
CPP_template(typename T, typename Function)(
    requires ad_utility::InvocableWithExactReturnType<
        Function, void, std::vector<T>&&>) struct Batcher {
  Function function_;
  size_t blocksize_;
  std::vector<T> vec_;

  // ___________________________________________________________________________
  Batcher(Function function, size_t blocksize)
      : function_{std::move(function)}, blocksize_{blocksize} {
    vec_.reserve(blocksize_);
  }

  // ___________________________________________________________________________
  void operator()(T t) {
    vec_.push_back(std::move(t));
    if (vec_.size() >= blocksize_) {
      function_(std::move(vec_));
      vec_.clear();
      vec_.reserve(blocksize_);
    }
  }

  // ___________________________________________________________________________
  ~Batcher() {
    ad_utility::terminateIfThrows(
        [this]() {
          if (!vec_.empty()) {
            function_(std::move(vec_));
          }
        },
        "Batcher function threw an exception while processing the final "
        "(possibly incomplete) block.");
  }

  // No copy or move operations (neither needed nor easy to get right).
  Batcher(const Batcher&) = delete;
  Batcher& operator=(const Batcher&) = delete;
};

using MetadataCallback = CompressedRelationWriter::MetadataCallback;

// The `CompressedRelationMetadata` for a single permutation can be directly
// input blockwise to the `MetadataCallback` (collecting the blocks uses the
// `Batcher` helper from above).
using SingleMetadataWriter =
    Batcher<CompressedRelationMetadata, MetadataCallback>;

// A class that is called for all pairs of `CompressedRelationMetadata` for
// the same `col0Id` and the "twin permutations" (e.g. PSO and POS). The
// multiplicity of the last column is exchanged and then the metadata are
// passed on to the respective `MetadataCallback`.
class PairMetadataWriter {
 private:
  SingleMetadataWriter batcher1_;
  SingleMetadataWriter batcher2_;

 public:
  // ___________________________________________________________________________
  PairMetadataWriter(MetadataCallback callback1, MetadataCallback callback2,
                     size_t blocksize)
      : batcher1_{std::move(callback1), blocksize},
        batcher2_{std::move(callback2), blocksize} {}

  // ___________________________________________________________________________
  void operator()(CompressedRelationMetadata md1,
                  CompressedRelationMetadata md2) {
    md1.multiplicityCol2_ = md2.multiplicityCol1_;
    md2.multiplicityCol2_ = md1.multiplicityCol1_;
    batcher1_(md1);
    batcher2_(md2);
  }
};

// A simple class to count distinct IDs in a sorted sequence.
class DistinctIdCounter {
  Id lastSeen_ = std::numeric_limits<Id>::max();
  size_t count_ = 0;

 public:
  // ___________________________________________________________________________
  void operator()(Id id) {
    count_ += static_cast<size_t>(id != lastSeen_);
    lastSeen_ = id;
  }

  // ___________________________________________________________________________
  size_t getAndReset() {
    lastSeen_ = std::numeric_limits<Id>::max();
    return std::exchange(count_, 0);
  }
};

}  // namespace compressedRelationHelpers

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
