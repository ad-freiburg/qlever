// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_ZIPPERJOINER_H
#define QLEVER_SRC_ENGINE_IDTABLE_ZIPPERJOINER_H

#include <cstring>

#include "engine/idTable/IdColumn.h"
#include "global/Id.h"

namespace columnBasedIdTable {

// Merges two sorted `ConstIdColumn`s (see `IdColumnZipperJoin.h`), comparing
// datatype bytes and payload words directly instead of materializing `Id`s.
// `run()` alternates between two phases: `mergeBitwiseChunk()` compares
// bitwise for the stretches without `LocalVocabIndex` IDs (which don't
// compare bitwise), `mergeSemanticChunk()` falls back to `Id::compareThreeWay`
// around them. Construct once, call `run()` once.
template <typename AddRow, typename AddRows, typename NotFoundAction,
          typename CancelCallback>
class ZipperJoiner {
 public:
  ZipperJoiner(ConstIdColumn left, ConstIdColumn right, AddRow& addRow,
               AddRows& addRows, NotFoundAction& notFoundAction,
               CancelCallback& cancelCallback)
      : typesLeft_{left.rawDatatypes()},
        typesRight_{right.rawDatatypes()},
        payloadsLeft_{left.rawPayloads()},
        payloadsRight_{right.rawPayloads()},
        numLeft_{left.size()},
        numRight_{right.size()},
        nextLocalVocabLeft_{nextLocalVocabPosition(typesLeft_, 0)},
        nextLocalVocabRight_{nextLocalVocabPosition(typesRight_, 0)},
        addRow_{addRow},
        addRows_{addRows},
        notFoundAction_{notFoundAction},
        cancelCallback_{cancelCallback} {}

  void run() {
    while (l_ < numLeft_ && r_ < numRight_) {
      refreshNextLocalVocabPositions();
      mergeBitwiseChunk();
      if (l_ >= numLeft_ || r_ >= numRight_) {
        break;
      }
      mergeSemanticChunk();
    }
    // Report the unmatched trailing elements of the left column (relevant
    // for OPTIONAL joins and MINUS).
    for (; l_ < numLeft_; ++l_) {
      notFoundAction_(l_);
    }
  }

 private:
  static constexpr uint8_t localVocabType_ =
      static_cast<uint8_t>(Datatype::LocalVocabIndex);

  // Position of the next `LocalVocabIndex` ID at or after `from`, or
  // `types.size()` if there is none.
  static size_t nextLocalVocabPosition(ql::span<const uint8_t> types,
                                       size_t from) {
    const void* pointer =
        std::memchr(types.data() + from, localVocabType_, types.size() - from);
    return pointer == nullptr
               ? types.size()
               : static_cast<size_t>(static_cast<const uint8_t*>(pointer) -
                                     types.data());
  }

  // Lazily recompute a side's cached next-`LocalVocabIndex` position once
  // the current position has passed it.
  void refreshNextLocalVocabPositions() {
    if (nextLocalVocabLeft_ < l_) {
      nextLocalVocabLeft_ = nextLocalVocabPosition(typesLeft_, l_);
    }
    if (nextLocalVocabRight_ < r_) {
      nextLocalVocabRight_ = nextLocalVocabPosition(typesRight_, r_);
    }
  }

  // --- Phase 1 (mergeBitwiseChunk and its helpers) ---

  // End of the run of equal datatype bytes starting at `begin` (bounded by
  // `end`).
  static size_t typeRunEnd(ql::span<const uint8_t> types, size_t begin,
                           size_t end) {
    uint8_t type = types[begin];
    size_t position = begin + 1;
    while (position < end && types[position] == type) {
      ++position;
    }
    return position;
  }

  // Skip the run of the smaller datatype; it can't match anything on the
  // other side (datatype-major ID order).
  void skipSmallerTypeRun(uint8_t typeLeft, uint8_t typeRight) {
    if (typeLeft < typeRight) {
      size_t runEnd = typeRunEnd(typesLeft_, l_, nextLocalVocabLeft_);
      for (; l_ < runEnd; ++l_) {
        notFoundAction_(l_);
      }
    } else {
      r_ = typeRunEnd(typesRight_, r_, nextLocalVocabRight_);
    }
  }

  // Merge two runs of matching datatype on their payloads only. Returns
  // true, without consuming the current group, if a group reaches the
  // chunk boundary and may need to be finished by `mergeSemanticChunk()`.
  bool mergeEqualTypeRuns(size_t runEndLeft, size_t runEndRight) {
    while (l_ < runEndLeft && r_ < runEndRight) {
      if (++stepsUntilCancelCheck_ >= (1ull << 20)) {
        stepsUntilCancelCheck_ = 0;
        cancelCallback_();
      }
      uint64_t payloadLeft = payloadsLeft_[l_];
      uint64_t payloadRight = payloadsRight_[r_];
      if (payloadLeft < payloadRight) {
        notFoundAction_(l_);
        ++l_;
      } else if (payloadRight < payloadLeft) {
        ++r_;
      } else {
        // Extend to the ranges of equal payloads (bounded by the run ends).
        size_t endLeft = l_ + 1;
        while (endLeft < runEndLeft && payloadsLeft_[endLeft] == payloadLeft) {
          ++endLeft;
        }
        size_t endRight = r_ + 1;
        while (endRight < runEndRight &&
               payloadsRight_[endRight] == payloadRight) {
          ++endRight;
        }
        // Might continue past a following `LocalVocabIndex` ID; defer to
        // the semantic phase.
        if ((endLeft == nextLocalVocabLeft_ && endLeft < numLeft_) ||
            (endRight == nextLocalVocabRight_ && endRight < numRight_)) {
          return true;
        }
        if (endLeft == l_ + 1 && endRight == r_ + 1) {
          addRow_(l_, r_);
        } else {
          addRows_(l_, endLeft, r_, endRight);
        }
        l_ = endLeft;
        r_ = endRight;
      }
    }
    return false;
  }

  // Bitwise-merge the chunk before the next `LocalVocabIndex` ID, run by
  // run of matching datatype.
  void mergeBitwiseChunk() {
    bool groupTouchesChunkEnd = false;
    while (l_ < nextLocalVocabLeft_ && r_ < nextLocalVocabRight_ &&
           !groupTouchesChunkEnd) {
      uint8_t typeLeft = typesLeft_[l_];
      uint8_t typeRight = typesRight_[r_];
      if (typeLeft != typeRight) {
        skipSmallerTypeRun(typeLeft, typeRight);
        continue;
      }
      const size_t runEndLeft = typeRunEnd(typesLeft_, l_, nextLocalVocabLeft_);
      const size_t runEndRight =
          typeRunEnd(typesRight_, r_, nextLocalVocabRight_);
      groupTouchesChunkEnd = mergeEqualTypeRuns(runEndLeft, runEndRight);
    }
  }

  // --- Phase 2 (mergeSemanticChunk and its helper) ---

  // End of the group of elements equal to the one at `group`, extended
  // bitwise and then, if it borders a `LocalVocabIndex` ID, semantically.
  static size_t semanticGroupEnd(ql::span<const uint8_t> types,
                                 ql::span<const uint64_t> payloads,
                                 size_t numRows, size_t group) {
    size_t end = group + 1;
    while (end < numRows && types[end] == types[group] &&
           payloads[end] == payloads[group]) {
      ++end;
    }
    if (end < numRows &&
        (types[end] == localVocabType_ || types[group] == localVocabType_)) {
      Id representative = Id::fromBits({types[group], payloads[group]});
      while (end < numRows &&
             representative == Id::fromBits({types[end], payloads[end]})) {
        ++end;
      }
    }
    return end;
  }

  // Compare and merge elements as `Id`s while either side is at a
  // `LocalVocabIndex` ID.
  void mergeSemanticChunk() {
    do {
      cancelCallback_();
      Id idLeft = Id::fromBits({typesLeft_[l_], payloadsLeft_[l_]});
      Id idRight = Id::fromBits({typesRight_[r_], payloadsRight_[r_]});
      auto comparison = idLeft.compareThreeWay(idRight);
      if (comparison < 0) {
        notFoundAction_(l_);
        ++l_;
      } else if (comparison > 0) {
        ++r_;
      } else {
        size_t endLeft =
            semanticGroupEnd(typesLeft_, payloadsLeft_, numLeft_, l_);
        size_t endRight =
            semanticGroupEnd(typesRight_, payloadsRight_, numRight_, r_);
        if (endLeft == l_ + 1 && endRight == r_ + 1) {
          addRow_(l_, r_);
        } else {
          addRows_(l_, endLeft, r_, endRight);
        }
        l_ = endLeft;
        r_ = endRight;
      }
    } while (l_ < numLeft_ && r_ < numRight_ &&
             (typesLeft_[l_] == localVocabType_ ||
              typesRight_[r_] == localVocabType_));
  }

  ql::span<const uint8_t> typesLeft_;
  ql::span<const uint8_t> typesRight_;
  ql::span<const uint64_t> payloadsLeft_;
  ql::span<const uint64_t> payloadsRight_;
  size_t numLeft_;
  size_t numRight_;
  size_t l_ = 0;
  size_t r_ = 0;
  size_t nextLocalVocabLeft_;
  size_t nextLocalVocabRight_;
  size_t stepsUntilCancelCheck_ = 0;
  AddRow& addRow_;
  AddRows& addRows_;
  NotFoundAction& notFoundAction_;
  // Checked periodically so a long-running join can react to a query
  // timeout or cancellation.
  CancelCallback& cancelCallback_;
};

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_ZIPPERJOINER_H
