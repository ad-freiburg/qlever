// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/BinaryDiff.h"

namespace ad_utility {

// _____________________________________________________________________________
BinaryDiff::BinaryDiff(ql::span<const char> base)
    : baseSize_{base.size()}, baseChecksum_{checksum(base)} {}

// _____________________________________________________________________________
void BinaryDiff::addAlign(uint64_t alignment) {
  AD_CONTRACT_CHECK(isPowerOfTwo(alignment),
                    "The alignment of an `Align` instruction has to be a power "
                    "of two, but is ",
                    alignment);
  size_t alignedSize = alignUp(targetSize_, alignment);
  // An alignment that the target already has is a no-op, see `addAlign` in the
  // header.
  if (alignedSize == targetSize_) {
    return;
  }
  instructions_.push_back(Align{alignment});
  targetSize_ = alignedSize;
}

// _____________________________________________________________________________
void BinaryDiff::addCopy(uint64_t baseOffset, uint64_t length) {
  AD_CONTRACT_CHECK(baseOffset <= baseSize_ && length <= baseSize_ - baseOffset,
                    "The copied range [", baseOffset, ", ", baseOffset + length,
                    ") does not lie within the base of size ", baseSize_);
  targetSize_ += length;
  if (!instructions_.empty()) {
    if (auto* previous = std::get_if<Copy>(&instructions_.back());
        previous != nullptr &&
        previous->baseOffset_ + previous->length_ == baseOffset) {
      previous->length_ += length;
      return;
    }
  }
  instructions_.push_back(Copy{baseOffset, length});
}

// _____________________________________________________________________________
void BinaryDiff::addInsert(std::vector<char> bytes) {
  targetSize_ += bytes.size();
  instructions_.push_back(Insert{std::move(bytes)});
}

// _____________________________________________________________________________
void BinaryDiff::addInsert(ql::span<const char> bytes) {
  addInsert(std::vector<char>{bytes.begin(), bytes.end()});
}

// _____________________________________________________________________________
BinaryDiff::Statistics BinaryDiff::statistics() const {
  Statistics statistics;
  for (const auto& instruction : instructions_) {
    if (const auto* copy = std::get_if<Copy>(&instruction)) {
      ++statistics.numCopyInstructions_;
      statistics.numCopiedBytes_ += copy->length_;
    } else if (const auto* insert = std::get_if<Insert>(&instruction)) {
      ++statistics.numInsertInstructions_;
      statistics.numInsertedBytes_ += insert->bytes_.size();
    } else {
      ++statistics.numAlignInstructions_;
    }
  }
  return statistics;
}

// _____________________________________________________________________________
uint64_t BinaryDiff::checksum(ql::span<const char> bytes) {
  uint64_t hash = fnvOffsetBasis;
  for (char byte : bytes) {
    hash ^= static_cast<uint64_t>(static_cast<unsigned char>(byte));
    hash *= 0x100000001b3ULL;
  }
  return hash;
}

// _____________________________________________________________________________
bool BinaryDiff::isPowerOfTwo(uint64_t alignment) {
  return alignment > 0 && (alignment & (alignment - 1)) == 0;
}

// _____________________________________________________________________________
size_t BinaryDiff::alignUp(size_t offset, uint64_t alignment) {
  return (offset + alignment - 1) & ~(alignment - 1);
}

// _____________________________________________________________________________
void BinaryDiff::recomputeTargetSize() {
  targetSize_ = 0;
  for (const auto& instruction : instructions_) {
    if (const auto* copy = std::get_if<Copy>(&instruction)) {
      targetSize_ += copy->length_;
    } else if (const auto* insert = std::get_if<Insert>(&instruction)) {
      targetSize_ += insert->bytes_.size();
    } else {
      targetSize_ =
          alignUp(targetSize_, std::get<Align>(instruction).alignment_);
    }
  }
}

// _____________________________________________________________________________
void BinaryDiff::checkBase(ql::span<const char> base) const {
  // The size is checked first, because it is much cheaper than the checksum.
  AD_CONTRACT_CHECK(base.size() == baseSize_, wrongBaseMessage);
  AD_CONTRACT_CHECK(checksum(base) == baseChecksum_, wrongBaseMessage);
}

// _____________________________________________________________________________
void BinaryDiff::checkInstructions(ql::span<const char> base) const {
  // NOTE: The instructions of a diff that was created via `addCopy` always
  // fulfill this check; those of a diff that was deserialized from a corrupted
  // input might not. The alignments are already checked when reading a diff,
  // because they do not depend on the base.
  for (const auto& instruction : instructions_) {
    const auto* copy = std::get_if<Copy>(&instruction);
    if (copy == nullptr) {
      continue;
    }
    AD_CONTRACT_CHECK(copy->baseOffset_ <= base.size() &&
                          copy->length_ <= base.size() - copy->baseOffset_,
                      invalidInstructionMessage);
  }
}

}  // namespace ad_utility
