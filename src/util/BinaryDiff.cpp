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
BinaryDiff::BinaryDiff(ql::span<const char> base, uint64_t alignment)
    : alignment_{alignment},
      baseSize_{base.size()},
      baseChecksum_{checksum(base)} {
  AD_CONTRACT_CHECK(isPowerOfTwo(alignment),
                    "The alignment of a `BinaryDiff` has to be a power of two, "
                    "but is ",
                    alignment);
}

// _____________________________________________________________________________
void BinaryDiff::addCopy(uint64_t baseOffset, uint64_t length) {
  AD_CONTRACT_CHECK(baseOffset % alignment_ == 0,
                    "The offset of a copied range has to be a multiple of the "
                    "alignment ",
                    alignment_, ", but is ", baseOffset);
  AD_CONTRACT_CHECK(baseOffset <= baseSize_ && length <= baseSize_ - baseOffset,
                    "The copied range [", baseOffset, ", ", baseOffset + length,
                    ") does not lie within the base of size ", baseSize_);
  if (!instructions_.empty()) {
    if (auto* previous = std::get_if<Copy>(&instructions_.back());
        previous != nullptr &&
        alignUp(previous->baseOffset_ + previous->length_) == baseOffset) {
      previous->length_ = baseOffset + length - previous->baseOffset_;
      return;
    }
  }
  instructions_.push_back(Copy{baseOffset, length});
}

// _____________________________________________________________________________
void BinaryDiff::addInsert(std::vector<char> bytes) {
  instructions_.push_back(Insert{std::move(bytes)});
}

// _____________________________________________________________________________
void BinaryDiff::addInsert(ql::span<const char> bytes) {
  addInsert(std::vector<char>{bytes.begin(), bytes.end()});
}

// _____________________________________________________________________________
size_t BinaryDiff::targetSize() const {
  size_t size = 0;
  for (const auto& instruction : instructions_) {
    size = alignUp(size);
    if (const auto* copy = std::get_if<Copy>(&instruction)) {
      size += copy->length_;
    } else {
      size += std::get<Insert>(instruction).bytes_.size();
    }
  }
  return size;
}

// _____________________________________________________________________________
BinaryDiff::Statistics BinaryDiff::statistics() const {
  Statistics statistics;
  for (const auto& instruction : instructions_) {
    if (const auto* copy = std::get_if<Copy>(&instruction)) {
      ++statistics.numCopyInstructions_;
      statistics.numCopiedBytes_ += copy->length_;
    } else {
      ++statistics.numInsertInstructions_;
      statistics.numInsertedBytes_ +=
          std::get<Insert>(instruction).bytes_.size();
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
size_t BinaryDiff::alignUp(size_t offset) const {
  return (offset + alignment_ - 1) & ~(alignment_ - 1);
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
  // fulfill these checks; those of a diff that was deserialized from a
  // corrupted input might not.
  for (const auto& instruction : instructions_) {
    const auto* copy = std::get_if<Copy>(&instruction);
    if (copy == nullptr) {
      continue;
    }
    AD_CONTRACT_CHECK(copy->baseOffset_ % alignment_ == 0,
                      invalidInstructionMessage);
    AD_CONTRACT_CHECK(copy->baseOffset_ <= base.size() &&
                          copy->length_ <= base.size() - copy->baseOffset_,
                      invalidInstructionMessage);
  }
}

}  // namespace ad_utility
