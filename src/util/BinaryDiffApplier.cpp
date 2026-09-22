// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/BinaryDiffApplier.h"

#include <absl/numeric/bits.h>

#include <string_view>
#include <utility>

#include "backports/algorithm.h"
#include "util/BitUtils.h"
#include "util/CryptographicHashUtils.h"
#include "util/OverloadCallOperator.h"
#include "util/TransparentFunctors.h"
#include "util/VariantRangeFilter.h"

namespace ad_utility {

// _____________________________________________________________________________
BinaryDiffApplier::BinaryDiffApplier(ql::span<const char> base)
    : baseSize_{base.size()}, baseChecksum_{checksum(base)} {}

// _____________________________________________________________________________
void BinaryDiffApplier::addAlign(uint64_t alignment) {
  AD_CONTRACT_CHECK(absl::has_single_bit(alignment),
                    "The alignment of an `Align` instruction has to be a power "
                    "of two, but is ",
                    alignment);
  size_t alignedSize = alignUp(targetSize_, alignment);
  // An alignment that the target already has is a no-op, see `addAlign` in the
  // header.
  //
  // NOTE: In contrast to the `Copy` and the `Insert` instructions, two
  // consecutive `Align` instructions are deliberately not merged into one.
  // They arise when the section of the target in between them is empty, and a
  // consumer of such a section reads zero bytes from it, but might still check
  // the alignment of the pointer to it, so keep the padding of both.
  if (std::exchange(targetSize_, alignedSize) != alignedSize) {
    instructions_.push_back(Align{alignment});
  }
}

// _____________________________________________________________________________
void BinaryDiffApplier::addCopy(uint64_t baseOffset, uint64_t length) {
  AD_CONTRACT_CHECK(baseOffset <= baseSize_ && length <= baseSize_ - baseOffset,
                    "The copied range [", baseOffset, ", ", baseOffset + length,
                    ") does not lie within the base of size ", baseSize_);
  targetSize_ += length;
  // If the previous instruction copies the range that directly precedes the new
  // one, then merge the two into a single copy instruction.
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
void BinaryDiffApplier::addInsert(std::vector<char> bytes) {
  targetSize_ += bytes.size();
  // If the previous instruction is also an insert, then merge the two into a
  // single insert instruction.
  if (!instructions_.empty()) {
    if (auto* previous = std::get_if<Insert>(&instructions_.back());
        previous != nullptr) {
      previous->bytes_.insert(previous->bytes_.end(), bytes.begin(),
                              bytes.end());
      return;
    }
  }
  instructions_.push_back(Insert{std::move(bytes)});
}

// _____________________________________________________________________________
void BinaryDiffApplier::addInsert(ql::span<const char> bytes) {
  addInsert(std::vector<char>{bytes.begin(), bytes.end()});
}

// _____________________________________________________________________________
BinaryDiffApplier::Statistics BinaryDiffApplier::statistics() const {
  Statistics statistics;
  auto visitor = OverloadCallOperator{
      [&statistics](const Copy& copy) {
        ++statistics.numCopyInstructions_;
        statistics.numCopiedBytes_ += copy.length_;
      },
      [&statistics](const Insert& insert) {
        ++statistics.numInsertInstructions_;
        statistics.numInsertedBytes_ += insert.bytes_.size();
      },
      [&statistics](const Align&) { ++statistics.numAlignInstructions_; }};
  for (const auto& instruction : instructions_) {
    std::visit(visitor, instruction);
  }
  return statistics;
}

// _____________________________________________________________________________
BinaryDiffApplier::Checksum BinaryDiffApplier::checksum(
    ql::span<const char> bytes) {
  auto digest = HashSha256{}(std::string_view{bytes.data(), bytes.size()});
  Checksum result{};
  AD_CORRECTNESS_CHECK(digest.size() == result.size());
  ql::ranges::transform(digest, result.begin(), ad_utility::staticCast<char>);
  return result;
}

// _____________________________________________________________________________
void BinaryDiffApplier::recomputeTargetSize() {
  targetSize_ = 0;
  auto visitor = OverloadCallOperator{
      [this](const Copy& copy) { targetSize_ += copy.length_; },
      [this](const Insert& insert) { targetSize_ += insert.bytes_.size(); },
      [this](const Align& align) {
        targetSize_ = alignUp(targetSize_, align.alignment_);
      }};
  for (const auto& instruction : instructions_) {
    std::visit(visitor, instruction);
  }
}

// _____________________________________________________________________________
void BinaryDiffApplier::apply(ql::span<const char> base,
                              ql::span<char> target) const {
  checkApplicable(base);
  AD_CONTRACT_CHECK(target.size() == targetSize(),
                    "The target of a `BinaryDiffApplier` has to have exactly ",
                    targetSize(), " bytes, but has ", target.size());
  applyToCheckedTarget(base, target);
}

// _____________________________________________________________________________
void BinaryDiffApplier::applyToCheckedTarget(ql::span<const char> base,
                                             ql::span<char> target) const {
  size_t offset = 0;
  auto visitor = OverloadCallOperator{
      [&base, &target, &offset](const Copy& copy) {
        ql::ranges::copy(base.subspan(copy.baseOffset_, copy.length_),
                         target.begin() + offset);
        offset += copy.length_;
      },
      [&target, &offset](const Insert& insert) {
        ql::ranges::copy(insert.bytes_, target.begin() + offset);
        offset += insert.bytes_.size();
      },
      [&target, &offset](const Align& align) {
        size_t alignedOffset = alignUp(offset, align.alignment_);
        // NOTE: The padding has to be written explicitly, because `target`
        // might be a buffer of the caller that contains arbitrary bytes.
        ql::ranges::fill(target.subspan(offset, alignedOffset - offset),
                         char{0});
        offset = alignedOffset;
      }};
  for (const auto& instruction : instructions_) {
    std::visit(visitor, instruction);
  }
  AD_CORRECTNESS_CHECK(offset == target.size());
}

// _____________________________________________________________________________
void BinaryDiffApplier::checkApplicable(ql::span<const char> base) const {
  checkBase(base);
  // Validate all instructions before the first byte is written, so that a
  // partially written target cannot result from an invalid instruction.
  checkInstructions(base);
}

// _____________________________________________________________________________
void BinaryDiffApplier::checkBase(ql::span<const char> base) const {
  constexpr std::string_view wrongBaseMessage =
      "The given diff was created against a different base (the size or the "
      "checksum of the base does not match). Note that a diff has to be "
      "applied to exactly the base that it was created against";
  // The size is checked first, because it is much cheaper than the checksum.
  AD_CONTRACT_CHECK(base.size() == baseSize_, wrongBaseMessage);
  AD_CONTRACT_CHECK(checksum(base) == baseChecksum_, wrongBaseMessage);
}

// _____________________________________________________________________________
void BinaryDiffApplier::checkInstructions(ql::span<const char> base) const {
  constexpr std::string_view invalidInstructionMessage =
      "The given diff contains an invalid instruction; it is either corrupted, "
      "or it was created against a different base";
  // NOTE: The instructions of a diff that was created via `addCopy` always
  // fulfill this check; those of a diff that was deserialized from a corrupted
  // input might not. The alignments are already checked when reading a diff,
  // because they do not depend on the base.
  for (const Copy& copy : filterRangeOfVariantsByType<Copy>(instructions_)) {
    AD_CONTRACT_CHECK(copy.baseOffset_ <= base.size() &&
                          copy.length_ <= base.size() - copy.baseOffset_,
                      invalidInstructionMessage);
  }
}

// _____________________________________________________________________________
void BinaryDiffApplierSerializer::checkAlignments(
    const BinaryDiffApplier& diff) {
  for (const Align& align :
       filterRangeOfVariantsByType<Align>(diff.instructions_)) {
    AD_CONTRACT_CHECK(absl::has_single_bit(align.alignment_),
                      notReadableMessage, ". Details: the alignment ",
                      align.alignment_, " is not a power of two");
  }
}

}  // namespace ad_utility
