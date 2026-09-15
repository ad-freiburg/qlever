// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BINARYDIFF_H
#define QLEVER_SRC_UTIL_BINARYDIFF_H

#include <absl/strings/str_cat.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "util/Exception.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility {

// The serialization of a `BinaryDiff`, see below.
class BinaryDiffSerializer;

// A diff that turns a "base" byte buffer into a "target" byte buffer. It is a
// sequence of instructions, each of which copies a range of the base (`Copy`),
// inserts literal bytes (`Insert`), or pads the target with zeros up to a
// given alignment (`Align`). A diff is created by constructing it from the
// base and then appending instructions (see `addAlign`, `addCopy` and
// `addInsert`), and it is applied to a base via `apply`. It can be serialized
// and deserialized with the QLever serializer framework (see
// `util/Serializer/Serializer.h`, and `BinaryDiffSerializer` below for the
// format).
//
// ALIGNMENT: The alignment is not a property of the diff, but an ordinary
// instruction, so that it can change from one part of the target to the next
// ("align to 8, copy these bytes, insert those bytes, now align to 16, ...").
// This makes it possible to diff a format that consists of aligned,
// *position-independent* blocks: such a block can be copied to any other
// suitably aligned offset of the target and still be read back correctly, so
// that a target buffer can be assembled from blocks of the base buffer. It is
// then up to the creator of the diff to emit an `Align` in front of every such
// block; a format without alignment requirements simply uses no `Align` at
// all.
//
// BASE IDENTIFICATION: A diff stores the size and a checksum (see `checksum`)
// of the base that it was created against, and `apply` verifies both. A diff
// therefore has to be applied to exactly the base that it was created against;
// applying it to any other buffer throws instead of silently producing garbage.
class BinaryDiff {
 public:
  // An instruction that copies the bytes `[baseOffset_, baseOffset_ + length_)`
  // of the base.
  struct Copy {
    uint64_t baseOffset_ = 0;
    uint64_t length_ = 0;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Copy, baseOffset_, length_)
  };

  // An instruction that inserts the literal bytes `bytes_`, which are stored
  // in the diff itself.
  struct Insert {
    std::vector<char> bytes_;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Insert, bytes_)
  };

  // An instruction that pads the target with zeros until its size is a multiple
  // of `alignment_`, which has to be a power of two.
  struct Align {
    uint64_t alignment_ = 1;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Align, alignment_)
  };

  using Instruction = std::variant<Copy, Insert, Align>;

  // A summary of the instructions of a diff, as computed by `statistics()`.
  // Meant for tests and for logging by the users of this class.
  struct Statistics {
    size_t numCopyInstructions_ = 0;
    size_t numInsertInstructions_ = 0;
    size_t numAlignInstructions_ = 0;
    size_t numCopiedBytes_ = 0;
    size_t numInsertedBytes_ = 0;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(
        Statistics, numCopyInstructions_, numInsertInstructions_,
        numAlignInstructions_, numCopiedBytes_, numInsertedBytes_)
  };

 private:
  // The message that is reported when a diff is applied to a base other than
  // the one that it was created against.
  static constexpr std::string_view wrongBaseMessage =
      "The given diff was created against a different base (the size or the "
      "checksum of the base does not match). Note that a diff has to be "
      "applied to exactly the base that it was created against";

  // The message that is reported when a diff has an instruction that does not
  // make sense for the given base.
  static constexpr std::string_view invalidInstructionMessage =
      "The given diff contains an invalid instruction; it is either corrupted, "
      "or it was created against a different base";

  // The FNV-1a 64 offset basis, which is the checksum of an empty input, see
  // `checksum`.
  static constexpr uint64_t fnvOffsetBasis = 0xcbf29ce484222325ULL;

  uint64_t baseSize_ = 0;
  uint64_t baseChecksum_ = fnvOffsetBasis;
  std::vector<Instruction> instructions_;
  // The size of the target that `instructions_` currently produces. It is
  // maintained incrementally, so that `addAlign` can detect an alignment that
  // the target already has without walking all instructions.
  size_t targetSize_ = 0;

  // The serialization needs access to the members above, and is factored out
  // into its own class to keep this class free of the details of the format.
  friend class BinaryDiffSerializer;

 public:
  // The default constructor creates an empty diff for an empty base. It is
  // needed for deserialization; to create a diff, use the constructor below.
  BinaryDiff() = default;

  // Start a diff that turns `base` into some target.
  explicit BinaryDiff(ql::span<const char> base);

  // Append an instruction that pads the target with zeros until its size is a
  // multiple of `alignment`, which has to be a power of two.
  //
  // If the target already has that alignment, nothing is appended, because the
  // instruction would be a no-op. This keeps a diff free of redundant
  // alignments, and it allows the copies around such an alignment to be merged
  // (see `addCopy`).
  void addAlign(uint64_t alignment);

  // Append an instruction that copies the bytes
  // `[baseOffset, baseOffset + length)` of the base, which has to lie within
  // the base.
  //
  // If the previous instruction is a copy of the range that directly precedes
  // `[baseOffset, ...)` in the base, the two are merged into a single
  // instruction.
  void addCopy(uint64_t baseOffset, uint64_t length);

  // Append an instruction that inserts the given literal bytes, which become
  // part of the diff itself.
  void addInsert(std::vector<char> bytes);
  void addInsert(ql::span<const char> bytes);

  // Apply this diff to `base` and return the target. The `base` has to be
  // exactly the buffer that this diff was created against (which is verified
  // via its size and checksum, see the class comment); if it is not, or if an
  // instruction of the diff is invalid for it, throw with a descriptive
  // message. The returned buffer uses the given `allocator`, which allows
  // callers to obtain an aligned or a `pmr`-allocated buffer.
  template <typename Allocator = std::allocator<char>>
  std::vector<char, Allocator> apply(ql::span<const char> base,
                                     Allocator allocator = {}) const;

  // The exact size of the buffer that `apply` returns.
  size_t targetSize() const { return targetSize_; }

  // A summary of the instructions of this diff, see `Statistics`.
  Statistics statistics() const;

  // Simple getters.
  const std::vector<Instruction>& instructions() const { return instructions_; }
  uint64_t baseSize() const { return baseSize_; }
  uint64_t baseChecksum() const { return baseChecksum_; }

  // Compute the FNV-1a 64 bit hash of `bytes`, which is used as the checksum
  // that ties a diff to the exact base that it was created against. NOTE: This
  // is not a cryptographic hash; it only guards against accidentally applying a
  // diff to the wrong (or to a corrupted) base, not against deliberate
  // tampering.
  static uint64_t checksum(ql::span<const char> bytes);

 private:
  // Return true if `alignment` is a power of two, which the alignment of an
  // `Align` instruction always has to be.
  static bool isPowerOfTwo(uint64_t alignment);

  // Round `offset` up to the next multiple of `alignment`, which has to be a
  // power of two.
  static size_t alignUp(size_t offset, uint64_t alignment);

  // Recompute `targetSize_` from the instructions. Needed after
  // deserialization, where the instructions are not appended one by one.
  void recomputeTargetSize();

  // Throw if `base` is not the base that this diff was created against, see
  // `apply`.
  void checkBase(ql::span<const char> base) const;

  // Throw if any instruction of this diff is invalid for `base` (a copied range
  // that does not lie within `base`), see `apply`.
  void checkInstructions(ql::span<const char> base) const;
};

// The serialization format of a `BinaryDiff`: a header of magic bytes, a format
// version, and the identification of the base, followed by the number of
// instructions and the instructions themselves, each of which is preceded by
// its `InstructionKind`. When reading, verify the magic bytes and the format
// version, and report a truncated or otherwise unreadable input with a
// descriptive message.
//
// This is a separate class so that `BinaryDiff` itself is concerned only with
// the building and the application of a diff. It is used by the `serialize`
// function below, which is the only intended entry point.
class BinaryDiffSerializer {
 private:
  using Copy = BinaryDiff::Copy;
  using Insert = BinaryDiff::Insert;
  using Align = BinaryDiff::Align;
  using Instruction = BinaryDiff::Instruction;

  // The kinds of instruction, as stored in the serialization of a diff.
  enum class InstructionKind : uint8_t { Copy = 0, Insert = 1, Align = 2 };

  // The header that is written at the beginning of the serialization of a
  // diff, to guard against reading data that is not a diff at all, or that was
  // written by an incompatible version of QLever.
  static constexpr std::array<char, 8> magicBytes{'Q', 'L', 'V', 'R',
                                                  'D', 'I', 'F', 'F'};
  static constexpr uint16_t formatVersion = 1;

  // The message that is reported for any input that is not the serialization
  // of a `BinaryDiff`, or that is truncated or otherwise corrupted.
  static constexpr std::string_view notReadableMessage =
      "The given input is not a serialized `ad_utility::BinaryDiff`, or is "
      "corrupted";

 public:
  // Write `diff` to `serializer`, see the class comment for the format.
  template <typename S>
  static void write(S& serializer, const BinaryDiff& diff) {
    serializer << magicBytes;
    serializer << formatVersion;
    serializer << diff.baseSize_;
    serializer << diff.baseChecksum_;
    serializer << static_cast<uint64_t>(diff.instructions_.size());
    for (const auto& instruction : diff.instructions_) {
      writeInstruction(serializer, instruction);
    }
  }

  // Read a `diff` that was written by `write`, and throw with a descriptive
  // message if `serializer` does not hold such a diff.
  template <typename S>
  static void read(S& serializer, BinaryDiff& diff) {
    auto readMagicBytes =
        readOrThrow<std::decay_t<decltype(magicBytes)>>(serializer);
    AD_CONTRACT_CHECK(readMagicBytes == magicBytes, notReadableMessage);
    auto version = readOrThrow<uint16_t>(serializer);
    AD_CONTRACT_CHECK(
        version == formatVersion,
        "The given diff was written by an incompatible version of QLever "
        "(format version ",
        version, ", expected ", formatVersion, ")");
    diff.baseSize_ = readOrThrow<uint64_t>(serializer);
    diff.baseChecksum_ = readOrThrow<uint64_t>(serializer);
    auto numInstructions = readOrThrow<uint64_t>(serializer);
    // NOTE: Deliberately no `reserve`, because `numInstructions` comes from a
    // possibly corrupted input.
    diff.instructions_.clear();
    for (uint64_t i = 0; i < numInstructions; ++i) {
      diff.instructions_.push_back(readInstruction(serializer));
    }
    diff.recomputeTargetSize();
  }

 private:
  // Read a single value of type `T`, and report a truncated or otherwise
  // unreadable input with `notReadableMessage` (instead of with the rather
  // cryptic message of the serializer).
  template <typename T, typename S>
  static T readOrThrow(S& serializer) {
    try {
      T value{};
      serializer >> value;
      return value;
    } catch (const std::exception& exception) {
      AD_THROW(
          absl::StrCat(notReadableMessage, ". Details: ", exception.what()));
    }
  }

  // Write a single instruction, preceded by its `InstructionKind`.
  template <typename S>
  static void writeInstruction(S& serializer, const Instruction& instruction) {
    if (const auto* copy = std::get_if<Copy>(&instruction)) {
      serializer << static_cast<uint8_t>(InstructionKind::Copy);
      serializer << copy->baseOffset_;
      serializer << copy->length_;
    } else if (const auto* insert = std::get_if<Insert>(&instruction)) {
      serializer << static_cast<uint8_t>(InstructionKind::Insert);
      serializer << insert->bytes_;
    } else {
      serializer << static_cast<uint8_t>(InstructionKind::Align);
      serializer << std::get<Align>(instruction).alignment_;
    }
  }

  // Read a single instruction that was written by `writeInstruction`.
  template <typename S>
  static Instruction readInstruction(S& serializer) {
    auto kind = readOrThrow<uint8_t>(serializer);
    if (kind == static_cast<uint8_t>(InstructionKind::Copy)) {
      Copy copy;
      copy.baseOffset_ = readOrThrow<uint64_t>(serializer);
      copy.length_ = readOrThrow<uint64_t>(serializer);
      return copy;
    }
    if (kind == static_cast<uint8_t>(InstructionKind::Insert)) {
      return Insert{readOrThrow<std::vector<char>>(serializer)};
    }
    if (kind == static_cast<uint8_t>(InstructionKind::Align)) {
      auto alignment = readOrThrow<uint64_t>(serializer);
      AD_CONTRACT_CHECK(BinaryDiff::isPowerOfTwo(alignment), notReadableMessage,
                        ". Details: the alignment ", alignment,
                        " is not a power of two");
      return Align{alignment};
    }
    AD_THROW(absl::StrCat(notReadableMessage,
                          ". Details: unknown instruction kind ", kind));
  }
};

// Serialize and deserialize a `BinaryDiff`, see `BinaryDiffSerializer` for the
// format and `util/Serializer/Serializer.h` for the framework.
AD_SERIALIZE_FUNCTION(BinaryDiff) {
  if constexpr (ad_utility::serialization::WriteSerializer<S>) {
    BinaryDiffSerializer::write(serializer, arg);
  } else {
    BinaryDiffSerializer::read(serializer, arg);
  }
}

// _____________________________________________________________________________
template <typename Allocator>
std::vector<char, Allocator> BinaryDiff::apply(ql::span<const char> base,
                                               Allocator allocator) const {
  checkBase(base);
  // Validate all instructions before the first byte is written, so that
  // `targetSize` (which is used for the `reserve` below) cannot be a bogus
  // value that comes from a corrupted diff.
  checkInstructions(base);
  std::vector<char, Allocator> target(std::move(allocator));
  target.reserve(targetSize());
  for (const auto& instruction : instructions_) {
    if (const auto* copy = std::get_if<Copy>(&instruction)) {
      auto copiedBytes = base.subspan(copy->baseOffset_, copy->length_);
      target.insert(target.end(), copiedBytes.begin(), copiedBytes.end());
    } else if (const auto* insert = std::get_if<Insert>(&instruction)) {
      target.insert(target.end(), insert->bytes_.begin(), insert->bytes_.end());
    } else {
      uint64_t alignment = std::get<Align>(instruction).alignment_;
      target.insert(target.end(),
                    alignUp(target.size(), alignment) - target.size(), char{0});
    }
  }
  return target;
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BINARYDIFF_H
