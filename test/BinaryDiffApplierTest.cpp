// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/BinaryDiffApplier.h"
#include "util/Serializer/ByteBufferSerializer.h"

using ad_utility::BinaryDiffApplier;
using Align = BinaryDiffApplier::Align;
using Copy = BinaryDiffApplier::Copy;
using Insert = BinaryDiffApplier::Insert;
using Instruction = BinaryDiffApplier::Instruction;
using Statistics = BinaryDiffApplier::Statistics;
using ad_utility::serialization::ByteBufferReadSerializer;
using ad_utility::serialization::ByteBufferWriteSerializer;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;

namespace {

// Convert `bytes` to a `std::string`, so that a mismatch of two buffers is
// reported readably by GoogleTest.
template <typename Bytes>
std::string toString(const Bytes& bytes) {
  return std::string{bytes.begin(), bytes.end()};
}

// Convert `string` to a buffer of bytes.
std::vector<char> toBytes(std::string_view string) {
  return std::vector<char>{string.begin(), string.end()};
}

// Convert a checksum to its hexadecimal representation, so that a mismatch is
// reported readably by GoogleTest.
std::string toHex(const BinaryDiffApplier::Checksum& checksum) {
  return absl::BytesToHexString(
      std::string_view{checksum.data(), checksum.size()});
}

// Shorthands for the expected instructions and statistics of a diff. They are
// functions (and not braced initializers) because the commas of a braced
// initializer would be parsed as argument separators of the surrounding test
// macro.
Instruction copyInstruction(uint64_t baseOffset, uint64_t length) {
  return Copy{baseOffset, length};
}
Instruction insertInstruction(std::string_view bytes) {
  return Insert{toBytes(bytes)};
}
Instruction alignInstruction(uint64_t alignment) { return Align{alignment}; }
Statistics statistics(size_t numCopyInstructions, size_t numInsertInstructions,
                      size_t numAlignInstructions, size_t numCopiedBytes,
                      size_t numInsertedBytes) {
  return {numCopyInstructions, numInsertInstructions, numAlignInstructions,
          numCopiedBytes, numInsertedBytes};
}

// The header of the serialization of a diff, so that the tests below can write
// diffs "by hand" (see `writeRawDiff`), in particular diffs that the interface
// of `BinaryDiffApplier` refuses to create.
struct RawDiffHeader {
  std::array<char, 8> magicBytes_{'Q', 'L', 'V', 'R', 'D', 'I', 'F', 'F'};
  uint16_t formatVersion_ = 1;
  uint64_t baseSize_ = 0;
  BinaryDiffApplier::Checksum baseChecksum_{};
};

// Write the serialization of a diff that consists of the given `header` and of
// the `numInstructions` instructions that `writeInstructions` writes.
template <typename WriteInstructions>
std::vector<char> writeRawDiff(const RawDiffHeader& header,
                               uint64_t numInstructions,
                               WriteInstructions writeInstructions) {
  ByteBufferWriteSerializer writer;
  writer << header.magicBytes_;
  writer << header.formatVersion_;
  writer << header.baseSize_;
  writer << header.baseChecksum_;
  writer << numInstructions;
  writeInstructions(writer);
  return std::move(writer).data();
}

// The index of an instruction in the `BinaryDiffApplier::Instruction` variant,
// which is what the generic serialization of a `std::variant` writes to
// identify the instruction (see `util/Serializer/SerializeVariant.h`).
constexpr uint64_t copyIndex = 0;
constexpr uint64_t alignIndex = 2;

// Return a callable that writes a single copy instruction, for `writeRawDiff`.
auto writeRawCopy(uint64_t baseOffset, uint64_t length) {
  return [baseOffset, length](ByteBufferWriteSerializer& writer) {
    writer << copyIndex;
    writer << baseOffset;
    writer << length;
  };
}

// Return a callable that writes a single align instruction, for `writeRawDiff`.
auto writeRawAlign(uint64_t alignment) {
  return [alignment](ByteBufferWriteSerializer& writer) {
    writer << alignIndex;
    writer << alignment;
  };
}

// Deserialize a diff from `bytes`.
BinaryDiffApplier deserializeDiff(std::vector<char> bytes) {
  ByteBufferReadSerializer reader{std::move(bytes)};
  BinaryDiffApplier diff;
  reader >> diff;
  return diff;
}

// Serialize `diff` and immediately deserialize it again.
BinaryDiffApplier serializeAndDeserialize(const BinaryDiffApplier& diff) {
  ByteBufferWriteSerializer writer;
  writer << diff;
  return deserializeDiff(std::move(writer).data());
}

}  // namespace

// _____________________________________________________________________________
TEST(BinaryDiffApplier, alignmentHasToBeAPowerOfTwo) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  // The alignment only takes effect once the target is not empty, because an
  // empty target trivially has every alignment.
  diff.addInsert(toBytes("x"));
  for (uint64_t alignment : {uint64_t{0}, uint64_t{3}, uint64_t{6},
                             uint64_t{100}, (uint64_t{1} << 63) + 1}) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        diff.addAlign(alignment),
        HasSubstr("alignment of an `Align` instruction has to be a power of "
                  "two"));
  }
  for (uint64_t alignment : {uint64_t{1}, uint64_t{2}, uint64_t{16}}) {
    BinaryDiffApplier otherDiff{base};
    otherDiff.addInsert(toBytes("x"));
    otherDiff.addAlign(alignment);
    EXPECT_EQ(otherDiff.targetSize(), alignment);
  }
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, checksum) {
  // The checksum is the SHA-256 digest, here for the empty input.
  EXPECT_EQ(toHex(BinaryDiffApplier::checksum({})),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(toHex(BinaryDiffApplier::checksum(toBytes("abc"))),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  // The checksum is deterministic, and it differs for different inputs.
  auto hello = toBytes("hello");
  EXPECT_EQ(BinaryDiffApplier::checksum(hello),
            BinaryDiffApplier::checksum(hello));
  EXPECT_NE(BinaryDiffApplier::checksum(hello),
            BinaryDiffApplier::checksum(toBytes("hellp")));
  EXPECT_NE(BinaryDiffApplier::checksum(hello),
            BinaryDiffApplier::checksum(toBytes("olleh")));
  EXPECT_NE(BinaryDiffApplier::checksum(hello),
            BinaryDiffApplier::checksum(toBytes("hell")));
  // Bytes with the highest bit set are also covered (`char` may be signed).
  std::vector<char> highBitSet{'\x80'};
  std::vector<char> zeroByte{'\0'};
  EXPECT_NE(BinaryDiffApplier::checksum(highBitSet),
            BinaryDiffApplier::checksum(zeroByte));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, emptyDiff) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  EXPECT_EQ(diff.baseSize(), base.size());
  EXPECT_EQ(diff.baseChecksum(), BinaryDiffApplier::checksum(base));
  EXPECT_THAT(diff.instructions(), IsEmpty());
  EXPECT_EQ(diff.targetSize(), 0U);
  EXPECT_EQ(diff.statistics(), statistics(0, 0, 0, 0, 0));
  EXPECT_THAT(diff.apply(base), IsEmpty());
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, roundTripWithoutAlignment) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  diff.addCopy(0, 3);
  diff.addInsert(toBytes("XY"));
  diff.addCopy(5, 2);
  diff.addInsert(toBytes("!"));
  // Without `Align` instructions, no padding is inserted between the
  // instructions.
  EXPECT_THAT(diff.instructions(),
              ElementsAre(copyInstruction(0, 3), insertInstruction("XY"),
                          copyInstruction(5, 2), insertInstruction("!")));
  EXPECT_EQ(diff.targetSize(), 8U);
  EXPECT_EQ(toString(diff.apply(base)), "012XY56!");
  EXPECT_EQ(diff.statistics(), statistics(2, 2, 0, 5, 3));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, addInsertFromASpan) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  // A buffer that is not a `std::vector<char>`, so that the `ql::span`
  // overload of `addInsert` is chosen.
  std::string inserted = "XY";
  diff.addCopy(0, 3);
  diff.addInsert(ql::span<const char>{inserted.data(), inserted.size()});
  // The inserted bytes become part of the diff, so overwriting the buffer that
  // they were taken from does not change the diff or its target.
  inserted = "ZZ";
  EXPECT_THAT(diff.instructions(),
              ElementsAre(copyInstruction(0, 3), insertInstruction("XY")));
  EXPECT_EQ(diff.targetSize(), 5U);
  EXPECT_EQ(toString(diff.apply(base)), "012XY");
  EXPECT_EQ(diff.statistics(), statistics(1, 1, 0, 3, 2));

  // The two overloads of `addInsert` are interchangeable.
  BinaryDiffApplier diffFromVector{base};
  diffFromVector.addCopy(0, 3);
  diffFromVector.addInsert(toBytes("XY"));
  EXPECT_EQ(diff.instructions(), diffFromVector.instructions());
  EXPECT_EQ(diff.targetSize(), diffFromVector.targetSize());
  EXPECT_EQ(diff.statistics(), diffFromVector.statistics());
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, roundTripWithAlignment) {
  // A base that consists of three 16-byte blocks.
  std::string blockA(16, 'A');
  std::string blockB(16, 'B');
  std::string blockC(16, 'C');
  auto base = toBytes(blockA + blockB + blockC);
  BinaryDiffApplier diff{base};
  diff.addAlign(16);
  diff.addCopy(32, 16);
  diff.addAlign(16);
  diff.addInsert(toBytes("xyz"));
  diff.addAlign(16);
  diff.addCopy(0, 16);
  // The leading alignment and the alignment after the copy of `blockC` are
  // no-ops, because the target already has the required alignment.
  EXPECT_THAT(diff.instructions(),
              ElementsAre(copyInstruction(32, 16), insertInstruction("xyz"),
                          alignInstruction(16), copyInstruction(0, 16)));
  // The insert of three bytes is padded with 13 zeros, so that the copy of
  // `blockA` again lands at an offset that is a multiple of 16.
  std::string expected = blockC + "xyz" + std::string(13, '\0') + blockA;
  ASSERT_EQ(expected.size(), 48U);
  EXPECT_EQ(diff.targetSize(), 48U);
  EXPECT_EQ(toString(diff.apply(base)), expected);
  EXPECT_EQ(diff.statistics(), statistics(2, 1, 1, 32, 3));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, alignmentMayChangeWithinADiff) {
  auto base = toBytes(std::string(32, 'x'));
  BinaryDiffApplier diff{base};
  diff.addInsert(toBytes("ab"));
  // Align to 8, then to 16, and finally to 1 (which never pads).
  diff.addAlign(8);
  diff.addInsert(toBytes("cde"));
  diff.addAlign(16);
  diff.addInsert(toBytes("f"));
  diff.addAlign(1);
  diff.addInsert(toBytes("g"));
  // The alignment to one is a no-op, so the inserts around it are merged.
  EXPECT_THAT(diff.instructions(),
              ElementsAre(insertInstruction("ab"), alignInstruction(8),
                          insertInstruction("cde"), alignInstruction(16),
                          insertInstruction("fg")));
  std::string expected =
      "ab" + std::string(6, '\0') + "cde" + std::string(5, '\0') + "f" + "g";
  ASSERT_EQ(expected.size(), 18U);
  EXPECT_EQ(diff.targetSize(), 18U);
  EXPECT_EQ(toString(diff.apply(base)), expected);
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, applyUsesTheGivenAllocator) {
  auto base = toBytes(std::string(64, 'x'));
  BinaryDiffApplier diff{base};
  diff.addCopy(0, 64);
  using Allocator =
      ad_utility::AlignedAllocator<char, std::allocator<char>, 64>;
  std::vector<char, Allocator> target = diff.apply<Allocator>(base);
  EXPECT_EQ(toString(target), std::string(64, 'x'));
  EXPECT_EQ(reinterpret_cast<uintptr_t>(target.data()) % 64, 0U);
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, applyToAGivenTarget) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  diff.addCopy(0, 3);
  diff.addAlign(8);
  diff.addInsert(toBytes("XY"));
  const std::string expected = "012" + std::string(5, '\0') + "XY";

  // The target is filled exactly as the target that the other overload of
  // `apply` returns, and its previous contents are completely overwritten
  // (also where the diff only pads with zeros).
  std::vector<char> target(diff.targetSize(), 'u');
  diff.apply(base, target);
  EXPECT_EQ(toString(target), expected);
  EXPECT_EQ(toString(diff.apply(base)), expected);

  // A target of the wrong size is rejected.
  for (size_t size : {diff.targetSize() - 1, diff.targetSize() + 1}) {
    std::vector<char> targetOfWrongSize(size, 'u');
    AD_EXPECT_THROW_WITH_MESSAGE(
        diff.apply(base, targetOfWrongSize),
        HasSubstr(absl::StrCat("has to have exactly ", diff.targetSize(),
                               " bytes, but has ", size)));
  }

  // The base is checked, just as for the other overload of `apply`.
  std::vector<char> target2(diff.targetSize(), 'u');
  AD_EXPECT_THROW_WITH_MESSAGE(diff.apply(toBytes("012345678X"), target2),
                               HasSubstr("created against a different base"));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, mergeOfAdjacentCopies) {
  auto base = toBytes("0123456789");
  {
    // Two directly adjacent copies are merged.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addCopy(3, 4);
    EXPECT_THAT(diff.instructions(), ElementsAre(copyInstruction(0, 7)));
    EXPECT_EQ(diff.targetSize(), 7U);
    EXPECT_EQ(toString(diff.apply(base)), "0123456");
  }
  {
    // A gap between the two copies prevents the merge.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addCopy(4, 2);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), copyInstruction(4, 2)));
    EXPECT_EQ(toString(diff.apply(base)), "01245");
  }
  {
    // An insert between the two copies prevents the merge.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addInsert(toBytes("-"));
    diff.addCopy(3, 3);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), insertInstruction("-"),
                            copyInstruction(3, 3)));
    EXPECT_EQ(toString(diff.apply(base)), "012-345");
  }
  {
    // An alignment that actually pads also prevents the merge.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addAlign(4);
    diff.addCopy(3, 3);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), alignInstruction(4),
                            copyInstruction(3, 3)));
    EXPECT_EQ(toString(diff.apply(base)), "012" + std::string(1, '\0') + "345");
  }
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, mergeOfAdjacentInserts) {
  auto base = toBytes("0123456789");
  {
    // Two directly adjacent inserts are merged.
    BinaryDiffApplier diff{base};
    diff.addInsert(toBytes("XY"));
    diff.addInsert(toBytes("Z"));
    EXPECT_THAT(diff.instructions(), ElementsAre(insertInstruction("XYZ")));
    EXPECT_EQ(diff.targetSize(), 3U);
    EXPECT_EQ(diff.statistics(), statistics(0, 1, 0, 0, 3));
    EXPECT_EQ(toString(diff.apply(base)), "XYZ");
  }
  {
    // A copy between the two inserts prevents the merge.
    BinaryDiffApplier diff{base};
    diff.addInsert(toBytes("XY"));
    diff.addCopy(0, 2);
    diff.addInsert(toBytes("Z"));
    EXPECT_THAT(diff.instructions(),
                ElementsAre(insertInstruction("XY"), copyInstruction(0, 2),
                            insertInstruction("Z")));
    EXPECT_EQ(toString(diff.apply(base)), "XY01Z");
  }
  {
    // An alignment that actually pads also prevents the merge.
    BinaryDiffApplier diff{base};
    diff.addInsert(toBytes("XY"));
    diff.addAlign(4);
    diff.addInsert(toBytes("Z"));
    EXPECT_THAT(diff.instructions(),
                ElementsAre(insertInstruction("XY"), alignInstruction(4),
                            insertInstruction("Z")));
    EXPECT_EQ(toString(diff.apply(base)), "XY" + std::string(2, '\0') + "Z");
  }
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, redundantAlignmentsAreNotStored) {
  auto base = toBytes("0123456789");
  {
    // An alignment at the very beginning of a diff is always a no-op, because
    // the empty target has every alignment.
    BinaryDiffApplier diff{base};
    diff.addAlign(16);
    EXPECT_THAT(diff.instructions(), IsEmpty());
    EXPECT_EQ(diff.targetSize(), 0U);
  }
  {
    // An alignment that the target already has is dropped, so that the copies
    // around it are still merged.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 8);
    diff.addAlign(8);
    diff.addCopy(8, 2);
    EXPECT_THAT(diff.instructions(), ElementsAre(copyInstruction(0, 10)));
    EXPECT_EQ(toString(diff.apply(base)), "0123456789");
  }
  {
    // Consecutive alignments only pad once, because each of them replaces the
    // previous one.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addAlign(8);
    diff.addAlign(4);
    diff.addAlign(8);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), alignInstruction(8)));
    EXPECT_EQ(diff.targetSize(), 8U);
  }
  {
    // An alignment also replaces a previous alignment that is stronger,
    // because nothing was written in between, so the region that the previous
    // alignment aligned is empty.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addAlign(8);
    diff.addAlign(4);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), alignInstruction(4)));
    EXPECT_EQ(diff.targetSize(), 4U);
    EXPECT_EQ(toString(diff.apply(base)), "012" + std::string(1, '\0'));
  }
  {
    // An alignment of one is always a no-op, so it removes a previous
    // alignment without adding an instruction of its own.
    BinaryDiffApplier diff{base};
    diff.addCopy(0, 3);
    diff.addAlign(8);
    diff.addAlign(1);
    EXPECT_THAT(diff.instructions(), ElementsAre(copyInstruction(0, 3)));
    EXPECT_EQ(diff.targetSize(), 3U);
    EXPECT_EQ(toString(diff.apply(base)), "012");
  }
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, addCopyChecksItsArguments) {
  auto base = toBytes(std::string(32, 'x'));
  BinaryDiffApplier diff{base};
  AD_EXPECT_THROW_WITH_MESSAGE(
      diff.addCopy(16, 32),
      HasSubstr("copied range [16, 48) does not lie within the base of size "
                "32"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      diff.addCopy(48, 0),
      HasSubstr("copied range [48, 48) does not lie within the base of size "
                "32"));
  EXPECT_THAT(diff.instructions(), IsEmpty());
  // An offset that is not aligned in any way is fine, because the alignment is
  // the business of the `Align` instruction.
  diff.addCopy(8, 8);
  EXPECT_EQ(toString(diff.apply(base)), std::string(8, 'x'));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, applyToTheWrongBase) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  diff.addCopy(0, 10);
  const std::string expectedMessage =
      "created against a different base (the size or the checksum of the base "
      "does not match)";
  // A base of a different size.
  AD_EXPECT_THROW_WITH_MESSAGE(diff.apply(toBytes("012345678")),
                               HasSubstr(expectedMessage));
  // A base of the same size, but with different contents.
  AD_EXPECT_THROW_WITH_MESSAGE(diff.apply(toBytes("012345678X")),
                               HasSubstr(expectedMessage));
  // The correct base works.
  EXPECT_EQ(toString(diff.apply(base)), "0123456789");
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, applyChecksTheInstructions) {
  auto base = toBytes(std::string(32, 'x'));
  RawDiffHeader header{};
  header.baseSize_ = base.size();
  header.baseChecksum_ = BinaryDiffApplier::checksum(base);
  // A copy whose range does not lie within the base.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(header, 1, writeRawCopy(16, 32)))
          .apply(base),
      HasSubstr("contains an invalid instruction"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(header, 1, writeRawCopy(48, 0))).apply(base),
      HasSubstr("contains an invalid instruction"));
  // A valid copy of the same, hand-written shape works.
  auto validDiff =
      deserializeDiff(writeRawDiff(header, 1, writeRawCopy(16, 16)));
  EXPECT_EQ(toString(validDiff.apply(base)), std::string(16, 'x'));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, serializationRoundTrip) {
  std::string blockA(16, 'A');
  std::string blockB(16, 'B');
  auto base = toBytes(blockA + blockB);
  BinaryDiffApplier diff{base};
  diff.addCopy(16, 16);
  diff.addInsert(toBytes("insert"));
  diff.addAlign(16);
  diff.addCopy(0, 16);
  auto deserialized = serializeAndDeserialize(diff);
  EXPECT_EQ(deserialized.baseSize(), diff.baseSize());
  EXPECT_EQ(deserialized.baseChecksum(), diff.baseChecksum());
  EXPECT_EQ(deserialized.instructions(), diff.instructions());
  EXPECT_EQ(deserialized.targetSize(), diff.targetSize());
  EXPECT_EQ(toString(deserialized.apply(base)), toString(diff.apply(base)));

  // A diff without instructions also survives the round trip.
  BinaryDiffApplier emptyDiff{base};
  auto deserializedEmptyDiff = serializeAndDeserialize(emptyDiff);
  EXPECT_THAT(deserializedEmptyDiff.instructions(), IsEmpty());
  EXPECT_EQ(deserializedEmptyDiff.targetSize(), 0U);
  EXPECT_EQ(deserializedEmptyDiff.baseChecksum(), emptyDiff.baseChecksum());

  // A default-constructed diff is the diff of an empty base into an empty
  // target.
  auto deserializedDefaultDiff = serializeAndDeserialize(BinaryDiffApplier{});
  EXPECT_EQ(deserializedDefaultDiff.baseSize(), 0U);
  EXPECT_EQ(deserializedDefaultDiff.baseChecksum(),
            BinaryDiffApplier::checksum({}));
  EXPECT_THAT(deserializedDefaultDiff.apply({}), IsEmpty());
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, serializationRoundTripWithAnAlignedSerializer) {
  // A diff can also be written to and read from a serializer that inserts
  // alignment padding for trivially serializable types.
  auto base = toBytes(std::string(32, 'A'));
  BinaryDiffApplier diff{base};
  diff.addCopy(16, 16);
  diff.addInsert(toBytes("insert"));
  diff.addAlign(16);
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << diff;
  ad_utility::serialization::AlignedByteBufferReadSerializer reader{
      std::move(writer).data()};
  BinaryDiffApplier deserialized;
  reader >> deserialized;
  EXPECT_EQ(deserialized.baseSize(), diff.baseSize());
  EXPECT_EQ(deserialized.baseChecksum(), diff.baseChecksum());
  EXPECT_EQ(deserialized.instructions(), diff.instructions());
  EXPECT_EQ(toString(deserialized.apply(base)), toString(diff.apply(base)));
}

// _____________________________________________________________________________
TEST(BinaryDiffApplier, deserializationChecksTheInput) {
  auto base = toBytes("0123456789");
  BinaryDiffApplier diff{base};
  diff.addCopy(0, 10);
  ByteBufferWriteSerializer writer;
  writer << diff;
  auto serialized = std::move(writer).data();
  const std::string notReadableMessage =
      "not a serialized `ad_utility::BinaryDiffApplier`, or is corrupted";

  // Wrong magic bytes.
  auto wrongMagicBytes = serialized;
  wrongMagicBytes.at(4) = 'X';
  AD_EXPECT_THROW_WITH_MESSAGE(deserializeDiff(wrongMagicBytes),
                               HasSubstr(notReadableMessage));

  // A wrong format version.
  RawDiffHeader header{};
  header.formatVersion_ = 42;
  auto writeNoInstructions = [](ByteBufferWriteSerializer&) {};
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(header, 0, writeNoInstructions)),
      HasSubstr("written by an incompatible version of QLever (format version "
                "42, expected 1)"));

  // An unknown instruction kind, which is an out of range index of the
  // `Instruction` variant.
  auto writeUnknownKind = [](ByteBufferWriteSerializer& rawWriter) {
    rawWriter << uint64_t{7};
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(RawDiffHeader{}, 1, writeUnknownKind)),
      ::testing::AllOf(HasSubstr(notReadableMessage),
                       HasSubstr("out of range index 7")));

  // Truncated input, both inside the header and inside the instructions.
  for (size_t size :
       {size_t{0}, size_t{5}, size_t{20}, serialized.size() - 1}) {
    std::vector<char> truncated{serialized.begin(), serialized.begin() + size};
    AD_EXPECT_THROW_WITH_MESSAGE(deserializeDiff(truncated),
                                 HasSubstr(notReadableMessage));
  }

  // An alignment that is not a power of two.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(RawDiffHeader{}, 1, writeRawAlign(24))),
      HasSubstr("the alignment 24 is not a power of two"));

  // A diff that announces more instructions than it contains.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(RawDiffHeader{}, 3, writeNoInstructions)),
      HasSubstr(notReadableMessage));
}
