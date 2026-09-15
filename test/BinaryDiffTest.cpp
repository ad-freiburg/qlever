// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "./util/GTestHelpers.h"
#include "util/AlignedAllocator.h"
#include "util/BinaryDiff.h"
#include "util/Serializer/ByteBufferSerializer.h"

using ad_utility::BinaryDiff;
using Align = BinaryDiff::Align;
using Copy = BinaryDiff::Copy;
using Insert = BinaryDiff::Insert;
using Instruction = BinaryDiff::Instruction;
using Statistics = BinaryDiff::Statistics;
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
// of `BinaryDiff` refuses to create.
struct RawDiffHeader {
  std::array<char, 8> magicBytes_{'Q', 'L', 'V', 'R', 'D', 'I', 'F', 'F'};
  uint16_t formatVersion_ = 1;
  uint64_t baseSize_ = 0;
  uint64_t baseChecksum_ = 0;
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

// Return a callable that writes a single copy instruction, for `writeRawDiff`.
auto writeRawCopy(uint64_t baseOffset, uint64_t length) {
  return [baseOffset, length](ByteBufferWriteSerializer& writer) {
    writer << static_cast<uint8_t>(0);
    writer << baseOffset;
    writer << length;
  };
}

// Return a callable that writes a single align instruction, for `writeRawDiff`.
auto writeRawAlign(uint64_t alignment) {
  return [alignment](ByteBufferWriteSerializer& writer) {
    writer << static_cast<uint8_t>(2);
    writer << alignment;
  };
}

// Deserialize a diff from `bytes`.
BinaryDiff deserializeDiff(std::vector<char> bytes) {
  ByteBufferReadSerializer reader{std::move(bytes)};
  BinaryDiff diff;
  reader >> diff;
  return diff;
}

// Serialize `diff` and immediately deserialize it again.
BinaryDiff serializeAndDeserialize(const BinaryDiff& diff) {
  ByteBufferWriteSerializer writer;
  writer << diff;
  return deserializeDiff(std::move(writer).data());
}

}  // namespace

// _____________________________________________________________________________
TEST(BinaryDiff, alignmentHasToBeAPowerOfTwo) {
  auto base = toBytes("0123456789");
  BinaryDiff diff{base};
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
    BinaryDiff otherDiff{base};
    otherDiff.addInsert(toBytes("x"));
    otherDiff.addAlign(alignment);
    EXPECT_EQ(otherDiff.targetSize(), alignment);
  }
}

// _____________________________________________________________________________
TEST(BinaryDiff, checksum) {
  // The FNV-1a 64 offset basis, which is the checksum of the empty input.
  EXPECT_EQ(BinaryDiff::checksum({}), 0xcbf29ce484222325ULL);
  // The checksum is deterministic, and it differs for different inputs.
  auto hello = toBytes("hello");
  EXPECT_EQ(BinaryDiff::checksum(hello), BinaryDiff::checksum(hello));
  EXPECT_NE(BinaryDiff::checksum(hello),
            BinaryDiff::checksum(toBytes("hellp")));
  EXPECT_NE(BinaryDiff::checksum(hello),
            BinaryDiff::checksum(toBytes("olleh")));
  EXPECT_NE(BinaryDiff::checksum(hello), BinaryDiff::checksum(toBytes("hell")));
  // Bytes with the highest bit set are also covered (`char` may be signed).
  std::vector<char> highBitSet{'\x80'};
  std::vector<char> zeroByte{'\0'};
  EXPECT_NE(BinaryDiff::checksum(highBitSet), BinaryDiff::checksum(zeroByte));
}

// _____________________________________________________________________________
TEST(BinaryDiff, emptyDiff) {
  auto base = toBytes("0123456789");
  BinaryDiff diff{base};
  EXPECT_EQ(diff.baseSize(), base.size());
  EXPECT_EQ(diff.baseChecksum(), BinaryDiff::checksum(base));
  EXPECT_THAT(diff.instructions(), IsEmpty());
  EXPECT_EQ(diff.targetSize(), 0U);
  EXPECT_EQ(diff.statistics(), statistics(0, 0, 0, 0, 0));
  EXPECT_THAT(diff.apply(base), IsEmpty());
}

// _____________________________________________________________________________
TEST(BinaryDiff, roundTripWithoutAlignment) {
  auto base = toBytes("0123456789");
  BinaryDiff diff{base};
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
TEST(BinaryDiff, roundTripWithAlignment) {
  // A base that consists of three 16-byte blocks.
  std::string blockA(16, 'A');
  std::string blockB(16, 'B');
  std::string blockC(16, 'C');
  auto base = toBytes(blockA + blockB + blockC);
  BinaryDiff diff{base};
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
TEST(BinaryDiff, alignmentMayChangeWithinADiff) {
  auto base = toBytes(std::string(32, 'x'));
  BinaryDiff diff{base};
  diff.addInsert(toBytes("ab"));
  // Align to 8, then to 16, and finally to 1 (which never pads).
  diff.addAlign(8);
  diff.addInsert(toBytes("cde"));
  diff.addAlign(16);
  diff.addInsert(toBytes("f"));
  diff.addAlign(1);
  diff.addInsert(toBytes("g"));
  EXPECT_THAT(diff.instructions(),
              ElementsAre(insertInstruction("ab"), alignInstruction(8),
                          insertInstruction("cde"), alignInstruction(16),
                          insertInstruction("f"), insertInstruction("g")));
  std::string expected =
      "ab" + std::string(6, '\0') + "cde" + std::string(5, '\0') + "f" + "g";
  ASSERT_EQ(expected.size(), 18U);
  EXPECT_EQ(diff.targetSize(), 18U);
  EXPECT_EQ(toString(diff.apply(base)), expected);
}

// _____________________________________________________________________________
TEST(BinaryDiff, applyUsesTheGivenAllocator) {
  auto base = toBytes(std::string(64, 'x'));
  BinaryDiff diff{base};
  diff.addCopy(0, 64);
  using Allocator =
      ad_utility::AlignedAllocator<char, std::allocator<char>, 64>;
  std::vector<char, Allocator> target = diff.apply<Allocator>(base);
  EXPECT_EQ(toString(target), std::string(64, 'x'));
  EXPECT_EQ(reinterpret_cast<uintptr_t>(target.data()) % 64, 0U);
}

// _____________________________________________________________________________
TEST(BinaryDiff, mergeOfAdjacentCopies) {
  auto base = toBytes("0123456789");
  {
    // Two directly adjacent copies are merged.
    BinaryDiff diff{base};
    diff.addCopy(0, 3);
    diff.addCopy(3, 4);
    EXPECT_THAT(diff.instructions(), ElementsAre(copyInstruction(0, 7)));
    EXPECT_EQ(diff.targetSize(), 7U);
    EXPECT_EQ(toString(diff.apply(base)), "0123456");
  }
  {
    // A gap between the two copies prevents the merge.
    BinaryDiff diff{base};
    diff.addCopy(0, 3);
    diff.addCopy(4, 2);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), copyInstruction(4, 2)));
    EXPECT_EQ(toString(diff.apply(base)), "01245");
  }
  {
    // An insert between the two copies prevents the merge.
    BinaryDiff diff{base};
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
    BinaryDiff diff{base};
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
TEST(BinaryDiff, redundantAlignmentsAreNotStored) {
  auto base = toBytes("0123456789");
  {
    // An alignment at the very beginning of a diff is always a no-op, because
    // the empty target has every alignment.
    BinaryDiff diff{base};
    diff.addAlign(16);
    EXPECT_THAT(diff.instructions(), IsEmpty());
    EXPECT_EQ(diff.targetSize(), 0U);
  }
  {
    // An alignment that the target already has is dropped, so that the copies
    // around it are still merged.
    BinaryDiff diff{base};
    diff.addCopy(0, 8);
    diff.addAlign(8);
    diff.addCopy(8, 2);
    EXPECT_THAT(diff.instructions(), ElementsAre(copyInstruction(0, 10)));
    EXPECT_EQ(toString(diff.apply(base)), "0123456789");
  }
  {
    // Consecutive alignments only pad once.
    BinaryDiff diff{base};
    diff.addCopy(0, 3);
    diff.addAlign(8);
    diff.addAlign(4);
    diff.addAlign(8);
    EXPECT_THAT(diff.instructions(),
                ElementsAre(copyInstruction(0, 3), alignInstruction(8)));
    EXPECT_EQ(diff.targetSize(), 8U);
  }
}

// _____________________________________________________________________________
TEST(BinaryDiff, addCopyChecksItsArguments) {
  auto base = toBytes(std::string(32, 'x'));
  BinaryDiff diff{base};
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
TEST(BinaryDiff, applyToTheWrongBase) {
  auto base = toBytes("0123456789");
  BinaryDiff diff{base};
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
TEST(BinaryDiff, applyChecksTheInstructions) {
  auto base = toBytes(std::string(32, 'x'));
  RawDiffHeader header{};
  header.baseSize_ = base.size();
  header.baseChecksum_ = BinaryDiff::checksum(base);
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
TEST(BinaryDiff, serializationRoundTrip) {
  std::string blockA(16, 'A');
  std::string blockB(16, 'B');
  auto base = toBytes(blockA + blockB);
  BinaryDiff diff{base};
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
  BinaryDiff emptyDiff{base};
  auto deserializedEmptyDiff = serializeAndDeserialize(emptyDiff);
  EXPECT_THAT(deserializedEmptyDiff.instructions(), IsEmpty());
  EXPECT_EQ(deserializedEmptyDiff.targetSize(), 0U);
  EXPECT_EQ(deserializedEmptyDiff.baseChecksum(), emptyDiff.baseChecksum());

  // A default-constructed diff is the diff of an empty base into an empty
  // target.
  auto deserializedDefaultDiff = serializeAndDeserialize(BinaryDiff{});
  EXPECT_EQ(deserializedDefaultDiff.baseSize(), 0U);
  EXPECT_EQ(deserializedDefaultDiff.baseChecksum(), BinaryDiff::checksum({}));
  EXPECT_THAT(deserializedDefaultDiff.apply({}), IsEmpty());
}

// _____________________________________________________________________________
TEST(BinaryDiff, serializationRoundTripWithAnAlignedSerializer) {
  // A diff can also be written to and read from a serializer that inserts
  // alignment padding for trivially serializable types.
  auto base = toBytes(std::string(32, 'A'));
  BinaryDiff diff{base};
  diff.addCopy(16, 16);
  diff.addInsert(toBytes("insert"));
  diff.addAlign(16);
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << diff;
  ad_utility::serialization::AlignedByteBufferReadSerializer reader{
      std::move(writer).data()};
  BinaryDiff deserialized;
  reader >> deserialized;
  EXPECT_EQ(deserialized.baseSize(), diff.baseSize());
  EXPECT_EQ(deserialized.baseChecksum(), diff.baseChecksum());
  EXPECT_EQ(deserialized.instructions(), diff.instructions());
  EXPECT_EQ(toString(deserialized.apply(base)), toString(diff.apply(base)));
}

// _____________________________________________________________________________
TEST(BinaryDiff, deserializationChecksTheInput) {
  auto base = toBytes("0123456789");
  BinaryDiff diff{base};
  diff.addCopy(0, 10);
  ByteBufferWriteSerializer writer;
  writer << diff;
  auto serialized = std::move(writer).data();
  const std::string notReadableMessage =
      "not a serialized `ad_utility::BinaryDiff`, or is corrupted";

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

  // An unknown instruction kind.
  auto writeUnknownKind = [](ByteBufferWriteSerializer& rawWriter) {
    rawWriter << static_cast<uint8_t>(7);
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeDiff(writeRawDiff(RawDiffHeader{}, 1, writeUnknownKind)),
      HasSubstr("unknown instruction kind 7"));

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
