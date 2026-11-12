// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>

#include "../test/util/GTestHelpers.h"
#include "util/CompilerWarnings.h"
#include "util/CompressionUsingZstd/ZstdContext.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Random.h"
#include "util/jthread.h"

using ::testing::HasSubstr;

namespace {
// Compress the `chunks` with a `ZstdContext` of their own, and return a copy of
// the compressed bytes.
std::vector<char> compressChunks(
    ql::span<const ql::span<const std::byte>> chunks) {
  std::vector<char> buffer;
  auto compressed = ad_utility::ZstdContext{}.compressChunks(chunks, buffer);
  return {compressed.begin(), compressed.end()};
}

// Decompress the `compressed` chunks with a `ZstdContext` of their own.
void decompressChunks(ql::span<const char> compressed,
                      ql::span<const ql::span<std::byte>> targets) {
  ad_utility::ZstdContext{}.decompressChunks(compressed, targets);
}
}  // namespace

// _____________________________________________________________________________
TEST(CompressionTest, Basic) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      ZstdWrapper::compress(x.data(), x.size() * sizeof(int));
  auto decomp = ZstdWrapper::decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);
}

// _____________________________________________________________________________
TEST(CompressionTest, DecompressToBuffer) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      ZstdWrapper::compress(x.data(), x.size() * sizeof(int));
  std::vector<int> decomp(4);
  auto numBytesDecompressed = ZstdWrapper::decompressToBuffer<int>(
      comp.data(), comp.size(), decomp.data(), decomp.size() * sizeof(int));
  ASSERT_EQ(x, decomp);
  ASSERT_EQ(4ul * sizeof(int), numBytesDecompressed);
}

// Test that `compressChunks` writes each chunk as a frame of its own, and that
// `decompressChunks` reads each frame back into its own buffer, also when
// all the chunks are empty.
TEST(CompressionTest, compressAndDecompressSeveralChunks) {
  std::vector<std::byte> payloads(800, std::byte{7});
  std::vector<std::byte> datatypes(100, std::byte{3});
  std::array<ql::span<const std::byte>, 2> chunks{payloads, datatypes};
  auto compressed = compressChunks(chunks);
  // Two frames, one after the other.
  auto firstFrameSize =
      ZSTD_findFrameCompressedSize(compressed.data(), compressed.size());
  EXPECT_LT(firstFrameSize, compressed.size());
  EXPECT_EQ(ZstdWrapper::getUncompressedSize(compressed.data(), firstFrameSize),
            payloads.size());
  EXPECT_EQ(
      ZstdWrapper::getUncompressedSize(compressed.data() + firstFrameSize,
                                       compressed.size() - firstFrameSize),
      datatypes.size());

  // Each frame is decompressed into its own buffer.
  std::vector<std::byte> payloadsResult(800);
  std::vector<std::byte> datatypesResult(100);
  std::array<ql::span<std::byte>, 2> buffers{payloadsResult, datatypesResult};
  decompressChunks(compressed, buffers);
  EXPECT_EQ(payloadsResult, payloads);
  EXPECT_EQ(datatypesResult, datatypes);

  // Empty chunks, as for an empty column.
  std::array<ql::span<const std::byte>, 2> emptyChunks{};
  std::array<ql::span<std::byte>, 2> emptyBuffers{};
  decompressChunks(compressChunks(emptyChunks), emptyBuffers);
}

// Test that a single chunk is a plain ZSTD frame, so that the permutations of
// existing indexes, which were written with the plain `compress`, can be read
// with `decompressChunks`, and the other way round.
TEST(CompressionTest, singleChunkIsPlainFrame) {
  std::vector<std::byte> ids(800, std::byte{7});
  std::vector<std::byte> result(ids.size());
  std::array<ql::span<std::byte>, 1> buffers{result};

  auto oldFormat = ZstdWrapper::compress(ids.data(), ids.size());
  decompressChunks(oldFormat, buffers);
  EXPECT_EQ(result, ids);

  std::array<ql::span<const std::byte>, 1> chunks{ids};
  auto newFormat = compressChunks(chunks);
  EXPECT_EQ(newFormat, oldFormat);
}

// Test that a column is not read with the wrong layout: a single frame (a
// column of whole `Id`s) cannot be read into payloads and datatypes, two frames
// (a column of the split layout) cannot be read into a single buffer, and a
// truncated block cannot be read at all.
TEST(CompressionTest, framesMustMatchTheBuffers) {
  std::vector<std::byte> payloads(800, std::byte{7});
  std::vector<std::byte> datatypes(100, std::byte{3});
  std::array<ql::span<const std::byte>, 2> chunks{payloads, datatypes};
  auto twoFrames = compressChunks(chunks);
  auto oneFrame = ZstdWrapper::compress(payloads.data(), payloads.size());

  std::vector<std::byte> payloadsResult(800);
  std::vector<std::byte> datatypesResult(100);
  std::array<ql::span<std::byte>, 2> splitBuffers{payloadsResult,
                                                  datatypesResult};
  std::array<ql::span<std::byte>, 1> singleBuffer{payloadsResult};
  EXPECT_ANY_THROW(decompressChunks(oneFrame, splitBuffers));
  EXPECT_ANY_THROW(decompressChunks(twoFrames, singleBuffer));
  twoFrames.pop_back();
  EXPECT_ANY_THROW(decompressChunks(twoFrames, splitBuffers));
}

// Test the blocks for which no bytes were ever written (blocks of the
// permutations that only exist in the metadata): they are read as empty
// columns, but a column that is not empty is an error and not left unwritten.
TEST(CompressionTest, noBytesAreAnEmptyColumn) {
  std::array<ql::span<std::byte>, 2> emptyBuffers{};
  decompressChunks({}, emptyBuffers);

  std::vector<std::byte> payloads(8);
  std::array<ql::span<std::byte>, 2> buffers{payloads, ql::span<std::byte>{}};
  EXPECT_ANY_THROW(decompressChunks({}, buffers));
}

// _____________________________________________________________________________
TEST(CompressionTest, GetUncompressedSize) {
  std::vector<int> x{1, 2, 3, 4};
  constexpr size_t numBytes = 4 * sizeof(int);
  std::vector<char> comp = ZstdWrapper::compress(x.data(), numBytes);
  EXPECT_EQ(ZstdWrapper::getUncompressedSize(comp.data(), comp.size()),
            numBytes);

  // The empty input also yields a valid frame, with a stored size of zero.
  std::vector<char> emptyComp = ZstdWrapper::compress(x.data(), 0);
  EXPECT_EQ(
      ZstdWrapper::getUncompressedSize(emptyComp.data(), emptyComp.size()), 0u);

  // Data that does not start with a valid ZSTD frame header is rejected, ...
  std::vector<char> garbage(64, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::getUncompressedSize(garbage.data(), garbage.size()),
      HasSubstr("does not start with a valid ZSTD frame header"));
  // ... as is a buffer that is too short to even hold a frame header.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::getUncompressedSize(comp.data(), 1),
      HasSubstr("does not start with a valid ZSTD frame header"));
}

// _____________________________________________________________________________
// A frame that was written without the content size flag (which `ZstdWrapper`
// itself never does, but other ZSTD compressors may) does not store the size of
// its uncompressed data.
TEST(CompressionTest, GetUncompressedSizeOfFrameWithoutContentSize) {
  std::vector<int> x{1, 2, 3, 4};
  constexpr size_t numBytes = 4 * sizeof(int);
  std::vector<char> comp(ZSTD_compressBound(numBytes));

  ZSTD_CCtx* context = ZSTD_createCCtx();
  ASSERT_NE(context, nullptr);
  absl::Cleanup cleanup = [context] { ZSTD_freeCCtx(context); };
  ASSERT_FALSE(
      ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_contentSizeFlag, 0)));
  auto compressedSize =
      ZSTD_compress2(context, comp.data(), comp.size(), x.data(), numBytes);
  ASSERT_FALSE(ZSTD_isError(compressedSize));

  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::getUncompressedSize(comp.data(), compressedSize),
      HasSubstr("does not store that size in its header"));
}

// _____________________________________________________________________________
// Test that the non-throwing functions report the same errors as the throwing
// ones, but via their return value.
TEST(CompressionTest, NonThrowingFunctions) {
  using ::testing::_;
  using ::testing::Field;
  using ::testing::VariantWith;
  using Error = ZstdWrapper::Error;
  // Return a matcher for a `ZstdWrapper::SizeOrError` that holds an `Error`
  // whose message matches the `messageMatcher`.
  auto isError = [](auto messageMatcher) {
    return VariantWith<Error>(Field(&Error::message_, messageMatcher));
  };

  std::vector<int> x{1, 2, 3, 4};
  constexpr size_t numBytes = 4 * sizeof(int);
  std::vector<char> comp = ZstdWrapper::compress(x.data(), numBytes);

  EXPECT_THAT(ZstdWrapper::tryToGetUncompressedSize(comp.data(), comp.size()),
              VariantWith<size_t>(numBytes));
  std::vector<int> decomp(4);
  EXPECT_THAT(ZstdWrapper::tryToDecompressToBuffer(comp.data(), comp.size(),
                                                   decomp.data(), numBytes),
              VariantWith<size_t>(numBytes));
  EXPECT_EQ(x, decomp);

  std::vector<char> garbage(64, 'x');
  EXPECT_THAT(
      ZstdWrapper::tryToGetUncompressedSize(garbage.data(), garbage.size()),
      isError(HasSubstr("does not start with a valid ZSTD frame header")));

  // Decompressing into a buffer that is too small fails, and the throwing
  // version reports the same error.
  auto tooSmall = ZstdWrapper::tryToDecompressToBuffer(
      comp.data(), comp.size(), decomp.data(), numBytes - 1);
  ASSERT_THAT(tooSmall, isError(::testing::Not(::testing::IsEmpty())));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::decompressToBuffer(comp.data(), comp.size(), decomp.data(),
                                      numBytes - 1),
      ::testing::AllOf(
          HasSubstr("error during decompression : "),
          HasSubstr(std::string{std::get<Error>(tooSmall).message_})));

  // Truncated compressed data cannot be decompressed either.
  EXPECT_THAT(ZstdWrapper::tryToDecompressToBuffer(comp.data(), comp.size() - 1,
                                                   decomp.data(), numBytes),
              VariantWith<Error>(_));
}

// Test the compression with a reference and the decompression with the same
// reference, including the error cases and memory overlap of the inputs.
TEST(CompressionTest, CompressWithReference) {
  using ::testing::_;
  using ::testing::VariantWith;
  using Error = ZstdWrapper::Error;
  // Decompress `compressed` with `reference` into a vector of the size that is
  // stored in the frame header, and return that vector.
  auto decompress =
      [](ql::span<const char> reference, ql::span<const char> compressed,
         ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
        auto t = generateLocationTrace(loc);
        std::vector<char> result(ZstdWrapper::getUncompressedSize(
            compressed.data(), compressed.size()));
        EXPECT_THAT(ZstdWrapper::tryToDecompressWithReference(
                        reference, compressed, result.data(), result.size()),
                    VariantWith<size_t>(result.size()));
        return result;
      };

  // A pseudo-random reference, and data that mostly consists of pieces of it.
  std::vector<char> reference(100'000);
  ad_utility::FastRandomIntGenerator<uint64_t> randomGenerator{
      ad_utility::RandomSeed::make(42)};
  ql::ranges::generate(reference, [&randomGenerator]() {
    return static_cast<char>(randomGenerator());
  });
  std::vector<char> data(reference.begin() + 60'000, reference.end());
  // GCC reports false-positive warnings for the following `insert`
  // (`-Wstringop-overread` on GCC 11, `-Warray-bounds` and
  // `-Wstringop-overflow` on GCC 15 and 16): the capacity of `data` equals its
  // size, so the insertion always reallocates, but GCC also analyzes
  // libstdc++'s (unreachable) branch that copies into the existing capacity,
  // and considers that copy out of bounds.
  DISABLE_OVERREAD_WARNINGS
  DISABLE_ARRAY_BOUNDS_WARNINGS
  DISABLE_STRINGOP_OVERFLOW_WARNINGS
  data.insert(data.end(), reference.begin(), reference.begin() + 30'000);
  GCC_REENABLE_WARNINGS
  GCC_REENABLE_WARNINGS
  GCC_REENABLE_WARNINGS
  std::string inserted = "some bytes that are not in the reference";
  data.insert(data.end(), inserted.begin(), inserted.end());

  auto compressed = ZstdWrapper::compressWithReference(reference, data, 19);
  // Random bytes are incompressible without the reference, but with it the
  // data costs only a few bytes.
  EXPECT_LT(compressed.size(), 200u);
  EXPECT_GT(ZstdWrapper::compress(data.data(), data.size(), 19).size(),
            data.size() * 9 / 10);
  EXPECT_EQ(
      ZstdWrapper::getUncompressedSize(compressed.data(), compressed.size()),
      data.size());
  std::vector<char> buffer(data.size());
  EXPECT_THAT(ZstdWrapper::tryToDecompressWithReference(
                  reference, compressed, buffer.data(), buffer.size()),
              VariantWith<size_t>(data.size()));
  EXPECT_EQ(buffer, data);

  // A too small buffer and garbage are reported as errors.
  EXPECT_THAT(ZstdWrapper::tryToDecompressWithReference(reference, compressed,
                                                        buffer.data(), 10),
              VariantWith<Error>(_));
  std::vector<char> garbage(50, 'x');
  EXPECT_THAT(ZstdWrapper::tryToDecompressWithReference(
                  reference, garbage, buffer.data(), buffer.size()),
              VariantWith<Error>(_));

  // Empty reference and empty data work like plain compression.
  auto plain = ZstdWrapper::compressWithReference({}, data, 3);
  EXPECT_EQ(decompress({}, plain), data);
  auto empty = ZstdWrapper::compressWithReference(reference, {}, 3);
  EXPECT_TRUE(decompress(reference, empty).empty());
  auto bothEmpty = ZstdWrapper::compressWithReference({}, {}, 3);
  EXPECT_TRUE(decompress({}, bothEmpty).empty());

  // Data that overlaps with the reference in memory (here: the second part of
  // the reference itself) is still compressed with the reference. Without the
  // internal copy, ZSTD would silently ignore the reference, and the random
  // bytes would be incompressible.
  ql::span<const char> referenceSpan{reference};
  auto overlapping = referenceSpan.subspan(60'000);
  auto compressedOverlap =
      ZstdWrapper::compressWithReference(referenceSpan, overlapping, 19);
  EXPECT_LT(compressedOverlap.size(), 200u);
  EXPECT_THAT(decompress(reference, compressedOverlap),
              ::testing::ElementsAreArray(overlapping));

  // Disjoint data directly before the reference in memory (the other ordering
  // of the overlap check) is compressed with the reference.
  std::vector<char> dataThenReference(40'000 + reference.size());
  ql::ranges::copy(referenceSpan.subspan(60'000), dataThenReference.begin());
  ql::ranges::copy(referenceSpan, dataThenReference.begin() + 40'000);
  ql::span<const char> dataBefore{dataThenReference.data(), 40'000};
  ql::span<const char> referenceAfter{dataThenReference.data() + 40'000,
                                      reference.size()};
  auto compressedBefore =
      ZstdWrapper::compressWithReference(referenceAfter, dataBefore, 19);
  EXPECT_LT(compressedBefore.size(), 200u);
  EXPECT_THAT(decompress(referenceAfter, compressedBefore),
              ::testing::ElementsAreArray(dataBefore));

  // A wrong reference is reported as an error (via the checksum in the frame).
  std::vector<char> wrongReference(reference.rbegin(), reference.rend());
  EXPECT_THAT(ZstdWrapper::tryToDecompressWithReference(
                  wrongReference, compressed, buffer.data(), buffer.size()),
              VariantWith<Error>(_));
}

// Test that `checkError` returns sizes unchanged and throws for error codes.
TEST(CompressionTest, CheckError) {
  // A size is returned unchanged.
  EXPECT_EQ(ZstdWrapper::checkError(0), 0u);
  EXPECT_EQ(ZstdWrapper::checkError(42), 42u);

  // An error code (here: from decompressing garbage) leads to an exception.
  std::vector<char> garbage(50, 'x');
  std::vector<char> buffer(100);
  auto errorCode = ZSTD_decompress(buffer.data(), buffer.size(), garbage.data(),
                                   garbage.size());
  EXPECT_TRUE(ZSTD_isError(errorCode));
  AD_EXPECT_THROW_WITH_MESSAGE(ZstdWrapper::checkError(errorCode),
                               HasSubstr("ZSTD error: "));
}

// Compress and decompress many inputs of different sizes and with different
// compression levels with the same `ZstdContext` and buffer. In particular, a
// small input is compressed after a much larger one, and the result is always
// the same as that of the one-shot `ZstdWrapper::compress`.
TEST(ZstdContext, ReuseForManyInputs) {
  ad_utility::ZstdContext context;
  std::vector<char> buffer;
  ad_utility::FastRandomIntGenerator<uint64_t> randomGenerator{
      ad_utility::RandomSeed::make(42)};
  std::vector<size_t> sizes{0, 1, 1'000'000, 3, 0, 50'000, 7, 1'000'000, 1};
  std::vector<int> levels{1, 3, 19};
  size_t maxBufferSize = 0;
  for (size_t i = 0; i < sizes.size(); ++i) {
    // Use few distinct values, s.t. the data is compressible.
    std::vector<uint64_t> input(sizes[i]);
    ql::ranges::generate(
        input, [&randomGenerator]() { return randomGenerator() % 16; });
    size_t numBytes = input.size() * sizeof(uint64_t);
    int level = levels[i % levels.size()];
    auto compressed = context.compress(input.data(), numBytes, buffer, level);
    EXPECT_EQ(compressed.data(), buffer.data());
    EXPECT_THAT(compressed, ::testing::ElementsAreArray(ZstdWrapper::compress(
                                input.data(), numBytes, level)));

    // The buffer is never shrunk.
    EXPECT_GE(buffer.size(), maxBufferSize);
    maxBufferSize = buffer.size();

    std::vector<uint64_t> result(input.size());
    EXPECT_EQ(
        context.decompressToBuffer(
            compressed, ql::as_writable_bytes(ql::span<uint64_t>{result})),
        numBytes);
    EXPECT_EQ(result, input);
  }
}

// _____________________________________________________________________________
TEST(ZstdContext, DecompressionErrors) {
  ad_utility::ZstdContext context;
  std::vector<char> buffer;
  std::vector<uint64_t> input(100, 7);
  auto compressed =
      context.compress(input.data(), input.size() * sizeof(uint64_t), buffer);
  // The target is too small.
  std::vector<uint64_t> result(input.size() - 1);
  AD_EXPECT_THROW_WITH_MESSAGE(
      context.decompressToBuffer(
          compressed, ql::as_writable_bytes(ql::span<uint64_t>{result})),
      HasSubstr("ZSTD error: "));
  // The data is corrupted.
  std::vector<char> corrupted(compressed.begin(), compressed.end());
  corrupted.at(0) = 'x';
  AD_EXPECT_THROW_WITH_MESSAGE(
      context.decompressToBuffer(
          corrupted, ql::as_writable_bytes(ql::span<uint64_t>{result})),
      HasSubstr("ZSTD error: "));
}

// Test that `ThreadLocalZstdContext` (de)compresses correctly, that each thread
// gets a compression buffer of its own and keeps using it, and that different
// objects have different buffers.
TEST(ThreadLocalZstdContext, PerThreadState) {
  // Compress and decompress with the `threadLocal`, check the round trip, and
  // return the address of the compressed bytes, which are in the buffer of
  // the calling thread.
  auto roundTrip = [](const ad_utility::ThreadLocalZstdContext& threadLocal) {
    std::vector<uint64_t> input(1000, 3);
    size_t numBytes = input.size() * sizeof(uint64_t);
    std::array chunks{
        ql::as_bytes(ql::span<const uint64_t>{input}).subspan(0, numBytes / 2),
        ql::as_bytes(ql::span<const uint64_t>{input}).subspan(numBytes / 2)};
    auto compressedChunks = threadLocal.compressChunks(chunks);
    std::vector<uint64_t> result(input.size());
    auto resultBytes = ql::as_writable_bytes(ql::span<uint64_t>{result});
    std::array targets{resultBytes.subspan(0, numBytes / 2),
                       resultBytes.subspan(numBytes / 2)};
    threadLocal.decompressChunks(compressedChunks, targets);
    EXPECT_EQ(result, input);

    ql::ranges::fill(result, 0);
    auto compressed = threadLocal.compress(input.data(), numBytes);
    EXPECT_EQ(threadLocal.decompressToBuffer(compressed, resultBytes),
              numBytes);
    EXPECT_EQ(result, input);
    return compressed.data();
  };

  ad_utility::ThreadLocalZstdContext threadLocal;
  const char* buffer = roundTrip(threadLocal);
  EXPECT_EQ(roundTrip(threadLocal), buffer);

  std::vector<const char*> bufferOfThreads(4);
  std::vector<ad_utility::JThread> threads;
  for (size_t i = 0; i < bufferOfThreads.size(); ++i) {
    threads.emplace_back([&roundTrip, &threadLocal, &bufferOfThreads, i] {
      bufferOfThreads[i] = roundTrip(threadLocal);
      EXPECT_EQ(roundTrip(threadLocal), bufferOfThreads[i]);
    });
  }
  // Join the threads, s.t. all the `bufferOfThreads` are set.
  threads.clear();
  for (const char* bufferOfThread : bufferOfThreads) {
    EXPECT_NE(bufferOfThread, buffer);
  }

  // Another object has a buffer of its own.
  ad_utility::ThreadLocalZstdContext other;
  EXPECT_NE(roundTrip(other), roundTrip(threadLocal));
}
