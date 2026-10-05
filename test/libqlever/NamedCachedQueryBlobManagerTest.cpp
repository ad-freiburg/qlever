// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gmock/gmock.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "../util/GTestHelpers.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/json.h"

using namespace qlever;
using namespace testing;

namespace {
using Manager = NamedCachedQueryBlobManager;
using ErrorType = Manager::BlobErrorType;
using ad_utility::VocabularyType;

// A `ql::pmr::memory_resource` that counts the allocations routed through it,
// used to verify that a caller-provided allocator is actually used for the
// decompressed blob buffer.
class CountingMemoryResource : public ql::pmr::memory_resource {
 public:
  size_t numAllocations_ = 0;
  size_t bytesAllocated_ = 0;

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    ++numAllocations_;
    bytesAllocated_ += bytes;
    return ql::pmr::new_delete_resource()->allocate(bytes, alignment);
  }
  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    ql::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
  }
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

// The serializer that reads a decompressed blob (see
// `Manager::tryToSkipAndVerifyBlobHeader`).
using BlobReader =
    ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                         ql::span<const char>>;

// The magic bytes that `Manager::writeBlobHeader` writes (see
// `blobMagicBytes`).
constexpr std::array<char, 8> correctMagicBytes{'Q', 'L', 'V', 'R',
                                                'B', 'L', 'O', 'B'};

// Return a validly ZSTD-compressed blob whose decompressed contents consist of
// the given magic bytes followed by the given format version, and nothing else.
std::vector<char> compressedBlobWithHeader(std::array<char, 8> magicBytes,
                                           uint16_t formatVersion) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << magicBytes;
  writer << formatVersion;
  auto data = std::move(writer).data();
  return Manager::compressBlob(ql::span<const char>{data});
}

// Return a validly ZSTD-compressed blob that consists of nothing but a valid
// header, so that reading the index metadata JSON that is expected to follow it
// fails.
std::vector<char> compressedBlobWithOnlyHeader() {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  Manager::writeBlobHeader(writer);
  auto data = std::move(writer).data();
  return Manager::compressBlob(ql::span<const char>{data});
}

// Return a matcher for a `Manager::BlobError` of the given `type` whose message
// contains the `messageSubstring`.
auto blobErrorWith(ErrorType type, std::string_view messageSubstring = {}) {
  return AllOf(Field(&Manager::BlobError::type_, type),
               Field(&Manager::BlobError::message_,
                     HasSubstr(std::string{messageSubstring})));
}

// Return a matcher for a `std::optional<Manager::BlobError>` that holds an
// error of the given `type` whose message contains the `messageSubstring`.
auto isBlobError(ErrorType type, std::string_view messageSubstring = {}) {
  return Optional(blobErrorWith(type, messageSubstring));
}

// Return a matcher for the result of `Manager::tryToDecompressBlob` that holds
// an error of type `notDecompressible` with our own message.
auto isNotDecompressible() {
  return VariantWith<Manager::BlobError>(
      blobErrorWith(ErrorType::notDecompressible, "was not written by"));
}

// Return the decompressed `compressedBlob`, and fail the test if it cannot be
// decompressed.
std::vector<char, Manager::BlobAllocator> decompressOrFail(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator = {}) {
  auto result = Manager::tryToDecompressBlob(compressedBlob, allocator);
  if (const auto* error = std::get_if<Manager::BlobError>(&result)) {
    ADD_FAILURE() << error->message_;
    return std::vector<char, Manager::BlobAllocator>{};
  }
  return std::get<std::vector<char, Manager::BlobAllocator>>(std::move(result));
}

// Return a validly ZSTD-compressed blob with a valid header, followed by an
// index metadata JSON whose index format version is incompatible with the
// current version of QLever, and nothing else.
std::vector<char> compressedBlobWithIncompatibleIndexFormat() {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  Manager::writeBlobHeader(writer);
  nlohmann::json metadata;
  metadata["index-format-version"] =
      nlohmann::json{{"date", "1900-01-01"}, {"pull-request-number", 42}};
  writer << metadata.dump();
  auto data = std::move(writer).data();
  return Manager::compressBlob(ql::span<const char>{data});
}

// Write the `turtleContents` to a turtle file, build an index from it with the
// given vocabulary `type`, and return the corresponding `IndexBuilderConfig`.
// The basename of the index is derived from the name of the currently running
// test, so that concurrently running tests do not interfere with each other.
// The turtle input file is deleted again immediately after the index was built.
//
// NOTE: The default vocabulary type is the in-memory, uncompressed one,
// because `serializeVocabAndNamedCacheToCompressedBlob` currently requires it
// unless vocabulary entries are excluded (see
// `Vocabulary::writeAsZeroCopyBlob`).
IndexBuilderConfig buildTestIndex(
    std::string_view turtleContents,
    VocabularyType type = VocabularyType::InMemoryUncompressed) {
  std::string basename = gtestCurrentTestName();
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << turtleContents;
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  config.baseName_ = std::move(basename);
  config.vocabType_ = type;
  EXPECT_NO_THROW(Qlever::buildIndex(config));
  return config;
}

// The turtle data used by the tests for the filtered vocabulary export below:
// one triple whose subject and object survive the filtering, and one whose
// subject and object are excluded from the blob (they contain `dropped`, which
// is what the regexes below match).
constexpr std::string_view filterTestData =
    "<keptSubject> <filterPredicate> \"kept literal\".\n"
    "<droppedSubject> <filterPredicate> \"dropped literal\".";

// The name under which the tests below pin the result of their source query.
constexpr std::string_view filterPinName = "filterPin";

// The query whose result the tests below pin under `filterPinName`.
constexpr std::string_view filterSourceQuery =
    "SELECT ?s ?o WHERE { ?s <filterPredicate> ?o }";

// The query that returns the pinned result (see `filterPinName`) of a blob.
constexpr std::string_view filterPinQuery =
    "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-filterPin {}}";

// The regex with which the tests below exclude the vocabulary entries of
// `filterTestData` that contain `dropped`.
constexpr std::string_view droppedEntriesRegex = ".*dropped.*";

// Return a `BlobSerializationConfig` that excludes all vocabulary entries
// matching one of the given `regexes`. Note that the regexes are matched
// against the complete entry, hence the leading and trailing `.*` of
// `droppedEntriesRegex` above, which thus excludes the IRI `<droppedSubject>`
// as well as the literal `"dropped literal"`.
BlobSerializationConfig excludeConfig(std::vector<std::string> regexes) {
  BlobSerializationConfig config;
  config.excludedEntryRegexes_ = std::move(regexes);
  return config;
}

// Return the vocabulary index of the unique vocabulary entry of `qlever` that
// contains the `substring`.
uint64_t vocabIndexOfEntryContaining(const Qlever& qlever,
                                     std::string_view substring) {
  std::optional<uint64_t> result;
  const auto& vocabulary = qlever.indexAndViewsSnapshot()->index_.getVocab();
  for (const IndexAndWord& entry : vocabulary.scanAll()) {
    if (absl::StrContains(entry.word_, substring)) {
      EXPECT_FALSE(result.has_value()) << entry.word_;
      result = entry.index_;
    }
  }
  EXPECT_TRUE(result.has_value()) << substring;
  return result.value_or(0);
}

// Return a read serializer for the (already decompressed) blob in `data`. Note
// that the serializer only stores a view of the `data`, which therefore has to
// outlive it.
auto makeBlobReader(ql::span<const char> data) {
  return ad_utility::serialization::ByteBufferReadSerializerT<
      true, ql::span<const char>>{data};
}

// Decompress the `compressedBlob`, skip its header, and return the index
// metadata JSON that is stored directly after that header (see
// `NamedCachedQueryBlobManager::serialize`).
nlohmann::json metadataFromBlob(ql::span<const char> compressedBlob) {
  auto uncompressed = decompressOrFail(compressedBlob);
  auto reader = makeBlobReader(uncompressed);
  EXPECT_EQ(Manager::tryToSkipAndVerifyBlobHeader(reader), std::nullopt);
  std::string metadataJson;
  reader >> metadataJson;
  return nlohmann::json::parse(metadataJson);
}

// Load the `compressedBlob` into a fresh `Qlever` instance that has NO index
// files on disk at all (constructed with `skipLoading`), and return the result
// of running the `query` on that instance in TSV format.
std::string queryBlobInFreshInstance(ql::span<const char> compressedBlob,
                                     std::string_view query) {
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));
  return target.query(std::string{query}, ad_utility::MediaType::tsv);
}

// The result of `serializeFilterTestBlob` below.
struct FilterTestBlob {
  std::vector<char> blob_;
  uint64_t droppedSubjectIndex_ = 0;
  uint64_t droppedObjectIndex_ = 0;
  uint64_t keptSubjectIndex_ = 0;
};

// Open a `Qlever` instance on the index described by `sourceConfig`, pin the
// result of `filterSourceQuery` under `filterPinName`, and serialize the
// vocabulary and the named result cache to a blob from which all entries
// matching `droppedEntriesRegex` are excluded. Also return the original
// vocabulary indices of the entries that the tests below inspect.
FilterTestBlob serializeFilterTestBlob(const IndexBuilderConfig& sourceConfig) {
  Qlever source{EngineConfig{sourceConfig}};
  source.queryAndPinResultWithName(std::string{filterPinName},
                                   std::string{filterSourceQuery});
  FilterTestBlob result;
  result.droppedSubjectIndex_ =
      vocabIndexOfEntryContaining(source, "droppedSubject");
  result.droppedObjectIndex_ =
      vocabIndexOfEntryContaining(source, "dropped literal");
  result.keptSubjectIndex_ = vocabIndexOfEntryContaining(source, "keptSubject");
  result.blob_ = source.serializeVocabAndNamedCacheToCompressedBlob(
      excludeConfig({std::string{droppedEntriesRegex}}));
  return result;
}
}  // namespace

// _____________________________________________________________________________
// Test the compression utility and its inverse in isolation, for several
// buffer sizes (including the empty buffer).
TEST(NamedCachedQueryBlobManager, compressAndDecompressBlob) {
  for (const std::string& original :
       {std::string{}, std::string{"x"}, std::string{"a short blob"},
        std::string(100'000, 'q')}) {
    std::vector<char> compressed =
        Manager::compressBlob(ql::span<const char>{original});
    // The size of the uncompressed data is stored in the ZSTD frame header, so
    // the compressed blob is a plain ZSTD frame without any extra bookkeeping.
    EXPECT_EQ(
        ZstdWrapper::getUncompressedSize(compressed.data(), compressed.size()),
        original.size());

    auto roundTripped = decompressOrFail(compressed);
    EXPECT_THAT(roundTripped, ::testing::ElementsAreArray(original));
  }
}

// _____________________________________________________________________________
// Test that `writeBlobHeader` and `tryToSkipAndVerifyBlobHeader` mirror each
// other, and that an invalid header is rejected.
TEST(NamedCachedQueryBlobManager, writeAndVerifyBlobHeader) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  Manager::writeBlobHeader(writer);
  // Append a payload so that we can check the reader is positioned correctly
  // after the header.
  writer << std::string_view{"payload"};
  auto data = std::move(writer).data();

  BlobReader reader{ql::span<const char>{data}};
  EXPECT_EQ(Manager::tryToSkipAndVerifyBlobHeader(reader), std::nullopt);
  std::string payload;
  reader >> payload;
  EXPECT_EQ(payload, "payload");

  // A buffer that does not start with the expected magic header is rejected.
  ad_utility::serialization::AlignedByteBufferWriteSerializer wrongWriter;
  wrongWriter << std::array<char, 8>{'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X'};
  wrongWriter << uint16_t{1};
  auto wrongData = std::move(wrongWriter).data();
  BlobReader wrongReader{ql::span<const char>{wrongData}};
  EXPECT_THAT(Manager::tryToSkipAndVerifyBlobHeader(wrongReader),
              isBlobError(ErrorType::invalidMagicBytes, "was not written by"));
}

// _____________________________________________________________________________
// Test that a blob with the correct magic bytes but an incompatible format
// version is rejected.
TEST(NamedCachedQueryBlobManager, skipAndVerifyBlobHeaderRejectsWrongVersion) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  // The correct magic bytes (see `blobMagicBytes`), followed by a format
  // version that is definitely not the current one.
  writer << std::array<char, 8>{'Q', 'L', 'V', 'R', 'B', 'L', 'O', 'B'};
  writer << uint16_t{63999};
  auto data = std::move(writer).data();

  BlobReader reader{ql::span<const char>{data}};
  auto error = Manager::tryToSkipAndVerifyBlobHeader(reader);
  EXPECT_THAT(error,
              isBlobError(ErrorType::invalidVersion, "incompatible version"));
  // The message also names the version that was found.
  EXPECT_THAT(error,
              isBlobError(ErrorType::invalidVersion, "format version 63999"));
}

// _____________________________________________________________________________
// Test that a blob with the correct magic bytes but a truncated header is
// rejected with our own message, instead of with a cryptic message from the
// serializer.
TEST(NamedCachedQueryBlobManager, skipAndVerifyBlobHeaderRejectsShortInput) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << std::array<char, 4>{'Q', 'L', 'V', 'R'};
  auto data = std::move(writer).data();

  BlobReader reader{ql::span<const char>{data}};
  EXPECT_THAT(Manager::tryToSkipAndVerifyBlobHeader(reader),
              isBlobError(ErrorType::invalidMagicBytes, "was not written by"));
}

// _____________________________________________________________________________
// Test that a completely empty blob is rejected (and in particular without
// throwing).
TEST(NamedCachedQueryBlobManager, skipAndVerifyBlobHeaderRejectsEmptyInput) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  auto data = std::move(writer).data();
  ASSERT_TRUE(data.empty());

  BlobReader reader{ql::span<const char>{data}};
  EXPECT_THAT(Manager::tryToSkipAndVerifyBlobHeader(reader),
              isBlobError(ErrorType::invalidMagicBytes, "was not written by"));
}

// _____________________________________________________________________________
// Test that the header is also correctly verified if it is not at the very
// beginning of the buffer, and that the reader is positioned after the header
// if the header is valid.
TEST(NamedCachedQueryBlobManager, verifyBlobHeaderAtNonZeroPosition) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << uint64_t{42};
  Manager::writeBlobHeader(writer);
  auto data = std::move(writer).data();

  BlobReader reader{ql::span<const char>{data}};
  uint64_t prefix = 0;
  reader >> prefix;
  ASSERT_EQ(prefix, 42u);
  size_t positionBeforeHeader = reader.getCurrentPosition();
  EXPECT_EQ(Manager::tryToSkipAndVerifyBlobHeader(reader), std::nullopt);
  // The reader has been advanced past the magic bytes and the version, and the
  // buffer is exhausted.
  EXPECT_EQ(reader.getCurrentPosition() - positionBeforeHeader, 10u);
  EXPECT_EQ(reader.getCurrentPosition(), data.size());
}

// _____________________________________________________________________________
// Test that input which is not a ZSTD frame at all is rejected with our own
// message, rather than with a cryptic ZSTD error, and that in particular no
// attempt is made to allocate a buffer of an arbitrary size read from garbage.
TEST(NamedCachedQueryBlobManager, decompressBlobRejectsNonZstdInput) {
  // Input that is too short to even hold a ZSTD frame header.
  std::vector<char> tooShort(3, 'x');
  EXPECT_THAT(Manager::tryToDecompressBlob(tooShort, {}),
              isNotDecompressible());

  // Longer input that does not start with the ZSTD magic number. Note that
  // interpreting any eight of its bytes as the size of the uncompressed data
  // would yield about 18 exabytes.
  std::vector<char> garbage(1024, '\xFF');
  EXPECT_THAT(Manager::tryToDecompressBlob(garbage, {}), isNotDecompressible());
}

// _____________________________________________________________________________
// Test that a truncated blob (the typical result of an incomplete download) is
// rejected with our own message, rather than with a cryptic ZSTD error. Note
// that the frame header of such a blob is intact, so the size of the
// uncompressed data can be read, and the failure only occurs during the actual
// decompression.
TEST(NamedCachedQueryBlobManager, decompressBlobRejectsTruncatedInput) {
  const std::string original(10'000, 'q');
  std::vector<char> compressed =
      Manager::compressBlob(ql::span<const char>{original});
  ASSERT_GT(compressed.size(), 1u);
  EXPECT_EQ(ZstdWrapper::getUncompressedSize(compressed.data(),
                                             compressed.size() - 1),
            original.size());

  compressed.pop_back();
  // Note that the failure occurs during the decompression itself here, not
  // while reading the frame header.
  EXPECT_THAT(Manager::tryToDecompressBlob(compressed, {}),
              isNotDecompressible());
}

// _____________________________________________________________________________
// Test that a blob (index metadata + vocabulary + named result cache), written
// from one `Qlever` instance, can be loaded into a completely separate instance
// that has NO index files on disk at all (constructed with `skipLoading`), and
// there produce correct query results without loading any permutations.
TEST(NamedCachedQueryBlobManager, combinedBlob) {
  IndexBuilderConfig sourceConfig = buildTestIndex(
      "<combinedBlobSubject> <combinedBlobPredicate> "
      "\"combined blob literal\".");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(
        "blobPin", "SELECT ?s ?o WHERE { ?s <combinedBlobPredicate> ?o }");
    auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
    EXPECT_FALSE(blob.empty());
    return blob;
  }();

  // A completely fresh instance with NO index files on disk (`skipLoading`);
  // everything needed to answer the cached-result query comes from the blob.
  Qlever target{EngineConfig{}, /*skipLoading=*/true};

  // Before loading the blob, the named result cache is empty.
  std::string cachedResultQuery =
      "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-blobPin {}}";
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.query(cachedResultQuery, ad_utility::MediaType::tsv),
      HasSubstr("is not contained in the named result cache"));

  EXPECT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));

  // The named cached result, and the vocabulary needed to correctly export
  // its IDs as strings, now come entirely from the blob.
  auto res = target.query(cachedResultQuery, ad_utility::MediaType::tsv);
  EXPECT_EQ(res, "?s\t?o\n<combinedBlobSubject>\t\"combined blob literal\"\n");

  // Permutations are not part of the blob, so a query that needs them (i.e.
  // any query with actual triples) is unsupported on a blob-only instance and
  // throws.
  //
  // NOTE: The exception thrown in this case (no index loaded, but the query
  // requires one) is currently an internal `AD_CORRECTNESS_CHECK` (an
  // `ad_utility::Exception`) rather than a user-facing error message. We accept
  // this for now, as turning it into a graceful error would require changes to
  // a lot of code paths.
  EXPECT_THROW(target.query("SELECT ?s WHERE { ?s <combinedBlobPredicate> ?o }",
                            ad_utility::MediaType::tsv),
               ad_utility::Exception);

  // Loading a second blob on the same instance must throw.
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("must not be called more than once"));
}

// _____________________________________________________________________________
// Test that the `allocator` passed to
// `deserializeVocabAndNamedCacheFromCompressedBlob` is in fact used to allocate
// the (large) decompressed blob buffer.
TEST(NamedCachedQueryBlobManager, blobUsesProvidedAllocator) {
  IndexBuilderConfig sourceConfig = buildTestIndex(
      "<allocatorBlobSubject> <allocatorBlobPredicate> "
      "\"allocator blob literal\".");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(
        "blobPin", "SELECT ?s ?o WHERE { ?s <allocatorBlobPredicate> ?o }");
    return source.serializeVocabAndNamedCacheToCompressedBlob();
  }();

  CountingMemoryResource resource;
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(target.deserializeVocabAndNamedCacheFromCompressedBlob(
      compressedBlob, ql::pmr::polymorphic_allocator<char>{&resource}));

  // The decompressed blob buffer must have been allocated via `resource`.
  EXPECT_GT(resource.numAllocations_, 0u);
  EXPECT_GT(resource.bytesAllocated_, 0u);

  // The instance still answers queries correctly from the resource-backed
  // buffer.
  auto res = target.query(
      "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-blobPin {}}",
      ad_utility::MediaType::tsv);
  EXPECT_EQ(res,
            "?s\t?o\n<allocatorBlobSubject>\t\"allocator blob literal\"\n");
}

// _____________________________________________________________________________
// Test that loading a blob that does not carry a valid header is rejected.
TEST(NamedCachedQueryBlobManager, deserializeRejectsInvalidBlob) {
  // A validly ZSTD-compressed blob whose decompressed content does not start
  // with the expected magic header.
  std::vector<char> bogus(64, 'X');
  std::vector<char> compressedBlob = Manager::compressBlob(bogus);

  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("was not written by"));
}

// _____________________________________________________________________________
// Test that a blob with a valid header, but with contents that cannot be read,
// is rejected with our own message, rather than with a cryptic message from
// deep inside the deserialization.
TEST(NamedCachedQueryBlobManager, deserializeRejectsBlobWithInvalidContents) {
  // A blob that consists of nothing but a valid header, so that reading the
  // index metadata JSON that is expected to follow it fails.
  std::vector<char> compressedBlob = compressedBlobWithOnlyHeader();

  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("Error while reading the contents of a blob"));
}

// _____________________________________________________________________________
// Test that `tryToDeserializeVocabAndNamedCacheFromCompressedBlob` reports the
// details of a failure in the message of the returned error, and also writes
// that message to the error log, and that
// `deserializeVocabAndNamedCacheFromCompressedBlob` throws with the same
// message.
TEST(NamedCachedQueryBlobManager, tryToDeserializeReportsFailureDetails) {
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  // Load the `compressedBlob`, expect an error of the `expectedType`, and
  // return its message. Also check that the message was written to the log,
  // and that the throwing version throws with the same message.
  auto loadAndGetMessage = [&target](ql::span<const char> compressedBlob,
                                     ErrorType expectedType) {
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto error = target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
        compressedBlob);
    EXPECT_THAT(error, isBlobError(expectedType));
    std::string message = error.has_value() ? error.value().message_ : "";
    EXPECT_THAT(logStream.str(), HasSubstr(message));
    AD_EXPECT_THROW_WITH_MESSAGE(
        target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
        HasSubstr(message));
    return message;
  };

  // The underlying ZSTD error, both for input that does not start with the
  // ZSTD magic number, and for a validly compressed but truncated blob.
  std::vector<char> garbage(1024, '\xFF');
  EXPECT_THAT(loadAndGetMessage(garbage, ErrorType::notDecompressible),
              AllOf(HasSubstr("was not written by"),
                    HasSubstr("does not start with a valid ZSTD frame")));
  std::vector<char> truncated = Manager::compressBlob(garbage);
  truncated.pop_back();
  EXPECT_THAT(loadAndGetMessage(truncated, ErrorType::notDecompressible),
              HasSubstr("was not written by"));

  // The invalid magic bytes.
  std::vector<char> bogus(64, 'X');
  EXPECT_THAT(loadAndGetMessage(Manager::compressBlob(bogus),
                                ErrorType::invalidMagicBytes),
              HasSubstr("was not written by"));

  // The blob format version that was found.
  EXPECT_THAT(
      loadAndGetMessage(compressedBlobWithHeader(correctMagicBytes, 63999),
                        ErrorType::invalidVersion),
      AllOf(HasSubstr("incompatible version"),
            HasSubstr("format version 63999")));

  // The details of the incompatible index format.
  EXPECT_THAT(loadAndGetMessage(compressedBlobWithIncompatibleIndexFormat(),
                                ErrorType::incompatibleIndexFormat),
              AllOf(HasSubstr("incompatible index format"),
                    HasSubstr("The index is too old for this version of "
                              "QLever"),
                    HasSubstr("PR = 42")));
}

// _____________________________________________________________________________
// Test that a blob whose header is rejected by
// `tryToDeserializeVocabAndNamedCacheFromCompressedBlob` leaves the instance
// completely unchanged, so that a valid blob can still be loaded afterwards,
// and that a second valid blob is then rejected.
TEST(NamedCachedQueryBlobManager, tryToDeserializeLeavesInstanceUsable) {
  IndexBuilderConfig sourceConfig =
      buildTestIndex("<retrySubject> <retryPredicate> \"retry literal\".");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(
        "blobPin", "SELECT ?s ?o WHERE { ?s <retryPredicate> ?o }");
    return source.serializeVocabAndNamedCacheToCompressedBlob();
  }();

  Qlever target{EngineConfig{}, /*skipLoading=*/true};

  // None of an undecompressible, an unrecognized, an incompatible blob, or a
  // blob with an incompatible index format throws, and none of them counts as
  // the one allowed load.
  std::vector<char> garbage(1024, '\xFF');
  EXPECT_THAT(
      target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(garbage),
      isBlobError(ErrorType::notDecompressible));
  std::vector<char> bogus(64, 'X');
  EXPECT_THAT(target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
                  Manager::compressBlob(bogus)),
              isBlobError(ErrorType::invalidMagicBytes));
  EXPECT_THAT(target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
                  compressedBlobWithHeader(correctMagicBytes, uint16_t{63999})),
              isBlobError(ErrorType::invalidVersion));
  EXPECT_THAT(target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
                  compressedBlobWithIncompatibleIndexFormat()),
              isBlobError(ErrorType::incompatibleIndexFormat));

  // A blob whose metadata JSON cannot be read throws, but it does not count as
  // the one allowed load either, because nothing of the instance has been
  // modified yet.
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
          compressedBlobWithOnlyHeader()),
      HasSubstr("Error while reading the contents of a blob"));

  // The valid blob can still be loaded, and the instance then answers the query
  // from the named result cache and the vocabulary in the blob.
  EXPECT_EQ(target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
                compressedBlob),
            std::nullopt);
  EXPECT_EQ(
      target.query(
          "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-blobPin {}}",
          ad_utility::MediaType::tsv),
      "?s\t?o\n<retrySubject>\t\"retry literal\"\n");

  // After a successful load, a second blob is rejected, also by the
  // non-throwing version (a violated precondition is not a blob error).
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.tryToDeserializeVocabAndNamedCacheFromCompressedBlob(
          compressedBlob),
      HasSubstr("must not be called more than once"));
}

// _____________________________________________________________________________
// End-to-end test for a blob that carries a spatial (s2) index in its named
// result cache: build an index (with in-memory vocabulary), pin a query result
// together with a cached geometry index, serialize everything to a blob, load
// it into a fresh instance that has NO index files on disk, and run a spatial
// join that uses the cached geometry index from the blob.
TEST(NamedCachedQueryBlobManager, blobWithSpatialIndex) {
  // Four rail segments (linestrings) that are pinned as a cached s2 geometry
  // index. The query point used below lies within 1 km of all four segments
  // (see `SpatialJoinCachedIndexTest`).
  IndexBuilderConfig sourceConfig = buildTestIndex(
      "<s1> <asWKT> \"LINESTRING(7.8428469 47.9995367,7.8413293 "
      "47.9974942)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s2> <asWKT> \"LINESTRING(7.8409068 47.9975041,7.8420114 "
      "47.9989233)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s3> <asWKT> \"LINESTRING(7.8427369 47.9995806,7.8411672 "
      "47.9975175)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s4> <asWKT> \"LINESTRING(7.8422376 47.9990144,7.8411016 "
      "47.9975307)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    // Pin the linestrings together with a cached s2 geometry index on `?geo2`.
    source.queryAndPinResultWithName(
        QueryExecutionContext::PinResultWithName{"geoPin", Variable{"?geo2"}},
        "SELECT * { ?s2 <asWKT> ?geo2 }");
    auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
    EXPECT_FALSE(blob.empty());
    return blob;
  }();

  // A spatial join whose right side is the cached geometry index (from the
  // blob) and whose left side is a single point provided inline via `VALUES`,
  // so that no permutations (and hence no on-disk index) are needed.
  std::string spatialQuery =
      "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/> "
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
      "SELECT ?s2 WHERE { "
      "VALUES ?geo1 { \"POINT(7.841295 47.997731)\"^^geo:wktLiteral } "
      "SERVICE qlss: { "
      "_:config qlss:right ?geo2 ; "
      "qlss:left ?geo1 ; "
      "qlss:maxDistance 1000 ; "
      "qlss:algorithm qlss:experimentalPointPolyline ; "
      "qlss:experimentalRightCacheName \"geoPin\" . "
      "} }";

  // A fresh instance with no index files on disk. Before loading the blob the
  // cached geometry index does not exist, so the spatial query fails.
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.query(spatialQuery, ad_utility::MediaType::tsv),
      HasSubstr("is not contained in the named result cache"));

  // After loading the blob, the cached geometry index comes from the blob and
  // the spatial join succeeds, relating the query point to all four segments.
  EXPECT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));
  auto res = target.query(spatialQuery, ad_utility::MediaType::tsv);
  EXPECT_THAT(res, HasSubstr("<s1>"));
  EXPECT_THAT(res, HasSubstr("<s2>"));
  EXPECT_THAT(res, HasSubstr("<s3>"));
  EXPECT_THAT(res, HasSubstr("<s4>"));

  // The pinned result itself is also queryable directly from the blob.
  auto cachedRes = target.query(
      "SELECT ?s2 ?geo2 WHERE { SERVICE ql:cached-result-with-name-geoPin {} }",
      ad_utility::MediaType::tsv);
  EXPECT_THAT(cachedRes, HasSubstr("<s1>"));
}

// A test suite for the round trip of a blob from which some of the vocabulary
// entries were excluded, parameterized by the vocabulary type of the source
// index and the vocabulary type that the resulting blob is expected to have.
class BlobFilterTest
    : public testing::TestWithParam<std::pair<VocabularyType, VocabularyType>> {
};

// _____________________________________________________________________________
// Test a round trip of a blob from which some of the vocabulary entries were
// excluded, for each of the source vocabulary types that supports the
// filtering. The pinned query result references both a kept and an excluded
// entry, so that the preservation of the original vocabulary indices is
// actually exercised: the kept entries have to resolve to their original
// strings, the excluded ones to `placeholderForMissingVocabIndex`.
TEST_P(BlobFilterTest, blobWithExcludedVocabularyEntries) {
  const auto& [sourceType, expectedBlobType] = GetParam();
  auto sourceConfig = buildTestIndex(filterTestData, sourceType);
  auto [compressedBlob, droppedSubjectIndex, droppedObjectIndex,
        keptSubjectIndex] = serializeFilterTestBlob(sourceConfig);

  // The type of the vocabulary in the blob is recorded in its metadata JSON,
  // so that the reading side does not need to know about the filtering.
  EXPECT_EQ(metadataFromBlob(compressedBlob)["vocabulary-type"],
            expectedBlobType.toString());

  auto result = queryBlobInFreshInstance(compressedBlob, filterPinQuery);
  // The kept entries resolve to their original strings, and in particular the
  // kept subject kept its original vocabulary index (which is larger than the
  // index of the dropped subject, so the surviving vocabulary has a hole).
  EXPECT_GT(keptSubjectIndex, droppedSubjectIndex);
  EXPECT_THAT(result, HasSubstr("<keptSubject>\t\"kept literal\""));
  // The excluded entries resolve to the placeholder for their original index.
  EXPECT_THAT(result,
              HasSubstr(ad_utility::vocabulary::placeholderForMissingVocabIndex(
                  droppedSubjectIndex)));
  EXPECT_THAT(result,
              HasSubstr(ad_utility::vocabulary::placeholderForMissingVocabIndex(
                  droppedObjectIndex)));
}

INSTANTIATE_TEST_SUITE_P(
    VocabularyTypes, BlobFilterTest,
    testing::ValuesIn(std::vector<std::pair<VocabularyType, VocabularyType>>{
        {VocabularyType::InMemoryUncompressed,
         VocabularyType::InMemoryUncompressedWithHoles},
        {VocabularyType::OnDiskUncompressed,
         VocabularyType::InMemoryUncompressedWithHoles},
        {VocabularyType::InMemoryCompressed,
         VocabularyType::InMemoryCompressedWithHoles},
        {VocabularyType::OnDiskCompressed,
         VocabularyType::InMemoryCompressedWithHoles}}),
    [](const testing::TestParamInfo<BlobFilterTest::ParamType>& info) {
      // A gtest test name may only consist of alphanumeric characters and
      // underscores, so the `-` of the string representation of the vocabulary
      // type has to be replaced.
      return absl::StrReplaceAll(info.param.first.toString(), {{"-", "_"}});
    });

// _____________________________________________________________________________
// Test that a blob written with a list of regexes that matches no vocabulary
// entry at all is functionally equivalent to (though not byte-identical with)
// the unfiltered blob, and that an empty list of regexes produces a blob in the
// original format (i.e. with the original vocabulary type).
TEST(NamedCachedQueryBlobManager, blobWithRegexesThatMatchNothing) {
  auto sourceConfig = buildTestIndex(filterTestData);

  std::vector<char> unfilteredBlob;
  std::vector<char> filteredBlob;
  {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(std::string{filterPinName},
                                     std::string{filterSourceQuery});
    unfilteredBlob = source.serializeVocabAndNamedCacheToCompressedBlob();
    filteredBlob = source.serializeVocabAndNamedCacheToCompressedBlob(
        excludeConfig({"thisMatchesNoVocabularyEntry"}));
  }

  // An empty list of regexes leaves the format (and hence the vocabulary type
  // in the metadata JSON) untouched, a non-empty list switches to a vocabulary
  // with holes, so the two blobs are not byte-identical.
  EXPECT_EQ(metadataFromBlob(unfilteredBlob)["vocabulary-type"],
            ad_utility::VocabularyType::InMemoryUncompressed.toString());
  EXPECT_EQ(
      metadataFromBlob(filteredBlob)["vocabulary-type"],
      ad_utility::VocabularyType::InMemoryUncompressedWithHoles.toString());
  EXPECT_NE(unfilteredBlob, filteredBlob);

  // Both blobs are functionally equivalent: no entry was excluded, so all
  // strings resolve to their original values.
  std::string expected =
      "?s\t?o\n<droppedSubject>\t\"dropped literal\"\n<keptSubject>\t\"kept "
      "literal\"\n";
  for (const std::vector<char>& blob : {unfilteredBlob, filteredBlob}) {
    EXPECT_EQ(queryBlobInFreshInstance(blob, filterPinQuery), expected);
  }
}

// _____________________________________________________________________________
// Test that excluding vocabulary entries from a blob is rejected with a
// descriptive message if the source vocabulary is a geo-split vocabulary (whose
// marker-encoded indices cannot be represented by a vocabulary with holes).
TEST(NamedCachedQueryBlobManager, blobWithExcludedEntriesRejectsGeoSplitVocab) {
  auto sourceConfig =
      buildTestIndex(filterTestData, VocabularyType::OnDiskCompressedGeoSplit);

  Qlever source{EngineConfig{sourceConfig}};
  source.queryAndPinResultWithName(std::string{filterPinName},
                                   std::string{filterSourceQuery});
  AD_EXPECT_THROW_WITH_MESSAGE(
      source.serializeVocabAndNamedCacheToCompressedBlob(
          excludeConfig({std::string{droppedEntriesRegex}})),
      HasSubstr("on-disk-compressed-geo-split"));
}
