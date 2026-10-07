// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H
#define QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/UninitializedAllocator.h"

namespace qlever {

class Qlever;

// Options that control how a blob is written by
// `NamedCachedQueryBlobManager::serialize`.
struct BlobSerializationConfig {
  // Regexes for vocabulary entries that are not needed in the blob. Every
  // vocabulary entry that matches any of these regexes (via `RE2::FullMatch`,
  // so the regex has to describe the complete entry) is omitted. Note that the
  // regexes are matched against literals as well as IRIs; to exclude only
  // IRIs, let the regex start with `<` and end with `>`. The remaining entries
  // keep their original vocabulary indices, so that the `Id`s in the
  // serialized `NamedResultCache` stay valid; the exported vocabulary
  // therefore has holes (see `VocabularyInMemoryBinSearch`) and an `Id` that
  // refers to an excluded entry resolves to
  // `placeholderForMissingVocabIndex`. If this is empty, the complete
  // vocabulary is exported in its original format.
  std::vector<std::string> excludedEntryRegexes_;
};

// Serialize and deserialize the vocabulary and the `NamedResultCache` of a
// `Qlever` instance to and from a single, self-contained, ZSTD-compressed blob
// (see `serialize`/`deserialize`). The functionality is bundled here (rather
// than in the `Qlever` class itself) to keep the core `Qlever` class small; a
// `Qlever` holds one instance of this manager as a member, and this class is a
// friend of `Qlever` so that it can access its internals.
class NamedCachedQueryBlobManager {
 public:
  // The versions of the blob format. The two versions differ only in that
  // blobs of the newer version additionally contain a secondary vocabulary
  // (see `serialize`), so both of them can be read by `tryToDeserialize`.
  static constexpr uint16_t formatVersionWithoutSecondaryVocab = 1;
  static constexpr uint16_t formatVersionWithSecondaryVocab = 2;

  // Allocator for the decompressed blob buffer (see `tryToDecompressBlob`). It
  // is stacked so that the buffer is 1. default-initialized (no redundant
  // zeroing of a buffer that is about to be overwritten by the decompression),
  // 2. allocated via a caller-provided `pmr` memory resource, and 3. aligned to
  // the maximal possible alignment (required so that the aligned, zero-copy
  // serialization written by `serialize` can be read back without
  // misalignment).
  using BlobAllocator = ad_utility::default_init_allocator<
      char,
      ad_utility::AlignedAllocator<char, ql::pmr::polymorphic_allocator<char>>>;

  // The reasons why a blob is rejected (see `BlobError`). All of these are
  // detected before the vocabulary or the named result cache of the blob are
  // read, and before the `Qlever` instance is modified.
  enum class BlobErrorType {
    // The blob is not a ZSTD frame written by `compressBlob`, or its compressed
    // data is corrupted (see `tryToDecompressBlob`).
    notDecompressible,
    // The decompressed blob does not start with the expected magic bytes. This
    // also covers the case that it is too short to hold a complete header.
    invalidMagicBytes,
    // The magic bytes are correct, but the format version stored in the blob is
    // none of the format versions above.
    invalidVersion,
    // The header is valid, but the index format version that is stored in the
    // index metadata of the blob is incompatible with this version of QLever
    // (see `IndexImpl::checkIndexFormatVersion`).
    incompatibleIndexFormat
  };

  // The error that is reported by the non-throwing functions below: the reason
  // for the rejection, and a descriptive message (which includes the details,
  // for example the underlying ZSTD error).
  struct BlobError {
    BlobErrorType type_;
    std::string message_;
  };

  // The result of `tryToDecompressBlob` below: the decompressed blob on
  // success, and the error otherwise.
  using DecompressedBlobOrError =
      std::variant<std::vector<char, BlobAllocator>, BlobError>;

  // The result of `tryToSkipAndVerifyBlobHeader` below: the format version of
  // the blob (one of the format versions above) on success, and the error
  // otherwise.
  using FormatVersionOrError = std::variant<uint16_t, BlobError>;

 private:
  // In this buffer, the blob passed to `deserialize` is kept alive (in
  // decompressed form) for the lifetime of this manager (and hence of the
  // owning `Qlever` instance), because the loaded vocabulary, secondary
  // vocabulary, and named cache entries are zero-copy views directly into it.
  std::optional<std::vector<char, BlobAllocator>>
      deserializedBlobLifetimeExtender_;

 public:
  // Serialize the index metadata JSON, the vocabulary, and the
  // `NamedResultCache` of `qlever` into a single, self-contained,
  // ZSTD-compressed blob that can later be loaded via `deserialize` (e.g. by a
  // different process, without needing access to the on-disk index). Throw if
  // the vocabulary implementation currently in use does not support zero-copy
  // serialization (see `Vocabulary::writeAsZeroCopyBlob`).
  //
  // If `config.excludedEntryRegexes_` is not empty, only those vocabulary
  // entries that match none of the regexes are exported (see
  // `BlobSerializationConfig` and `buildFilteredVocabulary`). The type of the
  // exported vocabulary then is one of the `...WithHoles` types, which is
  // recorded in the blob's metadata JSON (key `"vocabulary-type"`), so that
  // `deserialize` picks up the correct vocabulary implementation without any
  // change to the blob format.
  //
  // The named cache entries are allowed to contain `Id`s of type
  // `LocalVocabIndex` (for example because SPARQL UPDATE operations were
  // applied before an entry was pinned). The words of such `Id`s that are not
  // contained in the vocabulary are written to a secondary vocabulary in the
  // blob, which also contains the words of the secondary vocabulary of the
  // index of `qlever` (if any), and the entries are written as rewritten
  // copies that refer to that secondary vocabulary (see
  // `NamedCacheSecondaryVocabRewriter.h`); the entries of `qlever` stay
  // unchanged. If that secondary vocabulary is empty, then the blob is
  // written with `formatVersionWithoutSecondaryVocab`, else with
  // `formatVersionWithSecondaryVocab`.
  std::vector<char> serialize(const Qlever& qlever,
                              const BlobSerializationConfig& config = {}) const;

  // Load a blob previously written by `serialize`: decompress it, store it in
  // the buffer, and then replace `qlever`'s vocabulary and `NamedResultCache`
  // by the contents of the blob using zero-copy deserialization. The buffer is
  // kept alive for the lifetime of this manager and is allocated via the
  // `allocator` (see `BlobAllocator` above). If the blob contains a secondary
  // vocabulary, then it becomes the secondary vocabulary of the index of
  // `qlever` (see `IndexImpl::setSecondaryVocab`), which must not have one yet
  // (which is checked, and throws, before `qlever` is modified).
  //
  // Return `std::nullopt` on success, and the `BlobError` if the blob cannot be
  // decompressed, if its header is missing or incompatible, or if its index
  // format version is incompatible (see `BlobErrorType`). In those cases,
  // `qlever` is left completely unchanged, so it can be used as if this
  // function had never been called, and another blob can be loaded afterwards.
  // The message of the error is also written to the error log.
  //
  // NOTE: This function is non-throwing only for the failures that are detected
  // before the vocabulary or the named result cache of the blob are read (see
  // `BlobErrorType`). Contents that cannot be read behind a valid header still
  // throw (in particular a metadata JSON that cannot be parsed), as does a
  // violation of the preconditions below. If the metadata JSON cannot be read,
  // `qlever` is also left unchanged, and another blob can be loaded
  // afterwards.
  //
  // PRECONDITION: Must only be called while no other thread can concurrently
  // access `qlever`, e.g. right after construction and before the first query
  // is answered. Must not be called more than once on the same manager, except
  // after a call that left `qlever` unchanged (see above).
  std::optional<BlobError> tryToDeserialize(
      Qlever& qlever, ql::span<const char> compressedBlob,
      ql::pmr::polymorphic_allocator<char> allocator);

  // Same as `tryToDeserialize`, but throw an exception with the message of the
  // `BlobError` instead of returning it.
  void deserialize(Qlever& qlever, ql::span<const char> compressedBlob,
                   ql::pmr::polymorphic_allocator<char> allocator);

  // The following are stateless, self-contained utilities that make up the blob
  // format. They are exposed publicly so that they can be unit-tested in
  // isolation.

  // Compress `uncompressedBlob` into a single ZSTD frame.
  static std::vector<char> compressBlob(ql::span<const char> uncompressedBlob);

  // Inverse of `compressBlob`: decompress `compressedBlob` into a freshly
  // allocated buffer that uses `allocator` for its storage (see
  // `BlobAllocator`). Return a `notDecompressible` error if `compressedBlob`
  // was not written by `compressBlob`, or is corrupted. Errors that do not come
  // from ZSTD (in particular a failed allocation, should the frame header state
  // an uncompressed size that cannot be allocated) are not reported that way.
  static DecompressedBlobOrError tryToDecompressBlob(
      ql::span<const char> compressedBlob,
      ql::pmr::polymorphic_allocator<char> allocator);

  // Write the magic header and the `formatVersion` (one of the format versions
  // above) at the start of a blob. Mirrors `tryToSkipAndVerifyBlobHeader`
  // below.
  static void writeBlobHeader(
      ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer,
      uint16_t formatVersion);

  // Read and verify the magic header and format version at the start of a
  // decompressed blob, advancing `serializer` past them. Return the format
  // version if the header is valid, and the `BlobError` otherwise. Do not throw
  // for any input, provided that the current position of the `serializer` is
  // even (as it is at the start of a blob); in particular, a missing or
  // truncated header is reported as `invalidMagicBytes`. In case of an error,
  // the position of the `serializer` is unspecified afterwards. Mirrors
  // `writeBlobHeader`.
  static FormatVersionOrError tryToSkipAndVerifyBlobHeader(
      ad_utility::serialization::ByteBufferReadSerializerT<
          true, ql::span<const char>>& serializer);
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H
