// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H
#define QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
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
//
// THE CONTENTS OF A BLOB: the index metadata JSON, the vocabulary of the
// index, the *secondary vocabulary* of the blob (see below), and all entries of
// the `NamedResultCache`. That is everything that is needed to answer queries
// on the cached results, so a blob can be loaded by a process that has no
// access to the on-disk index at all.
//
// NEW WORDS: After SPARQL UPDATE operations have been applied to an index,
// re-pinning the same named queries can yield results that contain *new words*
// which are not part of the vocabulary of the index (they only live in the
// local vocab of the result). Store such words in the secondary vocabulary of
// the blob (see `index/vocabulary/SecondaryVocabulary.h`) and rewrite the `Id`s
// of the cache entries that refer to them into `Id`s of type
// `Datatype::SecondaryVocabIndex` (see `serialize`). The secondary vocabulary
// is an append-only sequence of segments; a blob written by `serialize` has at
// most one segment, but `deserialize` reads an arbitrary number of them.
//
// THE CHUNK FORMAT: Store the individual parts of a blob as *chunks*, each of
// which begins at an aligned offset (see `parseBlobLayout` for the exact
// format). The point of that alignment is that the bytes of a chunk are
// *position independent*, so that a chunk can be copied verbatim from one blob
// into another one. This is what a follow-up mechanism that writes a *diff*
// between two blobs (instead of a complete new blob after every update) builds
// on.
//
// PRECONDITION: The index that a blob is created from must not have a secondary
// vocabulary of its own, see `serialize`.
class NamedCachedQueryBlobManager {
 public:
  // Allocator for the decompressed blob buffer (see `decompressBlob`). It is
  // stacked so that the buffer is 1. default-initialized (no redundant zeroing
  // of a buffer that is about to be overwritten by the decompression),
  // 2. allocated via a caller-provided `pmr` memory resource, and 3. aligned to
  // the maximal possible alignment (required so that the aligned, zero-copy
  // serialization written by `serialize` can be read back without
  // misalignment).
  using BlobAllocator = ad_utility::default_init_allocator<
      char,
      ad_utility::AlignedAllocator<char, ql::pmr::polymorphic_allocator<char>>>;

  // The positions of the individual chunks of an uncompressed blob, as computed
  // by `parseBlobLayout`. This is what is needed to inspect a blob without
  // deserializing it (as the tests do), and what a follow-up diff mechanism
  // will need in order to copy those chunks that did not change byte for byte
  // from an earlier blob.
  struct BlobLayout {
    // The number of bytes that a chunk has in front of its payload (the size
    // field plus the padding that aligns the payload), see `parseBlobLayout`.
    static constexpr size_t chunkHeaderSize = alignof(std::max_align_t);

    // The byte region `[begin_, end_)` of one chunk of the blob, where
    // `begin_` is the (aligned) offset of the chunk's size field and `end_` is
    // the offset right after the chunk's payload.
    struct Region {
      size_t begin_ = 0;
      size_t end_ = 0;

      // The number of bytes of this region.
      size_t size() const { return end_ - begin_; }

      // The offset at which the payload of this chunk begins.
      size_t payloadBegin() const { return begin_ + chunkHeaderSize; }

      // Return the payload of this chunk as a view into `blob`, which has to
      // be the (already decompressed) blob that this region was obtained from
      // (via `parseBlobLayout`).
      ql::span<const char> payloadSpan(ql::span<const char> blob) const {
        return blob.subspan(payloadBegin(), end_ - payloadBegin());
      }
    };

    // One entry of the `NamedResultCache` in the blob, together with the name
    // under which it is cached (which is stored at the beginning of the
    // chunk's payload).
    struct Entry {
      std::string key_;
      Region region_;
    };

    Region metadata_;
    Region vocabulary_;
    Region segmentCount_;
    std::vector<Region> segments_;
    Region entryCount_;
    std::vector<Entry> entries_;
  };

 private:
  // In this buffer, the blob passed to `deserialize` is kept alive (in
  // decompressed form) for the lifetime of this manager (and hence of the
  // owning `Qlever` instance), because the loaded vocabulary and named cache
  // entries are zero-copy views directly into it.
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
  // The blob consists of a header, followed by a sequence of *chunks* (see
  // `parseBlobLayout` for the exact format): the index metadata JSON, the
  // vocabulary, the segments of the secondary vocabulary of the blob, and one
  // chunk per entry of the `NamedResultCache`.
  //
  // Every `Id` of a cache entry that refers to a word that is not part of the
  // vocabulary of the index (which happens if SPARQL UPDATE operations were
  // applied before the entry was pinned) is rewritten: the word is stored in
  // the secondary vocabulary of the blob, and the `Id` becomes an `Id` of type
  // `Datatype::SecondaryVocabIndex`.
  //
  // If `config.excludedEntryRegexes_` is not empty, only those vocabulary
  // entries that match none of the regexes are exported (see
  // `BlobSerializationConfig` and `buildFilteredVocabulary`). The type of the
  // exported vocabulary then is one of the `...WithHoles` types, which is
  // recorded in the blob's metadata JSON (key `"vocabulary-type"`), so that
  // `deserialize` picks up the correct vocabulary implementation without any
  // change to the blob format.
  //
  // PRECONDITION: The index of `qlever` must not have a secondary vocabulary
  // of its own (an index that was loaded from a blob does have one as soon as
  // that blob contained new words).
  std::vector<char> serialize(const Qlever& qlever,
                              const BlobSerializationConfig& config = {}) const;

  // Load a blob previously written by `serialize`: decompress it, store it in
  // the buffer, and then replace `qlever`'s vocabulary, secondary vocabulary,
  // and `NamedResultCache` by the contents of the blob using zero-copy
  // deserialization. The buffer is kept alive for the lifetime of this manager
  // and is allocated via the `allocator` (see `BlobAllocator` above).
  //
  // PRECONDITION: Must only be called while no other thread can concurrently
  // access `qlever`, e.g. right after construction and before the first query
  // is answered. Must not be called more than once on the same manager.
  void deserialize(Qlever& qlever, ql::span<const char> compressedBlob,
                   ql::pmr::polymorphic_allocator<char> allocator);

  // The following are stateless, self-contained utilities that make up the blob
  // format. They are exposed publicly so that they can be unit-tested in
  // isolation.

  // Compute the positions of the chunks of the given uncompressed blob, which
  // has to be one written by `serialize`.
  //
  // THE CHUNK FORMAT: A blob starts with the header (see `writeBlobHeader`),
  // followed by a sequence of chunks. Each chunk consists of zero padding up
  // to the next multiple of `alignof(std::max_align_t)`, a `uint64_t` with the
  // size of its payload, more zero padding up to the next multiple of
  // `alignof(std::max_align_t)`, and finally the payload itself. The chunks
  // are, in this order: the metadata JSON, the vocabulary, the number of
  // segments of the secondary vocabulary, those segments, the number of
  // entries of the `NamedResultCache`, and those entries (sorted by their
  // key).
  //
  // The point of the padding is that the payload of every chunk starts at a
  // multiple of `alignof(std::max_align_t)`. The bytes of a chunk are
  // therefore *position independent*: because the alignment padding that the
  // (aligned) serialization inserts inside the payload only depends on the
  // offsets *within* the payload, the bytes of a chunk can be moved to any
  // other suitably aligned offset of any suitably aligned buffer, and the
  // zero-copy deserialization of the payload still works. That is what makes
  // it possible to assemble a new blob out of chunks that are copied from an
  // older blob, which is what a follow-up diff mechanism builds on.
  static BlobLayout parseBlobLayout(ql::span<const char> uncompressedBlob);

  // Compress `uncompressedBlob` into a single ZSTD frame.
  static std::vector<char> compressBlob(ql::span<const char> uncompressedBlob);

  // Inverse of `compressBlob`: decompress `compressedBlob` into a freshly
  // allocated buffer that uses `allocator` for its storage (see
  // `BlobAllocator`). Throw with a descriptive message if `compressedBlob` was
  // not written by `compressBlob`, or is corrupted.
  static std::vector<char, BlobAllocator> decompressBlob(
      ql::span<const char> compressedBlob,
      ql::pmr::polymorphic_allocator<char> allocator);

  // Write the magic header and format version at the start of a blob. Mirrors
  // `skipAndVerifyBlobHeader` below.
  static void writeBlobHeader(
      ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer);

  // Read and verify the magic header and format version at the start of a
  // decompressed blob, advancing `serializer` past them. Throw if the header is
  // missing, truncated, or has an incompatible format version. Mirrors
  // `writeBlobHeader`.
  static void skipAndVerifyBlobHeader(
      ad_utility::serialization::ByteBufferReadSerializerT<
          true, ql::span<const char>>& serializer);
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H
