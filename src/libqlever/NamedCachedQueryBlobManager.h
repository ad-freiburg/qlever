// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H
#define QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H

#include <optional>
#include <vector>

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/UninitializedAllocator.h"

namespace qlever {

class Qlever;

// Serialize and deserialize the vocabulary and the `NamedResultCache` of a
// `Qlever` instance to and from a single, self-contained, ZSTD-compressed blob
// (see `serialize`/`deserialize`). The functionality is bundled here (rather
// than in the `Qlever` class itself) to keep the core `Qlever` class small; a
// `Qlever` holds one instance of this manager as a member, and this class is a
// friend of `Qlever` so that it can access its internals.
class NamedCachedQueryBlobManager {
 public:
  // Allocator for the decompressed blob buffer (see
  // `decompressBlobWithTrailingSizeInfo`). It is stacked so that the buffer is
  // 1. default-initialized (no redundant zeroing of a buffer that is about to
  // be overwritten by the decompression), 2. allocated via a caller-provided
  // `pmr` memory resource, and 3. aligned to the maximal possible alignment
  // (required so that the aligned, zero-copy serialization written by
  // `serialize` can be read back without misalignment).
  using BlobAllocator = ad_utility::default_init_allocator<
      char,
      ad_utility::AlignedAllocator<char, ql::pmr::polymorphic_allocator<char>>>;

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
  std::vector<char> serialize(const Qlever& qlever) const;

  // Load a blob previously written by `serialize`: decompress it, store it in
  // the buffer, and then replace `qlever`'s vocabulary and `NamedResultCache`
  // by the contents of the blob using zero-copy deserialization. The buffer is
  // kept alive for the lifetime of this manager and is allocated via the
  // `allocator` (see `BlobAllocator` above).
  //
  // PRECONDITION: Must only be called while no other thread can concurrently
  // access `qlever`, e.g. right after construction and before the first query
  // is answered. Must not be called more than once on the same manager.
  void deserialize(Qlever& qlever, ql::span<const char> compressedBlob,
                   ql::pmr::polymorphic_allocator<char> allocator);

  // The following are stateless, self-contained utilities that make up the blob
  // format. They are exposed publicly so that they can be unit-tested in
  // isolation.

  // Compress `uncompressedBlob` and append the size of the uncompressed buffer
  // as a trailing `uint64_t`. `decompressBlobWithTrailingSizeInfo` reads this
  // trailing size to allocate a target buffer of exactly the right size for
  // the decompression.
  static std::vector<char> compressBlobAndAddTrailingSizeInfo(
      ql::span<const char> uncompressedBlob);

  // Inverse of `compressBlobAndAddTrailingSizeInfo`: read the trailing
  // `uint64_t` size, then decompress the preceding compressed data into a
  // freshly allocated buffer that uses `allocator` for its storage (see
  // `BlobAllocator`).
  static std::vector<char, BlobAllocator> decompressBlobWithTrailingSizeInfo(
      ql::span<const char> compressedBlob,
      ql::pmr::polymorphic_allocator<char> allocator);

  // Write the magic header and format version at the start of a blob. Mirrors
  // `skipAndVerifyBlobHeader` below.
  static void writeBlobHeader(
      ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer);

  // Read and verify the magic header and format version at the start of a
  // decompressed blob, advancing `serializer` past them. Throws on mismatch.
  // Mirrors `writeBlobHeader`.
  static void skipAndVerifyBlobHeader(
      ad_utility::serialization::ByteBufferReadSerializerT<
          true, ql::span<const char>>& serializer);
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_NAMEDCACHEDQUERYBLOBMANAGER_H
