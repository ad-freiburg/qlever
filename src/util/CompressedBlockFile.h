// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSEDBLOCKFILE_H
#define QLEVER_SRC_UTIL_COMPRESSEDBLOCKFILE_H

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Synchronized.h"

namespace ad_utility {

// The default compression level of `ZstdWrapper::compress`, which is what a
// `CompressedBlockFile` uses unless the caller says otherwise.
constexpr inline int ZSTD_DEFAULT_LEVEL = 3;

// A temporary file that stores an append-only sequence of independently
// compressed blocks of bytes. A block is written via `appendBlock`, which
// returns the `BlockMetadata` that is required to read that block back via
// `readBlock`; the file itself stores no index of its own, so it is up to the
// caller to keep those metadata around and to organize them (for example one
// block per column of an `IdTable`, see the `compressedIdTable` namespace in
// `engine/idTable/CompressedIdTableBlocks.h`).
//
// THREAD SAFETY: All the operations of this class may be called concurrently
// from any number of threads. Appending takes an exclusive lock (it has to,
// because it uses the shared file offset), whereas reading takes a shared lock
// only, because it is implemented via `pread` (see `File::read` with an
// explicit offset). Reading can therefore run concurrently with other reads,
// and also with an append (a block that was appended before, and that the
// caller consequently holds the metadata of, is not touched by later appends).
//
// NOTE: The file is deleted in the destructor, so this class is only suitable
// for temporary data.
class CompressedBlockFile {
 public:
  // How the blocks of a file are stored: a ZSTD compression level, or
  // `std::nullopt` (see `NO_BLOCK_COMPRESSION` below) to store the bytes as
  // they are. Which of the two is faster depends entirely on the caller.
  // Compressing pays off for data that has to fit on disk, or that is written
  // once and read often. For a short-lived file whose blocks are read back
  // almost immediately it is often a pure loss, because the compression is CPU
  // work that competes with the actual computation, see
  // `engine/idTable/CompressedIdTableBlockStorage.h`.
  //
  // NOTE: The setting applies to the whole file, so `readBlock` does not have
  // to (and cannot) derive it from the metadata of a single block.
  using Compression = std::optional<int>;

  // Everything that is needed to read a single block back. The sizes are in
  // bytes, and they are equal for a file that is stored uncompressed.
  struct BlockMetadata {
    size_t compressedSize_;
    size_t uncompressedSize_;
    size_t offsetInFile_;
  };

 private:
  std::string filename_;
  Compression compression_;
  Synchronized<File, std::shared_mutex> file_{filename_, "w+"};

 public:
  // Create the file at `filename`, overwriting it if it already exists, and
  // store its blocks with the given `compression`.
  explicit CompressedBlockFile(std::string filename,
                               Compression compression = ZSTD_DEFAULT_LEVEL)
      : filename_{std::move(filename)}, compression_{compression} {}

  // Close and delete the file.
  ~CompressedBlockFile() {
    file_.wlock()->close();
    ad_utility::deleteFile(filename_);
  }

  // The name of the underlying file.
  const std::string& filename() const { return filename_; }

  // The compression that this file stores its blocks with.
  Compression compression() const { return compression_; }

  // Append the `numBytes` bytes at `data` to the file, compressing them unless
  // this file was created with `NO_BLOCK_COMPRESSION`. Return the metadata that
  // `readBlock` needs to read them back.
  BlockMetadata appendBlock(const void* data, size_t numBytes) {
    if (!compression_.has_value()) {
      return {numBytes, numBytes, appendBytes(data, numBytes)};
    }
    auto compressed =
        ZstdWrapper::compress(data, numBytes, compression_.value());
    return {compressed.size(), numBytes,
            appendBytes(compressed.data(), compressed.size())};
  }

  // Read the block that is described by `metadata` and decompress it into
  // `target`, which has to have room for `metadata.uncompressedSize_` bytes.
  //
  // NOTE: Only blocks that were appended before the last call to `flush` are
  // guaranteed to be readable.
  void readBlock(const BlockMetadata& metadata, void* target) const {
    if (!compression_.has_value()) {
      // NOTE: An uncompressed block is read straight into the `target`, so this
      // path needs neither an intermediate buffer nor a copy.
      AD_CORRECTNESS_CHECK(metadata.compressedSize_ ==
                           metadata.uncompressedSize_);
      readBytes(metadata, target);
      return;
    }
    std::vector<char> compressed(metadata.compressedSize_);
    readBytes(metadata, compressed.data());
    auto numBytesDecompressed = ZstdWrapper::decompressToBuffer(
        compressed.data(), compressed.size(), static_cast<char*>(target),
        metadata.uncompressedSize_);
    AD_CORRECTNESS_CHECK(numBytesDecompressed == metadata.uncompressedSize_);
  }

  // Flush the file, such that all the blocks that were appended so far become
  // readable.
  void flush() { file_.wlock()->flush(); }

  // Truncate the file, such that it can be reused. All the metadata that were
  // returned by previous calls to `appendBlock` become invalid.
  void clear() {
    auto file = file_.wlock();
    file->close();
    ad_utility::deleteFile(filename_);
    file->open(filename_, "w+");
  }

 private:
  // Append the `numBytes` bytes at `data` to the file and return the offset at
  // which they were written. This takes an exclusive lock, because it uses the
  // shared file offset.
  size_t appendBytes(const void* data, size_t numBytes) {
    size_t offset = 0;
    file_.withWriteLock([&offset, data, numBytes](File& file) {
      offset = static_cast<size_t>(file.tell());
      file.write(data, numBytes);
    });
    return offset;
  }

  // Read the `compressedSize_` bytes of the block that is described by
  // `metadata` into `target`.
  void readBytes(const BlockMetadata& metadata, void* target) const {
    auto numBytesRead =
        file_.rlock()->read(target, metadata.compressedSize_,
                            static_cast<off_t>(metadata.offsetInFile_));
    AD_CORRECTNESS_CHECK(numBytesRead >= 0 &&
                         static_cast<size_t>(numBytesRead) ==
                             metadata.compressedSize_);
  }
};

// Pass this as the compression of a `CompressedBlockFile` to store its blocks
// uncompressed, see `CompressedBlockFile::Compression`.
constexpr inline std::optional<int> NO_BLOCK_COMPRESSION = std::nullopt;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COMPRESSEDBLOCKFILE_H
