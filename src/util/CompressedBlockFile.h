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
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Synchronized.h"

namespace ad_utility {

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
  // Everything that is needed to read a single compressed block back. The
  // sizes are in bytes.
  struct BlockMetadata {
    size_t compressedSize_;
    size_t uncompressedSize_;
    size_t offsetInFile_;
  };

 private:
  std::string filename_;
  Synchronized<File, std::shared_mutex> file_{filename_, "w+"};

 public:
  // Create the file at `filename`, overwriting it if it already exists.
  explicit CompressedBlockFile(std::string filename)
      : filename_{std::move(filename)} {}

  // Close and delete the file.
  ~CompressedBlockFile() {
    file_.wlock()->close();
    ad_utility::deleteFile(filename_);
  }

  // The name of the underlying file.
  const std::string& filename() const { return filename_; }

  // Compress the `numBytes` bytes at `data` and append them to the file. Return
  // the metadata that `readBlock` needs to read them back.
  BlockMetadata appendBlock(const void* data, size_t numBytes) {
    auto compressed = ZstdWrapper::compress(data, numBytes);
    size_t offset = 0;
    file_.withWriteLock([&offset, &compressed](File& file) {
      offset = static_cast<size_t>(file.tell());
      file.write(compressed.data(), compressed.size());
    });
    return {compressed.size(), numBytes, offset};
  }

  // Read the block that is described by `metadata` and decompress it into
  // `target`, which has to have room for `metadata.uncompressedSize_` bytes.
  //
  // NOTE: Only blocks that were appended before the last call to `flush` are
  // guaranteed to be readable.
  void readBlock(const BlockMetadata& metadata, void* target) const {
    std::vector<char> compressed(metadata.compressedSize_);
    auto numBytesRead =
        file_.rlock()->read(compressed.data(), metadata.compressedSize_,
                            static_cast<off_t>(metadata.offsetInFile_));
    AD_CORRECTNESS_CHECK(numBytesRead >= 0 &&
                         static_cast<size_t>(numBytesRead) ==
                             metadata.compressedSize_);
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
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COMPRESSEDBLOCKFILE_H
