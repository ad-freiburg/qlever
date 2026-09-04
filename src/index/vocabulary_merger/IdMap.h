// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAP_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAP_H

#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/ExceptionHandling.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

// The part of the vocabulary merger (see `index/VocabularyMerger.h`) that
// exclusively deals with the mapping from the local indices of a partial
// vocabulary to the global IDs, and never with the words themselves.
namespace ad_utility::vocabulary_merger {

// A single entry of an ID map (see `IdMapWriter` below): the index that a word
// has inside a partial vocabulary, and the global ID that the vocabulary merger
// has assigned to that word.
//
// NOTE: This deliberately is a plain struct and not a `std::pair`. libstdc++'s
// `std::pair` has user-provided assignment operators and hence is not trivially
// copyable, which would make a `std::vector` of them neither trivially
// serializable (it would then be written and read one member at a time) nor
// bitwise relocatable.
struct IdMapEntry {
  // NOTE: The local index deliberately is a plain index and not an `Id`. Inside
  // a partial vocabulary a word is always a `VocabIndex`, so the datatype bits
  // of an `Id` would carry no information. Adding them is cheap, but
  // `Id::makeFromVocabIndex` also checks that the index fits into the available
  // bits, and that check for each of the (very many) entries would cost
  // measurable time in the vocabulary merger.
  uint64_t localIndex_;
  Id globalId_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(IdMapEntry, localIndex_,
                                              globalId_)

  // Enable the serialization of an `IdMapEntry` (and of contiguous ranges of
  // them) in the `ad_utility::serialization` framework.
  template <typename T>
  friend std::true_type allowTrivialSerialization(IdMapEntry, T);

  // Make the output of failed tests readable.
  friend std::ostream& operator<<(std::ostream& str, const IdMapEntry& entry) {
    return str << '{' << entry.localIndex_ << ", " << entry.globalId_ << '}';
  }
};
static_assert(
    ad_utility::serialization::TriviallySerializable<IdMapEntry>,
    "An `IdMapEntry` has to be trivially serializable, else a whole `IdMap` "
    "would be written and read one member at a time");

// Writes `IdMapEntry`s incrementally to a file. The entries are buffered and
// only handed to the file in blocks, because a single entry is only 16 bytes
// and writing each of them directly to the file would be very inefficient. The
// resulting file has exactly the format of a serialized `IdMap`.
class IdMapWriter {
 public:
  // The amount of data that is buffered before it is written to the file.
  // NOTE: There is one `IdMapWriter` per partial vocabulary, of which there can
  // be thousands, so this must not be too large: it is not only the memory
  // footprint, but also the cache and TLB pressure of the writes, which are
  // scattered over all the writers.
  static constexpr ad_utility::MemorySize bufferSize =
      ad_utility::MemorySize::kilobytes(16);

 private:
  using Serializer = ad_utility::serialization::BufferedWriteSerializer<
      ad_utility::serialization::FileWriteSerializer>;
  // NOTE: The indirection via the `unique_ptr` makes this class movable, which
  // is required because the `IdMapWriter`s are stored in a `std::vector`.
  std::unique_ptr<Serializer> serializer_;
  // The number of entries that have been pushed so far. It is written to the
  // beginning of the file by `finish()`.
  uint64_t numEntries_ = 0;

 public:
  explicit IdMapWriter(const std::string& filename)
      : serializer_{std::make_unique<Serializer>(
            ad_utility::serialization::FileWriteSerializer{filename},
            bufferSize)} {
    // Write a placeholder for the number of entries, which is only known once
    // `finish()` is called.
    *serializer_ << numEntries_;
  }

  // This class is move-only. NOTE: There deliberately is no move assignment,
  // as it would have to deal with the (currently never occurring) case that the
  // assigned-to writer has not been finished yet.
  IdMapWriter(const IdMapWriter&) = delete;
  IdMapWriter& operator=(const IdMapWriter&) = delete;
  IdMapWriter(IdMapWriter&&) noexcept = default;

  ~IdMapWriter() {
    ad_utility::terminateIfThrows([this]() { finish(); },
                                  "The closing of an `IdMapWriter` failed");
  }

  // Append a single entry.
  void push_back(const IdMapEntry& entry) {
    *serializer_ << entry;
    ++numEntries_;
  }

  // Flush the buffer, write the total number of entries to the beginning of
  // the file, and close the file. This is automatically called by the
  // destructor. After a call to `finish()`, no more calls to `push_back()` are
  // allowed.
  void finish() {
    if (!serializer_) {
      return;
    }
    auto file = std::move(*serializer_).underlyingSerializer();
    serializer_.reset();
    file.setSerializationPosition(0);
    file << numEntries_;
    file.close();
  }
};

// Get the `IdMapEntry`s deserialized from a file that has previously been
// written using the `IdMapWriter` class above.
using IdMap = std::vector<IdMapEntry>;
inline IdMap getIdMapFromFile(const std::string& filename) {
  IdMap idMap;
  ad_utility::serialization::FileReadSerializer serializer(filename);
  serializer >> idMap;
  return idMap;
}
}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAP_H
