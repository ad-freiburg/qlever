// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYSKIPPOINTERS_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYSKIPPOINTERS_H

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "index/ConstantsIndexBuilding.h"
#include "index/IndexBuilderTypes.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/Serializer.h"

// The skip pointers of a partial vocabulary words file, which allow reading
// such a file in independent blocks of consecutive words, each starting at a
// known byte offset.
//
// The complete layout of a partial vocabulary words file (as written by
// `writePartialVocabularyToFile` in `index/VocabularyMergerImpl.h`) is:
//
//     [uint64 numWords]                                       <- offset 0
//     numWords x [TripleComponentWithIndex]                   <- the words
//     [uint64 numEntries]                                     <- `wordsEnd`
//     numEntries x [PartialVocabularySkipPointer]
//     [uint64 wordsEnd]
//     [uint64 PARTIAL_VOCAB_SKIP_POINTERS_MAGIC]
//
// The `j`-th skip pointer describes the block of the words with the indices
// `[j * k, min((j + 1) * k, numWords))`, where `k` is the interval with which
// the file was written (`PARTIAL_VOCAB_SKIP_POINTER_INTERVAL` by default).
// Everything up to and including the words has the same format as before the
// skip pointers were introduced, so a reader that reads `numWords` and then
// exactly `numWords` words still works and ignores the trailing bytes.
namespace ad_utility::vocabulary_merger {

// A single skip pointer, see above.
struct PartialVocabularySkipPointer {
  // The byte offset in the file at which the first word of the block starts.
  uint64_t byteOffset_ = 0;
  // The number of words in all the previous blocks.
  uint64_t numWordsBefore_ = 0;
  // The first and the last word of the block, exactly as they are stored in
  // the block (including `isExternal_` and the local index).
  TripleComponentWithIndex firstWord_;
  TripleComponentWithIndex lastWord_;

  AD_SERIALIZE_FRIEND_FUNCTION(PartialVocabularySkipPointer) {
    serializer | arg.byteOffset_;
    serializer | arg.numWordsBefore_;
    serializer | arg.firstWord_;
    serializer | arg.lastWord_;
  }
};

// All the skip pointers of a single partial vocabulary words file, together
// with the information from the file that is needed to interpret them.
struct PartialVocabularySkipPointers {
  // The total number of words in the file.
  uint64_t numWords_ = 0;
  // The byte offset directly behind the last word, where the skip pointers
  // start.
  uint64_t wordsEnd_ = 0;
  std::vector<PartialVocabularySkipPointer> skipPointers_;

  // Return the number of blocks (which is the number of skip pointers).
  size_t numBlocks() const { return skipPointers_.size(); }

  // Return the number of words in the `block`-th block.
  uint64_t numWordsInBlock(size_t block) const {
    uint64_t end = block + 1 < numBlocks()
                       ? skipPointers_.at(block + 1).numWordsBefore_
                       : numWords_;
    return end - skipPointers_.at(block).numWordsBefore_;
  }

  // Return the byte offset directly behind the last word of the `block`-th
  // block.
  uint64_t byteEndOfBlock(size_t block) const {
    return block + 1 < numBlocks() ? skipPointers_.at(block + 1).byteOffset_
                                   : wordsEnd_;
  }
};

// Append the `skipPointers` and the footer to a partial vocabulary words file
// to which the `serializer` has just written all the words (see the layout
// above). The `serializer` has to support `getSerializationPosition()`.
template <typename Serializer>
void appendPartialVocabularySkipPointers(
    Serializer& serializer,
    const std::vector<PartialVocabularySkipPointer>& skipPointers) {
  uint64_t wordsEnd = serializer.getSerializationPosition();
  serializer << static_cast<uint64_t>(skipPointers.size());
  for (const auto& skipPointer : skipPointers) {
    serializer << skipPointer;
  }
  serializer << wordsEnd;
  serializer << PARTIAL_VOCAB_SKIP_POINTERS_MAGIC;
}

// Read the skip pointers of the partial vocabulary words file `filename`.
// Throw if the file has no skip pointers (e.g. because it was written by an
// older version of QLever), or if they are inconsistent with the file.
inline PartialVocabularySkipPointers readPartialVocabularySkipPointers(
    const std::string& filename) {
  namespace ser = ad_utility::serialization;
  ad_utility::File file{filename, "r"};
  auto fileSize = static_cast<uint64_t>(file.sizeOfFile());

  // Read exactly `numBytes` bytes at the given `offset` into `target`.
  auto readAt = [&file, &filename](void* target, size_t numBytes,
                                   uint64_t offset) {
    AD_CONTRACT_CHECK(file.read(target, numBytes, static_cast<off_t>(offset)) ==
                          static_cast<ssize_t>(numBytes),
                      "Reading from the partial vocabulary file `", filename,
                      "` failed");
  };

  // The footer consists of `wordsEnd` and the magic number.
  constexpr uint64_t footerSize = 2 * sizeof(uint64_t);
  uint64_t magic = 0;
  if (fileSize >= sizeof(uint64_t) + footerSize) {
    readAt(&magic, sizeof(magic), fileSize - sizeof(magic));
  }
  AD_CONTRACT_CHECK(
      magic == PARTIAL_VOCAB_SKIP_POINTERS_MAGIC,
      "The partial vocabulary file `", filename,
      "` has no skip pointers (the magic number at its end is missing). Such "
      "files have to be written by `writePartialVocabularyToFile`, files that "
      "were written by an older version of QLever are not supported");

  PartialVocabularySkipPointers result;
  readAt(&result.numWords_, sizeof(uint64_t), 0);
  readAt(&result.wordsEnd_, sizeof(uint64_t), fileSize - footerSize);
  auto errorMessage = [&filename](std::string_view what) {
    return absl::StrCat("The skip pointers of the partial vocabulary file `",
                        filename, "` are corrupted: ", what);
  };
  // The skip pointers start behind the `numWords` header and have to leave
  // room for their own number of entries and for the footer.
  AD_CONTRACT_CHECK(
      result.wordsEnd_ >= sizeof(uint64_t) &&
          result.wordsEnd_ + sizeof(uint64_t) + footerSize <= fileSize,
      errorMessage("their start offset is out of range"));

  // Read all the skip pointers into memory first, such that corrupted skip
  // pointers can never lead to a read past their end.
  std::vector<char> buffer(fileSize - footerSize - result.wordsEnd_);
  readAt(buffer.data(), buffer.size(), result.wordsEnd_);
  ser::ByteBufferReadSerializer reader{std::move(buffer)};
  uint64_t numEntries = 0;
  reader >> numEntries;
  // The smallest possible skip pointer consists of the two offsets and two
  // empty words (each of which consists of the size of the word, the
  // `isExternal_` flag and the local index).
  constexpr uint64_t minEntrySize =
      2 * sizeof(uint64_t) + 2 * (2 * sizeof(uint64_t) + sizeof(bool));
  size_t remainingBytes = reader.data().size() - reader.getCurrentPosition();
  AD_CONTRACT_CHECK(numEntries <= remainingBytes / minEntrySize,
                    errorMessage("their number is too large"));
  result.skipPointers_.resize(numEntries);
  for (auto& skipPointer : result.skipPointers_) {
    reader >> skipPointer;
  }
  AD_CONTRACT_CHECK(reader.getCurrentPosition() == reader.data().size(),
                    errorMessage("there are trailing bytes behind them"));

  // Check that the skip pointers cover all the words from the beginning on,
  // and that their byte offsets point into the words.
  const auto& pointers = result.skipPointers_;
  AD_CONTRACT_CHECK(pointers.empty() == (result.numWords_ == 0),
                    errorMessage("their number doesn't match the number "
                                 "of words"));
  for (size_t i = 0; i < pointers.size(); ++i) {
    const auto& pointer = pointers[i];
    if (i == 0) {
      AD_CONTRACT_CHECK(
          pointer.numWordsBefore_ == 0 &&
              pointer.byteOffset_ == sizeof(uint64_t),
          errorMessage("the first block doesn't start at the first word"));
    } else {
      AD_CONTRACT_CHECK(
          pointer.numWordsBefore_ > pointers[i - 1].numWordsBefore_ &&
              pointer.byteOffset_ > pointers[i - 1].byteOffset_,
          errorMessage("they are not strictly increasing"));
    }
    AD_CONTRACT_CHECK(pointer.numWordsBefore_ < result.numWords_ &&
                          pointer.byteOffset_ < result.wordsEnd_,
                      errorMessage("a block starts behind the last word"));
  }
  return result;
}

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYSKIPPOINTERS_H
