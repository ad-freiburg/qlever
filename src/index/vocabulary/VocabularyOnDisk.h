// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_VOCABULARYONDISK_H
#define QLEVER_SRC_INDEX_VOCABULARYONDISK_H

#include <string>
#include <vector>

#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"
#include "util/File.h"
#include "util/Iterators.h"
#include "util/MmapVector.h"

// On-disk vocabulary of strings. Each entry is a pair of <ID, String>. The IDs
// are ascending, but not (necessarily) contiguous. If the strings are sorted,
// then binary search for a string can be performed.
class VocabularyOnDisk : public VocabularyBinarySearchMixin<VocabularyOnDisk> {
 private:
  // The offset of a word in the underlying file.
  using Offset = uint64_t;
  // The file in which the words are stored.
  mutable ad_utility::File file_;

  // The IDs and offsets of the words.
  ad_utility::MmapVectorView<Offset> offsets_;

  // The number of words stored in the vocabulary.
  size_t size_ = 0;

  // This suffix is appended to the filename of the main file, in order to get
  // the name for the file in which IDs and offsets are stored.
  static constexpr std::string_view offsetSuffix_ = ".offsets";

 public:
  // A helper class that is used to build a vocabulary word by word.
  // Each call to `operator()` adds the next word to the vocabulary.
  // At the end, the `finish()` method can be called. Note that `finish`
  // is also implicitly called by the destructor, but doing so implicitly
  // releases resources earlier and is cleaner in case of exceptions.
  class WordWriter : public WordWriterBase {
   private:
    ad_utility::File file_;
    ad_utility::MmapVector<Offset> offsets_;
    uint64_t currentOffset_ = 0;

   public:
    // Constructor, used by `VocabularyOnDisk::wordWriter`.
    explicit WordWriter(const std::string& filename);
    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternalDummy) override;

    ~WordWriter() override;

   private:
    // Finish the writing. After this no more calls to `operator()` are allowed.
    void finishImpl() override;
  };

  /// Build from a vector of pairs of `(string, id)`. This requires the IDs to
  /// be contiguous.
  void buildFromStringsAndIds(
      const std::vector<std::pair<std::string, uint64_t>>& wordsAndIds,
      const std::string& fileName);

  /// Open the vocabulary from file. It must have been previously written to
  /// this file, for example via `buildFromVector` or `buildFromTextFile`.
  void open(const std::string& filename);

  // Return the word that is stored at the index. Throw an exception if `idx >=
  // size`.
  std::string operator[](uint64_t idx) const;

  /// Get the number of words in the vocabulary.
  size_t size() const { return size_; }

  /// Default constructor for an empty vocabulary.
  VocabularyOnDisk() = default;

  /// `VocabularyOnDisk` is movable, but not copyable.
  VocabularyOnDisk(VocabularyOnDisk&&) noexcept = default;
  VocabularyOnDisk& operator=(VocabularyOnDisk&&) noexcept = default;

  // The offset of a word in `file_` and its size in number of bytes.
  struct OffsetAndSize {
    uint64_t offset_;
    uint64_t size_;
  };

  // The `Accessor` for the `IteratorForAccessOperator` class below.
  struct Accessor {
    template <typename Voc>
    constexpr auto operator()(const Voc& vocabulary, uint64_t index) const {
      return vocabulary[index];
    }
  };
  // Const random access iterators, implemented via the
  // `IteratorForAccessOperator` template.
  using const_iterator =
      ad_utility::IteratorForAccessOperator<VocabularyOnDisk, Accessor>;
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // Convert an iterator to the corresponding `WordAndIndex`. Needed for the
  // Mixin base class
  WordAndIndex iteratorToWordAndIndex(const_iterator it) const {
    if (it == end()) {
      return WordAndIndex::end();
    } else {
      return {*it, static_cast<uint64_t>(it - begin())};
    }
  }

 private:
  // Get the `OffsetAndSize` for the element with the `idx`. Return
  // `std::nullopt` if `idx` is not contained in the vocabulary.
  OffsetAndSize getOffsetAndSize(uint64_t idx) const;

  // Build a vocabulary from any type that is forward-iterable and yields
  // pairs of (string-like, ID). Used as the common implementation for
  // the `buildFrom...` methods.
  template <class Iterable>
  void buildFromIterable(Iterable&& iterable, const std::string& filename);
};

#endif  // QLEVER_SRC_INDEX_VOCABULARYONDISK_H
