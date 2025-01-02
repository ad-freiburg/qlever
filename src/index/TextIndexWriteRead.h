// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "global/Id.h"
#include "global/IndexTypes.h"
#include "index/Postings.h"
#include "index/TextMetaData.h"
#include "util/HashMap.h"

using Posting = std::tuple<TextRecordIndex, WordIndex, Score>;

namespace TextIndexWriteRead {
ContextListMetaData writePostings(ad_utility::File& out,
                                  const vector<Posting>& postings,
                                  bool skipWordlistIfAllTheSame,
                                  off_t& currentOffset);

template <typename T>
size_t writeCodebook(const vector<T>& codebook, ad_utility::File& file);

//! Writes a list of elements (have to be able to be cast to unit64_t)
//! to file.
//! Returns the number of bytes written.
template <typename Numeric>
size_t writeList(Numeric* data, size_t nofElements, ad_utility::File& file);

template <typename T>
void writeVectorAndMoveOffset(const std::vector<T>& vectorToWrite,
                              size_t nofElements, ad_utility::File& file,
                              off_t& currentOffset);

}  // namespace TextIndexWriteRead

template <typename T>
class FrequencyEncode {
 public:
  using TypedMap = ad_utility::HashMap<T, size_t>;
  using TypedVector = std::vector<T>;
  using CodeMap = TypedMap;
  using CodeBook = TypedVector;

  explicit FrequencyEncode(const TypedVector& vectorToEncode);

  void writeToFile(ad_utility::File& out, size_t nofElements,
                   off_t& currentOffset);

  const TypedVector getEncodedVector() { return encodedVector_; }
  const CodeMap& getCodeMap() { return codeMap_; }
  const CodeBook& getCodeBook() { return codeBook_; }

 private:
  TypedVector encodedVector_;
  CodeMap codeMap_;
  CodeBook codeBook_;
};

template <typename T>
requires std::is_arithmetic_v<T> class GapEncode {
 public:
  using TypedVector = std::vector<T>;
  using GapList = TypedVector;

  explicit GapEncode(const TypedVector& vectorToEncode);

  void writeToFile(ad_utility::File& out, size_t nofElements,
                   off_t& currentOffset);

  const TypedVector& getEncodedVector() { return encodedVector_; }
  const GapList& getGapList() { return gapList_; }

 private:
  TypedVector encodedVector_;
  GapList gapList_;
};
