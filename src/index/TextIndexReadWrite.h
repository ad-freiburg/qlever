// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "global/Id.h"
#include "global/IndexTypes.h"
#include "index/Postings.h"
#include "index/TextMetaData.h"
#include "util/HashMap.h"
#include "util/Simple8bCode.h"

namespace TextIndexReadWrite {
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

// The readFreqCompr and readGapCompr methods have to be in the header file
// because of the constexpr being evaluated at compile time
template <typename T, typename MakeFromUint64t = std::identity>
vector<T> readFreqComprList(size_t nofElements, off_t from, size_t nofBytes,
                            const ad_utility::File& textIndexFile,
                            MakeFromUint64t makeFromUint = MakeFromUint64t{}) {
  AD_CONTRACT_CHECK(nofBytes > 0);
  LOG(DEBUG) << "Reading frequency-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  size_t nofCodebookBytes;
  vector<T> result;
  uint64_t* encoded = new uint64_t[nofElements];
  result.resize(nofElements + 250);
  off_t current = from;
  size_t ret = textIndexFile.read(&nofCodebookBytes, sizeof(size_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CONTRACT_CHECK(sizeof(size_t) == ret);
  current += ret;
  T* codebook = new T[nofCodebookBytes / sizeof(T)];
  ret = textIndexFile.read(codebook, nofCodebookBytes, current);
  current += ret;
  AD_CONTRACT_CHECK(ret == size_t(nofCodebookBytes));
  ret = textIndexFile.read(
      encoded, static_cast<size_t>(nofBytes - (current - from)), current);
  current += ret;
  AD_CONTRACT_CHECK(size_t(current - from) == nofBytes);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data(),
                                   makeFromUint);
  LOG(DEBUG) << "Reverting frequency encoded items to actual IDs...\n";
  result.resize(nofElements);
  for (size_t i = 0; i < result.size(); ++i) {
    // TODO<joka921> handle the strong ID types properly.
    if constexpr (requires(T t) { t.getBits(); }) {
      result[i] = Id::makeFromVocabIndex(
          VocabIndex::make(codebook[result[i].getBits()].getBits()));
    } else {
      result[i] = codebook[result[i]];
    }
  }
  delete[] encoded;
  delete[] codebook;
  LOG(DEBUG) << "Done reading frequency-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

template <typename T, typename MakeFromUint64t>
vector<T> readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                           const ad_utility::File& textIndexFile,
                           MakeFromUint64t makeFromUint64t) {
  LOG(DEBUG) << "Reading gap-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  vector<T> result;
  result.resize(nofElements + 250);
  uint64_t* encoded = new uint64_t[nofBytes / 8];
  textIndexFile.read(encoded, nofBytes, from);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements, result.data(),
                                   makeFromUint64t);
  LOG(DEBUG) << "Reverting gaps to actual IDs...\n";

  // TODO<joka921> make this hack unnecessary, probably by a proper output
  // iterator.
  if constexpr (requires { T::make(0); }) {
    uint64_t id = 0;
    for (size_t i = 0; i < result.size(); ++i) {
      id += result[i].get();
      result[i] = T::make(id);
    }
  } else {
    T id = 0;
    for (size_t i = 0; i < result.size(); ++i) {
      id += result[i];
      result[i] = id;
    }
  }
  result.resize(nofElements);
  delete[] encoded;
  LOG(DEBUG) << "Done reading gap-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

}  // namespace TextIndexReadWrite

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
  std::vector<size_t> encodedVector_;
  CodeMap codeMap_;
  CodeBook codeBook_;
};

template <typename T>
requires std::is_arithmetic_v<T> class GapEncode {
 public:
  using TypedVector = std::vector<T>;

  explicit GapEncode(const TypedVector& vectorToEncode);

  void writeToFile(ad_utility::File& out, size_t nofElements,
                   off_t& currentOffset);

  const TypedVector& getEncodedVector() { return encodedVector_; }

 private:
  TypedVector encodedVector_;
};
