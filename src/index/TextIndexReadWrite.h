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
/// THIS METHOD HAS BEEN MODIFIED
/// It basically tries to mimic the old function but with the new classes.
ContextListMetaData writePostings(ad_utility::File& out,
                                  const vector<Posting>& postings,
                                  bool skipWordlistIfAllTheSame,
                                  off_t& currentOffset);

/// NOTHING CHANGED
template <typename T>
size_t writeCodebook(const vector<T>& codebook, ad_utility::File& file);

/// NOTHING CHANGED
//! Writes a list of elements (have to be able to be cast to unit64_t)
//! to file.
//! Returns the number of bytes written.
template <typename Numeric>
size_t writeList(Numeric* data, size_t nofElements, ad_utility::File& file);

/// THIS METHOD HAS BEEN ADDED
/// This is just a helper function to remove the code duplication seen in the
/// deleted lines 457,458 and 467,468 and 474,475 of IndexImpl.Text.cpp
template <typename T>
void writeVectorAndMoveOffset(const std::vector<T>& vectorToWrite,
                              size_t nofElements, ad_utility::File& file,
                              off_t& currentOffset);

/// THIS METHOD HAS BEEN MODIFIED
/// It's hard to explain what has been changed since the original method was
/// all over the place and confusing. First of all I changed the way the
/// frequency encoded result gets decoded. Before it was done in the result
/// vector which was of type T which lead to complicated handling of Ids.
/// To see this look at the deleted lines 867, 888-896 of IndexImpl.Text.cpp
/// Also an ongoing confusion is the naming of some variables since there
/// has to happen two decodings thus we first have on variable each that can
/// be named encoded.
/// I tried to also template this function in a way to surpass the inconsistency
/// between the datatypes written to the file and the datatypes we want to get
/// out. This problem also stems from the fact that Posting is somewhat
/// ambiguous since the WordIndex in a posting can refer to an entityId so a
/// VocabIndex or a word so a WordVocabIndex.
// Read a freqComprList from the textIndexFile. The From specifies the type
// that was used to create the codebook in the writing step and the To
// specifies the type to cast that codebook values to. This is done with a
// static cast if no lambda function to cast is given.
template <typename To, typename From>
vector<To> readFreqComprList(
    size_t nofElements, off_t from, size_t nofBytes,
    const ad_utility::File& textIndexFile,
    const std::function<To(From)>& transformer = [](From x) {
      return static_cast<To>(x);
    }) {
  AD_CONTRACT_CHECK(nofBytes > 0);
  LOG(DEBUG) << "Reading frequency-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  size_t nofCodebookBytes;
  vector<uint64_t> frequencyEncodedResult;
  uint64_t* encoded = new uint64_t[nofElements];
  frequencyEncodedResult.resize(nofElements + 250);
  off_t current = from;
  size_t ret = textIndexFile.read(&nofCodebookBytes, sizeof(size_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CONTRACT_CHECK(sizeof(size_t) == ret);
  current += ret;
  From* codebook = new From[nofCodebookBytes / sizeof(From)];
  ret = textIndexFile.read(codebook, nofCodebookBytes, current);
  current += ret;
  AD_CONTRACT_CHECK(ret == size_t(nofCodebookBytes));
  ret = textIndexFile.read(
      encoded, static_cast<size_t>(nofBytes - (current - from)), current);
  current += ret;
  AD_CONTRACT_CHECK(size_t(current - from) == nofBytes);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements,
                                   frequencyEncodedResult.data());
  LOG(DEBUG) << "Reverting frequency encoded items to actual IDs...\n";
  frequencyEncodedResult.resize(nofElements);
  vector<To> result;
  result.reserve(frequencyEncodedResult.size());
  for (size_t i = 0; i < frequencyEncodedResult.size(); ++i) {
    result.push_back(transformer(codebook[frequencyEncodedResult[i]]));
  }
  delete[] encoded;
  delete[] codebook;
  LOG(DEBUG) << "Done reading frequency-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

/// THIS METHOD HAS BEEN MODIFIED
/// I tried to do the same thing as above with the templates since some values
/// are saved as a different type (that is the From type) and one wants to
/// retrieve them as another type. This also removed some weird specific Id
/// handling. Look at the deleted lines 835-849 of IndexImpl.Text.cpp
template <typename To, typename From>
vector<To> readGapComprList(
    size_t nofElements, off_t from, size_t nofBytes,
    const ad_utility::File& textIndexFile,
    const std::function<To(From)>& transformer = [](From x) {
      return static_cast<To>(x);
    }) {
  LOG(DEBUG) << "Reading gap-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';
  vector<From> gapEncodedVector;
  gapEncodedVector.resize(nofElements + 250);
  uint64_t* encoded = new uint64_t[nofBytes / 8];
  textIndexFile.read(encoded, nofBytes, from);
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  ad_utility::Simple8bCode::decode(encoded, nofElements,
                                   gapEncodedVector.data());
  LOG(DEBUG) << "Reverting gaps to actual IDs...\n";
  gapEncodedVector.resize(nofElements);

  // Undo gapEncoding
  vector<To> result;
  result.reserve(nofElements);
  From previous = 0;
  for (size_t i = 0; i < gapEncodedVector.size(); ++i) {
    previous += gapEncodedVector[i];
    result.push_back(transformer(previous));
  }
  delete[] encoded;
  LOG(DEBUG) << "Done reading gap-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

}  // namespace TextIndexReadWrite

/// THIS IS A NEW CLASS WHICH MAINLY STEMS FROM ONE FUNCTION BEFORE
/// The FrequencyEncode class basically does the olf createCodebook method
/// and encoding in one during the constructor. It can then be used to write
/// the encoded Vector to file. The improvement is the templating removing the
/// hard coded method createCodebook.
/// The writeToFile is just the code from the deleted lines 466-468 and 473-475
/// of IndexImpl.Text.cpp
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
