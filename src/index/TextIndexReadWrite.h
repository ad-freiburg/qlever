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
#include "util/TransparentFunctors.h"

namespace {

// This function contains the actual frequency compressed list reading and does
// the following steps:
// - Read codebook size
// - Read codebook
// - return the read codebook through reference
// - Read list from disk which is simple8b and frequency encoded
// - simple8b decode the list
// - return the still frequency encoded vector through reference
template <typename From>
void readFreqComprListHelper(size_t nofElements, off_t from, size_t nofBytes,
                             const ad_utility::File& textIndexFile,
                             vector<uint64_t>& frequencyEncodedVector,
                             std::vector<From>& codebook) {
  AD_CONTRACT_CHECK(nofBytes > 0);
  LOG(DEBUG) << "Reading frequency-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';

  // Read codebook size and advance pointer (current)
  size_t nofCodebookBytes;
  off_t current = from;
  size_t ret = textIndexFile.read(&nofCodebookBytes, sizeof(size_t), current);
  LOG(TRACE) << "Nof Codebook Bytes: " << nofCodebookBytes << '\n';
  AD_CONTRACT_CHECK(sizeof(size_t) == ret);
  current += ret;

  // Set correct size of codebook, read codebook, advance pointer (current)
  codebook.resize(nofCodebookBytes / sizeof(From));
  ret = textIndexFile.read(codebook.data(), nofCodebookBytes, current);
  AD_CONTRACT_CHECK(ret == size_t(nofCodebookBytes));
  current += ret;

  // Create vector that is simple8b and frequency encoded, read encoded vector
  // from file, advance pointer (current)
  std::vector<uint64_t> simple8bEncoded;
  simple8bEncoded.resize(nofElements);
  ret = textIndexFile.read(simple8bEncoded.data(), nofBytes - (current - from),
                           current);
  current += ret;

  AD_CONTRACT_CHECK(size_t(current - from) == nofBytes);

  // simple8b decode the list which is then directly passed to
  // frequencyEncodedVector. The resizing with overhead is necessary for
  // simple8b
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  frequencyEncodedVector.resize(nofElements + 250);
  ad_utility::Simple8bCode::decode(simple8bEncoded.data(), nofElements,
                                   frequencyEncodedVector.data());
  LOG(DEBUG) << "Reverting frequency encoded items to actual IDs...\n";
  frequencyEncodedVector.resize(nofElements);
}

// This function contains the actual gap compressed list reading and does the
// following steps:
// - Read list from disk which is simple8b and gap encoded
// - simple8b decode the list
// - return the still gap encoded vector through reference
template <typename From>
void readGapComprListHelper(size_t nofElements, off_t from, size_t nofBytes,
                            const ad_utility::File& textIndexFile,
                            vector<From>& gapEncodedVector) {
  LOG(DEBUG) << "Reading gap-encoded list from disk...\n";
  LOG(TRACE) << "NofElements: " << nofElements << ", from: " << from
             << ", nofBytes: " << nofBytes << '\n';

  // Create vector that is simple8b and gap encoded, read encoded vector from
  // file
  std::vector<uint64_t> simple8bEncoded;
  simple8bEncoded.resize(nofBytes / 8);
  textIndexFile.read(simple8bEncoded.data(), nofBytes, from);

  // simple8b decode the list which is then directly passed to
  // gapEncodedVector. The resizing with overhead is necessary for simple8b
  LOG(DEBUG) << "Decoding Simple8b code...\n";
  gapEncodedVector.resize(nofElements + 250);
  ad_utility::Simple8bCode::decode(simple8bEncoded.data(), nofElements,
                                   gapEncodedVector.data());
  LOG(DEBUG) << "Reverting gaps to actual IDs...\n";
  gapEncodedVector.resize(nofElements);
}

}  // namespace
namespace textIndexReadWrite {

/**
 * @brief Writes posting to given file. It splits the vector of postings into
 *        the lists for each respetive tuple element of postings.
 *        The TextRecordIndex list gets gap encoded and then simple8b encoded
 *        before being written to file. The WordIndex and Score lists get
 *        frequency encoded and then simple8b encoded before being written to
 *        file.
 * @param out The file to write to.
 * @param postings The vector of postings to write.
 * @param skipWordlistIfAllTheSame If true, the wordlist is not written to file.
 *                                 This can be done because the WordIndex for
 *                                 the first and last word in a block are saved
 *                                 in the TextBlockMetaData. Always should be
 *                                 false for the entity postings.
 * @param currentOffset The current offset in the file which gets passed by
 *                      reference because it gets updated.
 *
 */
ContextListMetaData writePostings(ad_utility::File& out,
                                  const vector<Posting>& postings,
                                  bool skipWordlistIfAllTheSame,
                                  off_t& currentOffset);

template <typename T>
size_t writeCodebook(const vector<T>& codebook, ad_utility::File& file);

/**
 * @brief Encodes a span of elements and writes the encoded list to file.
 *        Advances the currentOffset by number of bytes written.
 * @param spanToWrite The span of elements to encode and write.
 * @param file The file to write the encoded list to.
 * @param currentOffset The offset that's advanced.
 * @warning The elements of the list have to be able to be cast to uint64_t.
 */
template <typename T>
void encodeAndWriteSpanAndMoveOffset(std::span<const T> spanToWrite,
                                     ad_utility::File& file,
                                     off_t& currentOffset);

/**
 * @brief Reads a frequency encoded list from the given file and casts its
 *        elements to the To type using the given transformer. The From type
 *        specifies the type that was used to create the codebook in the writing
 *        step.
 * @return The decoded list as vector<To>.
 * @param nofElements The number of elements in the list.
 * @param from The offset in the file to start reading from.
 * @param nofBytes The number of bytes to read which can't be deduced from the
 *                 number of elements since the list is simple8b compressed.
 * @param textIndexFile The file to read from.
 * @param transformer The transformer to cast the decoded values to the To type.
 *                    If no transformer is given, a static cast is used.
 */
template <typename To, typename From,
          typename Transformer = decltype(ad_utility::staticCast<To>)>
vector<To> readFreqComprList(size_t nofElements, off_t from, size_t nofBytes,
                             const ad_utility::File& textIndexFile,
                             Transformer transformer = {}) {
  vector<uint64_t> frequencyEncodedVector;
  vector<From> codebook;
  readFreqComprListHelper(nofElements, from, nofBytes, textIndexFile,
                          frequencyEncodedVector, codebook);
  vector<To> result;
  result.reserve(frequencyEncodedVector.size());
  ql::ranges::for_each(frequencyEncodedVector, [&](const auto& encoded) {
    result.push_back(transformer(codebook.at(encoded)));
  });
  LOG(DEBUG) << "Done reading frequency-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

/**
 * @brief Does the same as the other readFreqComprList but writes the decoded
 *        list to the given iterator instead of returning it.
 * @warning The iterator has to be big enough to be increased nofElements times.
 *          (it is increased once more but not written to and then discarded)
 */
template <typename To, typename From, typename OutputIterator,
          typename Transformer = decltype(ad_utility::staticCast<To>)>
void readFreqComprList(OutputIterator iterator, size_t nofElements, off_t from,
                       size_t nofBytes, const ad_utility::File& textIndexFile,
                       Transformer transformer = {}) {
  vector<uint64_t> frequencyEncodedVector;
  vector<From> codebook;
  readFreqComprListHelper(nofElements, from, nofBytes, textIndexFile,
                          frequencyEncodedVector, codebook);
  ql::ranges::for_each(frequencyEncodedVector, [&](const auto& encoded) {
    *iterator = transformer(codebook.at(encoded));
    ++iterator;
  });
  LOG(DEBUG) << "Done reading frequency-encoded list.";
}

/**
 * @brief Reads a gap encoded list from the given file and casts its elements to
 *        the To type using the given transformer. The From type specifies the
 *        type that was used to calculate the gaps in the writing step.
 * @return The decoded list as vector<To>.
 * @param nofElements The number of elements in the list.
 * @param from The offset in the file to start reading from.
 * @param nofBytes The number of bytes to read which can't be deduced from the
 *                 number of elements since the list is simple8b compressed.
 * @param textIndexFile The file to read from.
 * @param transformer The transformer to cast the decoded values to the To type.
 *                    If no transformer is given, a static cast is used.
 */
template <typename To, typename From,
          typename Transformer = decltype(ad_utility::staticCast<To>)>
vector<To> readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                            const ad_utility::File& textIndexFile,
                            Transformer transformer = {}) {
  vector<From> gapEncodedVector;
  readGapComprListHelper(nofElements, from, nofBytes, textIndexFile,
                         gapEncodedVector);

  // Undo gapEncoding
  vector<To> result;
  result.reserve(nofElements);
  From previous = 0;
  for (auto gap : gapEncodedVector) {
    previous += gap;
    result.push_back(transformer(previous));
  }
  LOG(DEBUG) << "Done reading gap-encoded list. Size: " << result.size()
             << "\n";
  return result;
}

/**
 * @brief Does the same as the other readGapComprList but writes the decoded
 *        list to the given iterator instead of returning it.
 * @warning The iterator has to be big enough to be increased nofElements times.
 *          (it is increased once more but not written to and  then discarded)
 */
template <typename To, typename From, typename OutputIterator,
          typename Transformer = decltype(ad_utility::staticCast<To>)>
void readGapComprList(OutputIterator iterator, size_t nofElements, off_t from,
                      size_t nofBytes, const ad_utility::File& textIndexFile,
                      Transformer transformer = {}) {
  vector<From> gapEncodedVector;
  readGapComprListHelper(nofElements, from, nofBytes, textIndexFile,
                         gapEncodedVector);

  // Undo gapEncoding
  From previous = 0;
  for (auto gap : gapEncodedVector) {
    previous += gap;
    *iterator = transformer(previous);
    ++iterator;
  }
  LOG(DEBUG) << "Done reading gap-encoded list.";
}

}  // namespace textIndexReadWrite

/**
 * @brief A class used to encode a view of elements by frequency encoding them.
 *        It does this during the construction of the object and stores the
 *        encoded vector, the codebook and the code map. It also has a method
 *        to write the encoded vector to a file.
 * @warning Cannot deduce type of T from view, so it has to be given as
 *          template.
 */
template <typename T>
class FrequencyEncode {
 public:
  using TypedMap = ad_utility::HashMap<T, size_t>;
  using TypedVector = std::vector<T>;
  using CodeMap = TypedMap;
  using CodeBook = TypedVector;

  // View must be an input range with value type `T`.
  template <typename View>
  explicit FrequencyEncode(View&& view);

  void writeToFile(ad_utility::File& out, off_t& currentOffset);

  const std::vector<size_t>& getEncodedVector() { return encodedVector_; }
  const CodeMap& getCodeMap() const { return codeMap_; }
  const CodeBook& getCodeBook() const { return codeBook_; }

 private:
  std::vector<size_t> encodedVector_;
  CodeMap codeMap_;
  CodeBook codeBook_;
};

/**
 * @brief A class used to encode a view of elements by gap encoding them.
 *        It does this during the construction of the object and stores the
 *        encoded vector. It also has a method to write the encoded vector to
 *        a file.
 * @warning Cannot deduce type of T from view, so it has to be given as
 *          template.
 */
template <typename T>
requires std::is_arithmetic_v<T> class GapEncode {
 public:
  using TypedVector = std::vector<T>;

  // View must be an input range with value type `T`.
  template <typename View>
  explicit GapEncode(View&& view);

  void writeToFile(ad_utility::File& out, off_t& currentOffset);

  const TypedVector& getEncodedVector() const { return encodedVector_; }

 private:
  TypedVector encodedVector_;
};
