// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexReadWrite.h"

namespace textIndexReadWrite {

// ____________________________________________________________________________
ContextListMetaData writePostings(ad_utility::File& out,
                                  const vector<Posting>& postings,
                                  bool skipWordlistIfAllTheSame,
                                  off_t& currentOffset) {
  ContextListMetaData meta;
  meta._nofElements = postings.size();
  if (meta._nofElements == 0) {
    meta._startContextlist = currentOffset;
    meta._startWordlist = currentOffset;
    meta._startScorelist = currentOffset;
    meta._lastByte = currentOffset - 1;
    return meta;
  }

  GapEncode textRecordEncoder(postings |
                              ql::views::transform([](const Posting& posting) {
                                return std::get<0>(posting).get();
                              }));
  FrequencyEncode wordIndexEncoder(
      postings | ql::views::transform([](const Posting& posting) {
        return std::get<1>(posting);
      }));
  FrequencyEncode scoreEncoder(postings |
                               ql::views::transform([](const Posting& posting) {
                                 return std::get<2>(posting);
                               }));

  meta._startContextlist = currentOffset;
  textRecordEncoder.writeToFile(out, currentOffset);

  meta._startWordlist = currentOffset;
  if (!skipWordlistIfAllTheSame || wordIndexEncoder.getCodeBook().size() > 1) {
    wordIndexEncoder.writeToFile(out, currentOffset);
  }

  meta._startScorelist = currentOffset;
  scoreEncoder.writeToFile(out, currentOffset);

  meta._lastByte = currentOffset - 1;

  return meta;
}

// ____________________________________________________________________________
template <typename T>
size_t writeCodebook(const vector<T>& codebook, ad_utility::File& file) {
  size_t byteSizeOfCodebook = sizeof(T) * codebook.size();
  file.write(&byteSizeOfCodebook, sizeof(byteSizeOfCodebook));
  file.write(codebook.data(), byteSizeOfCodebook);
  return byteSizeOfCodebook + sizeof(byteSizeOfCodebook);
}

// ____________________________________________________________________________
template <typename T>
void encodeAndWriteSpanAndMoveOffset(std::span<const T> spanToWrite,
                                     ad_utility::File& file,
                                     off_t& currentOffset) {
  size_t bytes = 0;
  if (spanToWrite.size() > 0) {
    std::vector<uint64_t> encoded;
    encoded.resize(spanToWrite.size());
    bytes = ad_utility::Simple8bCode::encode(
        spanToWrite.data(), spanToWrite.size(), encoded.data());
    size_t ret = file.write(encoded.data(), bytes);
    AD_CONTRACT_CHECK(bytes == ret);
  }
  currentOffset += bytes;
}

}  // namespace textIndexReadWrite

// ____________________________________________________________________________
template <typename T>
template <typename View>
void FrequencyEncode<T>::initialize(View&& view) {
  if (ql::ranges::empty(view)) {
    return;
  }
  // Create the frequency map to count how often a certain value of type T
  // appears in the vector
  TypedMap frequencyMap;
  for (const auto& value : view) {
    ++frequencyMap[value];
  }

  // Convert the hashmap to a vector to sort it by most frequent first
  std::vector<std::pair<T, size_t>> frequencyVector;
  frequencyVector.reserve(frequencyMap.size());
  ql::ranges::copy(frequencyMap, std::back_inserter(frequencyVector));
  ql::ranges::sort(frequencyVector, [](const auto& a, const auto& b) {
    return a.second > b.second;
  });

  // Write the codeBook and codeMap
  // codeBook contains all values of type T exactly ones, sorted by frequency
  // descending
  // codeMap maps all values of type T that where in the vector to their
  // position in the codeBook
  codeBook_.reserve(frequencyVector.size());
  codeMap_.reserve(frequencyVector.size());
  size_t i = 0;
  for (const auto& frequencyPair : frequencyVector) {
    codeBook_.push_back(frequencyPair.first);
    codeMap_[frequencyPair.first] = i;
    ++i;
  }

  // Finally encode the vector
  encodedVector_.reserve(ql::ranges::size(view));
  for (const auto& value : view) {
    encodedVector_.push_back(codeMap_[value]);
  }
}

// ____________________________________________________________________________
template <typename T>
void FrequencyEncode<T>::writeToFile(ad_utility::File& out,
                                     off_t& currentOffset) {
  currentOffset += textIndexReadWrite::writeCodebook(codeBook_, out);
  textIndexReadWrite::encodeAndWriteSpanAndMoveOffset<size_t>(
      encodedVector_, out, currentOffset);
}

// ____________________________________________________________________________
template <typename T>
template <typename View>
void GapEncode<T>::initialize(View&& view) {
  if (ql::ranges::empty(view)) {
    return;
  }
  encodedVector_.reserve(ql::ranges::size(view));
  std::adjacent_difference(ql::ranges::begin(view), ql::ranges::end(view),
                           std::back_inserter(encodedVector_));
}

// ____________________________________________________________________________
template <typename T>
void GapEncode<T>::writeToFile(ad_utility::File& out, off_t& currentOffset) {
  textIndexReadWrite::encodeAndWriteSpanAndMoveOffset<T>(encodedVector_, out,
                                                         currentOffset);
}
