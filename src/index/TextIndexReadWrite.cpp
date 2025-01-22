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

  GapEncode<uint64_t> textRecordEncoder(
      postings | ql::views::transform([](const Posting& posting) {
        return std::get<0>(posting).get();
      }));
  FrequencyEncode<WordIndex> wordIndexEncoder(
      postings | ql::views::transform([](const Posting& posting) {
        return std::get<1>(posting);
      }));
  FrequencyEncode<Score> scoreEncoder(
      postings | ql::views::transform([](const Posting& posting) {
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
template <typename Numeric>
size_t writeList(std::span<const Numeric> data, ad_utility::File& file) {
  if (data.size() > 0) {
    std::vector<uint64_t> encoded;
    encoded.resize(data.size());
    size_t size = ad_utility::Simple8bCode::encode(data.data(), data.size(),
                                                   encoded.data());
    size_t ret = file.write(encoded.data(), size);
    AD_CONTRACT_CHECK(size == ret);
    return size;
  } else {
    return 0;
  }
}

// ____________________________________________________________________________
template <typename T>
void writeVectorAndMoveOffset(std::span<const T> vectorToWrite,
                              ad_utility::File& file, off_t& currentOffset) {
  size_t bytes = textIndexReadWrite::writeList(vectorToWrite, file);
  currentOffset += bytes;
}

}  // namespace textIndexReadWrite

// ____________________________________________________________________________
template <typename T>
template <typename View>
FrequencyEncode<T>::FrequencyEncode(View&& view) {
  if (ql::ranges::empty(view)) {
    return;
  }
  // Create the frequency map to count how often a certain value of type T
  // appears in the vector
  TypedMap frequencyMap;
  for (const auto& value : view) {
    frequencyMap[value] = 0;
  }
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
  textIndexReadWrite::writeVectorAndMoveOffset(
      std::span<const size_t>(encodedVector_), out, currentOffset);
}

// ____________________________________________________________________________
template <typename T>
requires std::is_arithmetic_v<T> template <typename View>
GapEncode<T>::GapEncode(View&& view) {
  if (ql::ranges::empty(view)) {
    return;
  }
  encodedVector_.reserve(ql::ranges::size(view));
  encodedVector_.push_back(*ql::ranges::begin(view));

  for (auto it1 = ql::ranges::begin(view),
            it2 = std::next(ql::ranges::begin(view));
       it2 != ql::ranges::end(view); ++it1, ++it2) {
    encodedVector_.push_back(*it2 - *it1);
  }
}

// ____________________________________________________________________________
template <typename T>
requires std::is_arithmetic_v<T>
void GapEncode<T>::writeToFile(ad_utility::File& out, off_t& currentOffset) {
  textIndexReadWrite::writeVectorAndMoveOffset(
      std::span<const T>(encodedVector_), out, currentOffset);
}
