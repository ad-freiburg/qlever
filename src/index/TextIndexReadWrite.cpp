// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexReadWrite.h"

#include "util/Simple8bCode.h"

namespace TextIndexReadWrite {

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

  auto firstElements =
      postings | std::ranges::views::transform([](const Posting& posting) {
        return std::get<0>(posting).get();
      });

  auto secondElements =
      postings | std::ranges::views::transform([](const Posting& posting) {
        return std::get<1>(posting);
      });

  auto thirdElements =
      postings | std::ranges::views::transform([](const Posting& posting) {
        return std::get<2>(posting);
      });

  std::vector<uint64_t> textRecordList(firstElements.begin(),
                                       firstElements.end());
  std::vector<WordIndex> wordIndexList(secondElements.begin(),
                                       secondElements.end());
  std::vector<Score> scoreList(thirdElements.begin(), thirdElements.end());

  GapEncode<uint64_t> textRecordEncoder(textRecordList);
  FrequencyEncode<WordIndex> wordIndexEncoder(wordIndexList);
  FrequencyEncode<Score> scoreEncoder(scoreList);

  meta._startContextlist = currentOffset;
  textRecordEncoder.writeToFile(out, meta._nofElements, currentOffset);

  meta._startWordlist = currentOffset;
  if (!skipWordlistIfAllTheSame || wordIndexEncoder.getCodeBook().size() > 1) {
    wordIndexEncoder.writeToFile(out, meta._nofElements, currentOffset);
  }

  meta._startScorelist = currentOffset;
  scoreEncoder.writeToFile(out, meta._nofElements, currentOffset);

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
size_t writeList(Numeric* data, size_t nofElements, ad_utility::File& file) {
  if (nofElements > 0) {
    uint64_t* encoded = new uint64_t[nofElements];
    size_t size = ad_utility::Simple8bCode::encode(data, nofElements, encoded);
    size_t ret = file.write(encoded, size);
    AD_CONTRACT_CHECK(size == ret);
    delete[] encoded;
    return size;
  } else {
    return 0;
  }
}

// ____________________________________________________________________________
template <typename T>
void writeVectorAndMoveOffset(const std::vector<T>& vectorToWrite,
                              size_t nofElements, ad_utility::File& file,
                              off_t& currentOffset) {
  T* vectorAsList = new T[vectorToWrite.size()];
  std::copy(vectorToWrite.begin(), vectorToWrite.end(), vectorAsList);
  size_t bytes = TextIndexReadWrite::writeList(vectorAsList, nofElements, file);
  currentOffset += bytes;
  delete[] vectorAsList;
}

}  // namespace TextIndexReadWrite

// ____________________________________________________________________________
template <typename T>
FrequencyEncode<T>::FrequencyEncode(const TypedVector& vectorToEncode) {
  if (vectorToEncode.size() == 0) {
    return;
  }
  // Create the frequency map to count how often a certain value of type T
  // appears in the vector
  TypedMap frequencyMap;
  for (const auto& value : vectorToEncode) {
    frequencyMap[value] = 0;
  }
  for (const auto& value : vectorToEncode) {
    ++frequencyMap[value];
  }

  // Convert the hashmap to a vector to sort it by most frequent first
  std::vector<std::pair<T, size_t>> frequencyVector;
  frequencyVector.reserve(frequencyMap.size());
  std::transform(frequencyMap.begin(), frequencyMap.end(),
                 std::back_inserter(frequencyVector),
                 [](const auto& kv) { return kv; });
  std::sort(frequencyVector.begin(), frequencyVector.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });

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
  encodedVector_.reserve(vectorToEncode.size());
  for (const auto& value : vectorToEncode) {
    encodedVector_.push_back(codeMap_[value]);
  }
}

// ____________________________________________________________________________
template <typename T>
void FrequencyEncode<T>::writeToFile(ad_utility::File& out, size_t nofElements,
                                     off_t& currentOffset) {
  currentOffset += TextIndexReadWrite::writeCodebook(codeBook_, out);
  TextIndexReadWrite::writeVectorAndMoveOffset(encodedVector_, nofElements, out,
                                               currentOffset);
}

// ____________________________________________________________________________
template <typename T>
requires std::is_arithmetic_v<T>
GapEncode<T>::GapEncode(const TypedVector& vectorToEncode) {
  if (vectorToEncode.size() == 0) {
    return;
  }
  encodedVector_.reserve(vectorToEncode.size());
  encodedVector_.push_back(vectorToEncode.at(0));

  for (auto it1 = vectorToEncode.begin(),
            it2 = std::next(vectorToEncode.begin());
       it2 != vectorToEncode.end(); ++it1, ++it2) {
    encodedVector_.push_back(*it2 - *it1);
  }
}

// ____________________________________________________________________________
template <typename T>
requires std::is_arithmetic_v<T>
void GapEncode<T>::writeToFile(ad_utility::File& out, size_t nofElements,
                               off_t& currentOffset) {
  TextIndexReadWrite::writeVectorAndMoveOffset(encodedVector_, nofElements, out,
                                               currentOffset);
}
