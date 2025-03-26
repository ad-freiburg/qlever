//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <type_traits>

#include "backports/concepts.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ad_utility {

namespace detail {

constexpr std::string_view magicBytes = "QLEVER";

template <typename T>
CPP_concept isTrivialValue =
    std::is_trivially_constructible_v<T> && std::is_trivially_copyable_v<T> &&
    std::is_trivially_copy_constructible_v<T> &&
    std::is_trivially_move_constructible_v<T> &&
    std::is_trivially_copy_assignable_v<T> &&
    std::is_trivially_move_assignable_v<T> &&
    std::is_trivially_destructible_v<T>;

// Write a value of type T to the output stream.
CPP_template(typename T)(requires isTrivialValue<T>) void writeBytes(
    std::ostream& os, const T& type) {
  os.write(reinterpret_cast<const char*>(&type), sizeof(T));
}

// Read a value of type T from the input stream.
CPP_template(typename T)(requires isTrivialValue<T>) T
    readBytes(std::istream& is) {
  T value;
  is.read(reinterpret_cast<char*>(&value), sizeof(T));
  return value;
}

// Write the header of the file format to the output stream. We are currently at
// version 0.
inline void writeHeader(std::ostream& os) {
  // Magic bytes to identify the file format.
  os.write(magicBytes.data(), magicBytes.size());
  // Version number of the file format.
  uint16_t version = 0;
  writeBytes(os, version);
}

// Read the header of the file format from the input stream and ensure that it
// is correct.
inline void readHeader(std::istream& is) {
  auto magicBuffer = readBytes<std::array<char, magicBytes.size()>>(is);
  AD_CORRECTNESS_CHECK(
      (std::string_view{magicBuffer.data(), magicBuffer.size()}) == magicBytes);
  uint16_t version = readBytes<uint16_t>(is);
  AD_CORRECTNESS_CHECK(version == 0);
}

// Serialize the local vocabulary to the output stream. Returns a mapping from
// Ids that map to their string position in the file.
inline void serializeLocalVocab(std::ostream& os, const LocalVocab& vocab) {
  AD_CONTRACT_CHECK(vocab.numSets() == 1);
  const auto& words = vocab.primaryWordSet();
  uint64_t size = words.size();
  writeBytes(os, size);

  for (const auto& localVocabEntry : words) {
    const auto& entryString = localVocabEntry.toStringRepresentation();
    writeBytes(os, Id::makeFromLocalVocabIndex(&localVocabEntry).getBits());
    // Make sure to write the null terminator as well.
    os.write(entryString.c_str(), entryString.size() + 1);
  }
}

// Deserialize the local vocabulary from the input stream.
inline std::tuple<LocalVocab, absl::flat_hash_map<Id::T, Id>>
deserializeLocalVocab(std::istream& is) {
  LocalVocab vocab;
  uint64_t size = readBytes<uint64_t>(is);
  AD_CORRECTNESS_CHECK(size > 0);
  absl::flat_hash_map<Id::T, Id> mapping{};
  mapping.reserve(size);
  for (uint64_t i = 0; i < size; ++i) {
    Id::T bits = readBytes<Id::T>(is);
    AD_CORRECTNESS_CHECK(Id::fromBits(bits).getDatatype() ==
                         Datatype::LocalVocabIndex);
    std::string value;
    std::getline(is, value, '\0');
    AD_CORRECTNESS_CHECK(is);
    auto localVocabIndex = vocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(std::move(value)));
    mapping.emplace(bits, Id::makeFromLocalVocabIndex(localVocabIndex));
  }
  return {std::move(vocab), std::move(mapping)};
}

// Serialize a range of Ids to the output stream. If an Id is of type
// LocalVocabIndex, apply the mapping to the Id before writing it.
CPP_template(typename Range)(
    requires ql::ranges::sized_range<Range>) void serializeIds(std::ostream& os,
                                                               Range&& range) {
  static_assert(sizeof(std::streamoff) == sizeof(LocalVocabIndex));
  uint64_t idCount = ql::ranges::size(range);
  // Store the size of the triples.
  writeBytes(os, idCount);
  for (const Id& value : range) {
    writeBytes(os, value.getBits());
  }
}

// Deserialize a range of Ids from the input stream. If an Id is of type
// LocalVocabIndex, apply the mapping to the Id after reading it.
CPP_template(typename BlankNodeFunc)(
    requires ad_utility::InvocableWithConvertibleReturnType<BlankNodeFunc,
                                                            BlankNodeIndex>)
    std::vector<Id> deserializeIds(
        std::istream& is, const absl::flat_hash_map<Id::T, Id>& mapping,
        BlankNodeFunc newBlankNodeIndex) {
  uint64_t idCount = readBytes<uint64_t>(is);
  std::vector<Id> ids;
  ids.reserve(idCount);
  absl::flat_hash_map<Id, BlankNodeIndex> blankNodeMapping;
  for (uint64_t i = 0; i < idCount; ++i) {
    Id id = Id::fromBits(readBytes<Id::T>(is));
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      ids.push_back(mapping.at(id.getBits()));
    } else if (id.getDatatype() == Datatype::BlankNodeIndex) {
      BlankNodeIndex index = blankNodeMapping.contains(id)
                                 ? blankNodeMapping[id]
                                 : (blankNodeMapping[id] = newBlankNodeIndex());
      ids.push_back(Id::makeFromBlankNodeIndex(index));
    } else {
      ids.push_back(id);
    }
  }
  return ids;
}
}  // namespace detail

// Serialize the local vocabulary and the given ranges of Ids to the given path.
CPP_template(typename Range)(
    requires ql::ranges::range<
        Range>) void serializeIds(const std::filesystem::path& path,
                                  const LocalVocab& vocab, Range&& idRanges) {
  std::ofstream os{path, std::ios::binary};
  detail::writeHeader(os);
  detail::serializeLocalVocab(os, vocab);
  for (const auto& ids : idRanges) {
    detail::serializeIds(os, ids);
  }
}

inline std::tuple<LocalVocab, std::vector<std::vector<Id>>> deserializeIds(
    const std::filesystem::path& path, BlankNodeManager* blankNodeManager) {
  std::ifstream is{path, std::ios::binary};
  if (!is) {
    return {LocalVocab{}, std::vector<std::vector<Id>>{}};
  }
  detail::readHeader(is);
  auto [vocab, mapping] = detail::deserializeLocalVocab(is);
  std::vector<std::vector<Id>> idVectors;
  while (is.peek() != std::char_traits<char>::eof()) {
    idVectors.push_back(
        detail::deserializeIds(is, mapping, [blankNodeManager, &vocab]() {
          return vocab.getBlankNodeIndex(blankNodeManager);
        }));
  }
  return {std::move(vocab), std::move(idVectors)};
}
}  // namespace ad_utility
