//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>

#include <array>
#include <filesystem>
#include <fstream>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/type_traits.h"
#include "engine/LocalVocab.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeString.h"
#include "util/TypeTraits.h"
#include "util/Views.h"

namespace ad_utility {

namespace detail {

constexpr std::array magicBytes{'Q', 'L', 'E', 'V', 'E', 'R', '.',
                                'U', 'P', 'D', 'A', 'T', 'E'};

// Read a value of type T from the `serializer`.
CPP_template(typename T, typename Serializer)(
    requires serialization::ReadSerializer<Serializer>) T
    readValue(Serializer& serializer) {
  T value;
  serializer >> value;
  return value;
}

// Write the header of the file format to the output stream. We are currently at
// version 0.
CPP_template(typename Serializer)(
    requires serialization::WriteSerializer<
        Serializer>) void writeHeader(Serializer& serializer) {
  serializer << magicBytes;
  uint16_t version = 0;
  serializer << version;
}

// Read the header of the file format from the input stream and ensure that it
// is correct.
CPP_template(typename Serializer)(
    requires serialization::ReadSerializer<
        Serializer>) void readHeader(Serializer& serializer) {
  std::decay_t<typeof(magicBytes)> magicByteBuffer{};
  serializer >> magicByteBuffer;
  AD_CORRECTNESS_CHECK(magicByteBuffer == magicBytes);
  uint16_t version;
  serializer >> version;
  AD_CORRECTNESS_CHECK(version == 0);
}

// Serialize the local vocabulary to the output stream. Returns a mapping from
// Ids that map to their string position in the file.
CPP_template(typename Serializer)(
    requires serialization::WriteSerializer<
        Serializer>) void serializeLocalVocab(Serializer& serializer,
                                              const LocalVocab& vocab) {
  AD_CONTRACT_CHECK(vocab.numSets() == 1);
  const auto& words = vocab.primaryWordSet();
  serializer << words.size();
  ql::ranges::for_each(words, [&serializer](const auto& localVocabEntry) {
    serializer << Id::makeFromLocalVocabIndex(&localVocabEntry);
    serializer << localVocabEntry.toStringRepresentation();
  });
}

// Deserialize the local vocabulary from the input stream.
CPP_template(typename Serializer)(
    requires serialization::ReadSerializer<Serializer>) std::
    tuple<LocalVocab, absl::flat_hash_map<Id::T, Id>> deserializeLocalVocab(
        Serializer& serializer) {
  LocalVocab vocab;
  auto size = readValue<uint64_t>(serializer);
  // Note:: It might happen that the `size` is zero because the local vocab was
  // empty.
  absl::flat_hash_map<Id::T, Id> mapping{};
  mapping.reserve(size);
  for (uint64_t i = 0; i < size; ++i) {
    auto id = readValue<Id::T>(serializer);
    auto s = readValue<std::string>(serializer);
    auto localVocabIndex = vocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(std::move(s)));
    mapping.emplace(id, Id::makeFromLocalVocabIndex(localVocabIndex));
  }
  return {std::move(vocab), std::move(mapping)};
}

// Serialize a range of Ids to the output stream. If an Id is of type
// LocalVocabIndex, apply the mapping to the Id before writing it.
CPP_template(typename Range, typename Serializer)(
    requires ql::ranges::range<Range>) void serializeIds(Serializer& serializer,
                                                         Range&& range) {
  ad_utility::serialization::VectorIncrementalSerializer<Id, Serializer>
      vectorSerializer{std::move(serializer)};
  for (const Id& value : range) {
    vectorSerializer.push(value);
  }
  vectorSerializer.finish();
  serializer = std::move(vectorSerializer).serializer();
}

// Deserialize a range of Ids from the input stream. If an Id is of type
// LocalVocabIndex, apply the mapping to the Id after reading it.
CPP_template(typename Serializer, typename BlankNodeFunc)(
    requires ad_utility::InvocableWithConvertibleReturnType<BlankNodeFunc,
                                                            BlankNodeIndex>)
    std::vector<Id> deserializeIds(
        Serializer& serializer, const absl::flat_hash_map<Id::T, Id>& mapping,
        BlankNodeFunc newBlankNodeIndex) {
  std::vector<Id> ids = readValue<std::vector<Id>>(serializer);
  absl::flat_hash_map<Id, BlankNodeIndex> blankNodeMapping;
  for (Id& id : ids) {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      id = mapping.at(id.getBits());
    } else if (id.getDatatype() == Datatype::BlankNodeIndex) {
      BlankNodeIndex index = blankNodeMapping.contains(id)
                                 ? blankNodeMapping[id]
                                 : (blankNodeMapping[id] = newBlankNodeIndex());
      id = Id::makeFromBlankNodeIndex(index);
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
  serialization::FileWriteSerializer serializer{path.c_str()};
  detail::writeHeader(serializer);
  detail::serializeLocalVocab(serializer, vocab);
  serializer << uint64_t{ql::ranges::size(idRanges)};
  for (const auto& ids : idRanges) {
    detail::serializeIds(serializer, ids);
  }
}

inline std::tuple<LocalVocab, std::vector<std::vector<Id>>> deserializeIds(
    const std::filesystem::path& path, BlankNodeManager* blankNodeManager) {
  // This is a minor TOCTOU issue, the file might be gone after this check and
  // before the call to `fopen`, done by `FileReadSerializer`, so ideally we'd
  // handle this as a special exception type of our own `File` class, which
  // doesn't exist yet.
  if (!std::filesystem::exists(path)) {
    return {};
  }
  auto serializer = [p = path.c_str()]() {
    try {
      return serialization::FileReadSerializer{p};
    } catch (const std::runtime_error& err) {
      throw std::runtime_error{absl::StrCat(
          "The file '", p,
          "' exists, but cannot be opened for reading. Please check the file "
          "permissions. The error received when opening it was: ",
          err.what())};
    }
  }();
  AD_LOG_INFO << "Reading and processing persisted updates from " << path
              << " ..." << std::endl;
  detail::readHeader(serializer);
  auto [vocab, mapping] = detail::deserializeLocalVocab(serializer);
  std::vector<std::vector<Id>> idVectors;
  auto numRanges = detail::readValue<uint64_t>(serializer);
  for ([[maybe_unused]] auto i : ad_utility::integerRange(numRanges)) {
    idVectors.push_back(detail::deserializeIds(
        serializer, mapping, [blankNodeManager, &vocab]() {
          return vocab.getBlankNodeIndex(blankNodeManager);
        }));
  }
  return {std::move(vocab), std::move(idVectors)};
}
}  // namespace ad_utility
