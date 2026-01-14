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
#include "util/TransparentFunctors.h"
#include "util/TypeTraits.h"
#include "util/Views.h"

namespace ad_utility {

namespace detail {

constexpr std::array magicBytes{'Q', 'L', 'E', 'V', 'E', 'R', '.',
                                'U', 'P', 'D', 'A', 'T', 'E'};

// The `formatVersion` has to be increased when the format below is changed.
constexpr uint16_t formatVersion = 1;

// Read a value of type T from the `serializer`.
CPP_template(typename T, typename Serializer)(
    requires serialization::ReadSerializer<Serializer>) T
    readValue(Serializer& serializer) {
  T value;
  serializer >> value;
  return value;
}

// Write the header of the file format to the output stream. We are currently at
// version 1.
CPP_template(typename Serializer)(
    requires serialization::WriteSerializer<
        Serializer>) void writeHeader(Serializer& serializer) {
  serializer << magicBytes;
  serializer << formatVersion;
}

// Read the header of the file format from the input stream and ensure that it
// is correct.
CPP_template(typename Serializer)(
    requires serialization::ReadSerializer<
        Serializer>) void readHeader(Serializer& serializer) {
  std::decay_t<decltype(magicBytes)> magicByteBuffer{};
  serializer >> magicByteBuffer;
  AD_CORRECTNESS_CHECK(magicByteBuffer == magicBytes);
  uint16_t version;
  serializer >> version;
  AD_CORRECTNESS_CHECK(
      version == formatVersion,
      "The format version for serialized triples (e.g. persisted UPDATEs or "
      "serialized cached results) in the version of QLever is ",
      formatVersion, " but you tried to read serialized triples with version ",
      version,
      ", As those features are currently still experimental, please contact "
      "the developers of QLever");
}

// Serialize the local vocabulary to the output stream. Returns a mapping from
// Ids that map to their string position in the file.
CPP_template(typename Serializer)(
    requires serialization::WriteSerializer<
        Serializer>) void serializeLocalVocab(Serializer& serializer,
                                              const LocalVocab& vocab) {
  serializer << vocab.getOwnedLocalBlankNodeBlocks();
  uint64_t numWords = vocab.size();
  serializer << numWords;

  auto writeWordSet = [&serializer](const auto& words) {
    ql::ranges::for_each(words, [&serializer](const auto& localVocabEntry) {
      serializer << Id::makeFromLocalVocabIndex(&localVocabEntry);
      serializer << localVocabEntry.toStringRepresentation();
    });
  };
  writeWordSet(vocab.primaryWordSet());
  ql::ranges::for_each(vocab.otherSets(), writeWordSet,
                       ad_utility::dereference);
}

// Deserialize the local vocabulary from the input stream.
CPP_template(typename Serializer)(
    requires serialization::ReadSerializer<Serializer>) std::
    tuple<LocalVocab, absl::flat_hash_map<Id::T, Id>> deserializeLocalVocab(
        Serializer& serializer, BlankNodeManager* blankNodeManager) {
  LocalVocab vocab;
  vocab.reserveBlankNodeBlocksFromExplicitIndices(
      readValue<std::vector<
          BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>(
          serializer),
      blankNodeManager);
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
  if constexpr (std::ranges::contiguous_range<std::decay_t<Range>>) {
    serializer << ql::span{range};
  } else {
    ad_utility::serialization::VectorIncrementalSerializer<Id, Serializer>
        vectorSerializer{std::move(serializer)};
    for (const Id& value : range) {
      vectorSerializer.push(value);
    }
    vectorSerializer.finish();
    serializer = std::move(vectorSerializer).serializer();
  }
}

// TODO<joka921> Comments.
inline void remapLocalVocab(ql::span<Id> ids,
                            const absl::flat_hash_map<Id::T, Id>& mapping) {
  for (Id& id : ids) {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      id = mapping.at(id.getBits());
    }
  }
}

// Deserialize a range of Ids from the input stream. If an Id is of type
// LocalVocabIndex, apply the mapping to the Id after reading it.
template <typename Serializer>
void deserializeIds(Serializer& serializer,
                    const absl::flat_hash_map<Id::T, Id>& mapping,
                    ql::span<Id> ids) {
  serializer >> ids;
  remapLocalVocab(ids, mapping);
}
// Deserialize a range of Ids from the input stream. If an Id is of type
// LocalVocabIndex, apply the mapping to the Id after reading it.
template <typename Serializer>
std::vector<Id> deserializeIds(Serializer& serializer,
                               const absl::flat_hash_map<Id::T, Id>& mapping) {
  std::vector<Id> ids = readValue<std::vector<Id>>(serializer);
  remapLocalVocab(ids, mapping);
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
  auto [vocab, mapping] =
      detail::deserializeLocalVocab(serializer, blankNodeManager);
  std::vector<std::vector<Id>> idVectors;
  auto numRanges = detail::readValue<uint64_t>(serializer);
  for ([[maybe_unused]] auto i : ad_utility::integerRange(numRanges)) {
    idVectors.push_back(detail::deserializeIds(serializer, mapping));
  }
  return {std::move(vocab), std::move(idVectors)};
}
}  // namespace ad_utility
