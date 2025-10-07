// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#include "engine/BinaryExport.h"

#include <absl/functional/bind_front.h>

#include "ExportQueryExecutionTrees.h"
// TODO<joka921> Can we get further type erasure here to not include this
// detail?
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/http/HttpClient.h"

using CancellationHandle = ad_utility::SharedCancellationHandle;
namespace ad_utility::serialization {
// TODO<joka921> Move this to a global file.
template <typename F>
class ReadViaCallableSerializer {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  F readFunction_;

 public:
  explicit ReadViaCallableSerializer(F readFunction)
      : readFunction_{std::move(readFunction)} {};
  void serializeBytes(char* bytePointer, size_t numBytes) {
    readFunction_(bytePointer, numBytes);
  }

  ReadViaCallableSerializer(const ReadViaCallableSerializer&) noexcept = delete;
  ReadViaCallableSerializer& operator=(const ReadViaCallableSerializer&) =
      delete;
  ReadViaCallableSerializer(ReadViaCallableSerializer&&) noexcept = default;
  ReadViaCallableSerializer& operator=(ReadViaCallableSerializer&&) noexcept =
      default;
};
}  // namespace ad_utility::serialization
namespace {
// Return a `std::string_view` wrapping the passed value.
std::string_view raw(const std::integral auto& value) {
  return std::string_view{reinterpret_cast<const char*>(&value), sizeof(value)};
}

// Return true iff the value can be serialized without a vocab entry.
AD_ALWAYS_INLINE auto isTrivial(Id id) {
  auto datatype = id.getDatatype();
  return datatype == Datatype::Undefined || datatype == Datatype::Bool ||
         datatype == Datatype::Int || datatype == Datatype::Double ||
         datatype == Datatype::Date || datatype == Datatype::GeoPoint ||
         datatype == Datatype::EncodedVal;
}
}  // namespace

namespace qlever::binary_export {
// _____________________________________________________________________________
std::string StringMapping::flush(const Index& index) {
  numProcessedRows_ = 0;
  std::vector<std::string> sortedStrings;
  sortedStrings.resize(stringMapping_.size());
  for (auto& [oldId, newId] : stringMapping_) {
    auto type = oldId.getDatatype();
    if (type == Datatype::LocalVocabIndex) {
      sortedStrings[newId] =
          oldId.getLocalVocabIndex()->toStringRepresentation();
    } else {
      // TODO<joka921, RobinTF>, make the list exhaustive.
      AD_CONTRACT_CHECK(type == Datatype::VocabIndex);
      // TODO<joka921> Deduplicate on the level of IDs for the string mapping.
      sortedStrings[newId] = index.indexToString(oldId.getVocabIndex());
    }
  }
  stringMapping_.clear();

  std::string result;
  // Rough estimate
  result.reserve(sortedStrings.size() * 100);

  for (const std::string& string : sortedStrings) {
    absl::StrAppend(&result, raw(string.size()), string);
  }

  return result;
}

// _____________________________________________________________________________
Id StringMapping::remapId(Id id) {
  size_t distinctIndex = 0;
  if (stringMapping_.contains(id)) {
    distinctIndex = stringMapping_.at(id);
  } else {
    distinctIndex = stringMapping_[id] = stringMapping_.size();
  }
  // The shift is required to imitate the unused bits of a pointer.
  return Id::makeFromLocalVocabIndex(
      reinterpret_cast<LocalVocabIndex>(distinctIndex << Id::numDatatypeBits));
}

// _____________________________________________________________________________
AD_ALWAYS_INLINE Id
toExportableId(Id originalId, [[maybe_unused]] const LocalVocab& localVocab,
               StringMapping& stringMapping) {
  if (isTrivial(originalId)) {
    return originalId;
  } else {
    // TODO<joka921, RobinTF>, make the list exhaustive.
    return stringMapping.remapId(originalId);
  }
}

void writeHeader(auto& serializer, const auto& qet, const auto& columns) {
  // Magic bytes
  serializer << "QLEVER.EXPORT"sv;
  // Export format version.
  serializer << uint16_t{0};

  // Export encoded values.
  const auto& prefixes = qet.getQec()->getIndex().encodedIriManager().prefixes_;
  serializer << prefixes;
  serializer << columns;
}

auto readHeader(auto& serializer) {
  // If we don't get the magic bytes this is not a QLever instance on the other
  // end.
  std::string magicBytes;
  serializer >> magicBytes;
  AD_CONTRACT_CHECK(std::string_view(magicBytes.data(), magicBytes.size()) ==
                    "QLEVER.EXPORT");

  uint16_t version;
  serializer >> version;
  // We only support version 0.
  AD_CONTRACT_CHECK(version == 0);

  std::vector<std::string> prefixes;
  serializer >> prefixes;

  QueryExecutionTree::ColumnIndicesAndTypes columns;
  serializer >> columns;
  // TODO<joka921> only serialize the variable names when exporting.
  std::vector<std::string> variableNames;
  for (auto& opt : columns) {
    variableNames.push_back(std::move(opt.value().variable_));
  }

  return std::pair{std::move(prefixes), std::move(variableNames)};
}

// Use special undefined value that's not actually used as a real value.
static constexpr Id::T vocabMarker = Id::makeUndefined().getBits() + 0b10101010;
static_assert(Id::fromBits(vocabMarker).getDatatype() == Datatype::Undefined);

// _____________________________________________________________________________
ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle) {
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();
  AD_LOG_DEBUG << "Starting binary export..." << std::endl;

  ad_utility::serialization::ByteBufferWriteSerializer serializer{};

  using namespace std::string_view_literals;
  // Get all columns with defined variables.
  QueryExecutionTree::ColumnIndicesAndTypes columns =
      qet.selectedVariablesToColumnIndices(selectClause, false);
  std::erase(columns, std::nullopt);

  writeHeader(serializer, qet, columns);

  // TODO<joka921> Use serialization for additional stuff.
  co_yield std::string_view{serializer.data().data(), serializer.data().size()};
  serializer.clear();

  // Maps strings to reusable ids.
  StringMapping stringMapping;

  // Iterate over the result and yield the bindings.
  uint64_t resultSize = 0;
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           limitAndOffset, *result, resultSize)) {
    for (uint64_t i : range) {
      for (const auto& column : columns) {
        Id id = pair.idTable_(i, column->columnIndex_);
        co_yield raw(
            toExportableId(id, pair.localVocab_, stringMapping).getBits());
      }
      if (stringMapping.needsFlush()) {
        co_yield raw(vocabMarker);
        co_yield stringMapping.flush(qet.getQec()->getIndex());
        co_yield raw(static_cast<size_t>(0));
      }
      cancellationHandle->throwIfCancelled();
      stringMapping.nextRow();
    }
  }

  std::string trailingVocab = stringMapping.flush(qet.getQec()->getIndex());
  if (!trailingVocab.empty()) {
    co_yield raw(vocabMarker);
    co_yield trailingVocab;
    co_yield raw(static_cast<size_t>(0));
  }

  // If there are no variables, just export the total number of rows.
  if (columns.empty()) {
    co_yield raw(resultSize);
  }
}
namespace {
template <typename T>
T read(auto& it, const auto& end) {
  T buffer;
  std::span bufferView{reinterpret_cast<char*>(&buffer), sizeof(T)};
  for (char& byte : bufferView) {
    if (it == end) {
      throw std::runtime_error{"Stream ended unexpectedly."};
    }
    byte = static_cast<char>(*it);
    ++it;
  }
  return buffer;
}

std::string readString(auto& it, const auto& end) {
  std::string buffer;
  buffer.resize(read<size_t>(it, end));
  for (char& byte : buffer) {
    if (it == end) {
      throw std::runtime_error{"Stream ended unexpectedly."};
    }
    byte = static_cast<char>(*it);
    ++it;
  }
  return buffer;
}

template <typename It, typename End>
struct IteratorReader {
  It it;
  End end;

  void operator()(char* target, size_t numBytes) {
    for (size_t i = 0; i < numBytes; ++i) {
      AD_CORRECTNESS_CHECK(it != end);
      *target = absl::bit_cast<char>(*it);
      ++it, ++target;
    }
  }
};

auto readVectorOfStrings(auto& it, auto end) {
  std::vector<std::string> transmittedStrings;
  std::string current;
  while (!(current = readString(it, end)).empty()) {
    transmittedStrings.emplace_back(std::move(current));
  }
  return transmittedStrings;
}

void rewriteVocabIds(IdTable& result, const size_t dirtyIndex, const auto& qec,
                     auto& vocab, const auto& transmittedStrings) {
  for (auto col : result.getColumns()) {
    ql::ranges::for_each(
        col.subspan(dirtyIndex), [&qec, &vocab, &transmittedStrings](Id& id) {
          if (id.getDatatype() == Datatype::LocalVocabIndex) {
            // Undo the shift done during encoding.
            auto literalOrIri = ad_utility::triple_component::LiteralOrIri::
                fromStringRepresentation(transmittedStrings.at(
                    reinterpret_cast<size_t>(id.getLocalVocabIndex()) >>
                    Id::numDatatypeBits));
            auto tc = [&]() {
              if (literalOrIri.isIri()) {
                return TripleComponent{std::move(literalOrIri.getIri())};
              } else {
                AD_CORRECTNESS_CHECK(literalOrIri.isLiteral());
                return TripleComponent{std::move(literalOrIri.getLiteral())};
              }
            };
            id = tc().toValueId(qec.getIndex().getVocab(), vocab,
                                qec.getIndex().encodedIriManager());
          }
        });
  }
}

Id toIdImpl(const auto& qec, const auto& prefixes, const auto& prefixMapping,
            auto& vocab, Id::T bits) {
  // TODO<RobinTF> check local vocab for id conversion. Also the strings are
  // transmitted after the ids, so we might need to search `result` for
  // changes.
  Id id = Id::fromBits(bits);
  if (id.getDatatype() == Datatype::EncodedVal) {
    // TODO<RobinTF> This is basically EncodedIriManager::toString copy
    // pasted.
    static constexpr auto mask =
        ad_utility::bitMaskForLowerBits(EncodedIriManager::NumBitsEncoding);
    auto digitEncoding = id.getEncodedVal() & mask;
    // Get the index of the prefix.
    auto prefixIdx = id.getEncodedVal() >> EncodedIriManager::NumBitsEncoding;
    if (prefixMapping.contains(prefixIdx)) {
      return Id::makeFromEncodedVal(
          digitEncoding | (static_cast<uint64_t>(prefixMapping.at(prefixIdx))
                           << EncodedIriManager::NumBitsEncoding));
    }
    std::string result;
    const auto& prefix = prefixes.at(prefixIdx);
    result.reserve(prefix.size() + EncodedIriManager::NumDigits + 1);
    result = prefix;
    EncodedIriManager::decodeDecimalFrom64Bit(result, digitEncoding);
    result.push_back('>');
    return TripleComponent{
        ad_utility::triple_component::Iri::fromStringRepresentation(
            std::move(result))}
        .toValueId(qec.getIndex().getVocab(), vocab,
                   qec.getIndex().encodedIriManager());
  }
  // TODO<RobinTF> Add assertion that type is either trivial or local vocab
  // index here.
  return id;
};

auto getPrefixMapping(const auto& qec, const auto& prefixes) {
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping;
  const auto& localPrefixes = qec.getIndex().encodedIriManager().prefixes_;
  for (const auto& [index, prefix] : ::ranges::views::enumerate(prefixes)) {
    auto prefixIt = ql::ranges::find(localPrefixes, prefix);
    if (prefixIt != localPrefixes.end()) {
      prefixMapping[index] = static_cast<uint8_t>(
          ql::ranges::distance(localPrefixes.begin(), prefixIt));
    }
  }
  return prefixMapping;
}

}  // namespace

// _____________________________________________________________________________
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<ColumnIndex> resultSortedOn) {
  // TODO<RobinTF> honor laziness setting.
  (void)requestLaziness;
  auto bytes = response.body_ | ql::views::join;
  auto it = ql::ranges::begin(bytes);
  auto end = ql::ranges::end(bytes);

  IteratorReader<decltype(it), decltype(end)> itReader{it, end};
  ad_utility::serialization::ReadViaCallableSerializer serializer{
      std::ref(itReader)};

  auto [prefixes, variableNames] = readHeader(serializer);

  auto prefixMapping = getPrefixMapping(qec, prefixes);

  auto numColumns = variableNames.size();

  // Done with the serializer for now, reextracting the iterator.
  it = itReader.it;

  IdTable result{numColumns, qec.getAllocator()};

  // Special case 0 columns. In this case just return the correct amount of
  // columns.
  if (variableNames.empty()) {
    auto numRows = read<uint64_t>(it, end);
    result.resize(numRows);
    return Result{std::move(result), std::move(resultSortedOn), LocalVocab{}};
  }

  // TODO<RobinTF> check if variable names do actually match expected names.

  LocalVocab vocab;

  auto toId = [&qec, &prefixes, &vocab, &prefixMapping](Id::T bits) mutable {
    return toIdImpl(qec, prefixes, prefixMapping, vocab, bits);
  };

  // At which index we need to start converting values.
  size_t dirtyIndex = 0;

  while (it != end) {
    auto firstValue = read<Id::T>(it, end);
    if (firstValue == vocabMarker) {
      auto transmittedStrings = readVectorOfStrings(it, end);
      rewriteVocabIds(result, dirtyIndex, qec, vocab, transmittedStrings);
      dirtyIndex = result.size();
    } else {
      result.emplace_back();
      result.at(result.size() - 1, 0) = toId(firstValue);
      for ([[maybe_unused]] auto colIndex : ql::views::iota(1u, numColumns)) {
        result.at(result.size() - 1, colIndex) = toId(read<Id::T>(it, end));
      }
    }
  }

  return Result{std::move(result), std::move(resultSortedOn), std::move(vocab)};
}
}  // namespace qlever::binary_export
