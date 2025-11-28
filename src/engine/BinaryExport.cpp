// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#include "engine/BinaryExport.h"

#include <absl/functional/bind_front.h>

#include "engine/ExportQueryExecutionTrees.h"
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
}  // namespace

namespace qlever::binary_export {

// _____________________________________________________________________________
bool BinaryExportHelpers::isTrivial(Id id) {
  auto datatype = id.getDatatype();
  return datatype == Datatype::Undefined || datatype == Datatype::Bool ||
         datatype == Datatype::Int || datatype == Datatype::Double ||
         datatype == Datatype::Date || datatype == Datatype::GeoPoint ||
         datatype == Datatype::EncodedVal;
}

// _____________________________________________________________________________
std::string StringMapping::flush(const Index& index) {
  LocalVocab dummy;
  numProcessedRows_ = 0;
  std::vector<std::string> sortedStrings;
  sortedStrings.resize(stringMapping_.size());
  for (auto& [oldId, newId] : stringMapping_) {
    auto literalOrIri =
        ExportQueryExecutionTrees::idToLiteralOrIri(index, oldId, dummy, true);
    AD_CORRECTNESS_CHECK(literalOrIri.has_value());
    sortedStrings[newId] =
        std::move(literalOrIri.value().toStringRepresentation());
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
  static constexpr std::array allowedDatatypes{
      Datatype::VocabIndex, Datatype::LocalVocabIndex,
      Datatype::TextRecordIndex, Datatype::WordVocabIndex};
  AD_EXPENSIVE_CHECK(ad_utility::contains(allowedDatatypes, id.getDatatype()));
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
  if (BinaryExportHelpers::isTrivial(originalId) ||
      originalId.getDatatype() == Datatype::BlankNodeIndex) {
    return originalId;
  } else {
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
  AD_CONTRACT_CHECK(magicBytes == "QLEVER.EXPORT");

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
template <typename It, typename End>
struct IteratorReader {
  It it;
  End end;

  void operator()(char* target, size_t numBytes) {
    for (size_t i = 0; i < numBytes; ++i) {
      AD_CORRECTNESS_CHECK(it != end);
      *target = static_cast<char>(*it);
      ++it;
      ++target;
    }
  }
};
}  // namespace

// _____________________________________________________________________________
void BinaryExportHelpers::rewriteVocabIds(
    IdTable& result, const size_t dirtyIndex, const QueryExecutionContext& qec,
    LocalVocab& vocab, const std::vector<std::string>& transmittedStrings) {
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

// _____________________________________________________________________________
Id BinaryExportHelpers::toIdImpl(
    const QueryExecutionContext& qec, const std::vector<std::string>& prefixes,
    const ad_utility::HashMap<uint8_t, uint8_t>& prefixMapping,
    LocalVocab& vocab, Id::T bits,
    ad_utility::HashMap<Id::T, Id>& blankNodeMapping) {
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
  if (id.getDatatype() == Datatype::BlankNodeIndex) {
    auto [it, inserted] = blankNodeMapping.try_emplace(bits, ValueId{});

    if (inserted) {
      it->second = Id::makeFromBlankNodeIndex(
          vocab.getBlankNodeIndex(qec.getIndex().getBlankNodeManager()));
    }
    return it->second;
  }
  AD_EXPENSIVE_CHECK(isTrivial(id) ||
                     id.getDatatype() == Datatype::LocalVocabIndex);
  return id;
}

// _____________________________________________________________________________
ad_utility::HashMap<uint8_t, uint8_t> BinaryExportHelpers::getPrefixMapping(
    const QueryExecutionContext& qec,
    const std::vector<std::string>& prefixes) {
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

  auto prefixMapping = BinaryExportHelpers::getPrefixMapping(qec, prefixes);

  auto numColumns = variableNames.size();

  // Done with the serializer for now, reextracting the iterator.
  it = itReader.it;

  IdTable result{numColumns, qec.getAllocator()};

  // Special case 0 columns. In this case just return the correct amount of
  // columns.
  if (variableNames.empty()) {
    auto numRows = BinaryExportHelpers::read<uint64_t>(it, end);
    result.resize(numRows);
    return Result{std::move(result), std::move(resultSortedOn), LocalVocab{}};
  }

  // TODO<RobinTF> check if variable names do actually match expected names.

  LocalVocab vocab;
  ad_utility::HashMap<Id::T, Id> blankNodeMapping;

  auto toId = [&qec, &prefixes, &vocab, &prefixMapping,
               &blankNodeMapping](Id::T bits) mutable {
    return BinaryExportHelpers::toIdImpl(qec, prefixes, prefixMapping, vocab,
                                         bits, blankNodeMapping);
  };

  // At which index we need to start converting values.
  size_t dirtyIndex = 0;

  while (it != end) {
    auto firstValue = BinaryExportHelpers::read<Id::T>(it, end);
    if (firstValue == vocabMarker) {
      auto transmittedStrings =
          BinaryExportHelpers::readVectorOfStrings(it, end);
      BinaryExportHelpers::rewriteVocabIds(result, dirtyIndex, qec, vocab,
                                           transmittedStrings);
      dirtyIndex = result.size();
    } else {
      result.emplace_back();
      result.at(result.size() - 1, 0) = toId(firstValue);
      for ([[maybe_unused]] auto colIndex : ql::views::iota(1u, numColumns)) {
        result.at(result.size() - 1, colIndex) =
            toId(BinaryExportHelpers::read<Id::T>(it, end));
      }
    }
  }

  return Result{std::move(result), std::move(resultSortedOn), std::move(vocab)};
}
}  // namespace qlever::binary_export
