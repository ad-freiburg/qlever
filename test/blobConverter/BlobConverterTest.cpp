// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "../util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "blobConverter/BlobConverter.h"
#include "blobConverter/LegacyBlobReader.h"
#include "blobConverter/LegacyDatatype.h"
#include "blobConverter/LegacyEncodedIriManager.h"
#include "global/Constants.h"
#include "index/IndexFormatVersion.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "util/BlankNodeManager.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/json.h"

using namespace qlever::blobConverter;
using namespace testing;
namespace fs = ql::filesystem;

namespace {
// The directory with the sample blobs in the legacy format.
fs::path legacyBlobDirectory() {
  return fs::path{QLEVER_TEST_DATA_DIR} / "oldBlobFormat";
}

// Read the complete contents of the binary file at `path`.
std::vector<char> readBinaryFile(const fs::path& path) {
  auto stream = ad_utility::makeIfstream(path, std::ios::binary);
  return std::vector<char>{std::istreambuf_iterator<char>{stream},
                           std::istreambuf_iterator<char>{}};
}

// A sample blob and its expected contents.
struct SampleBlob {
  std::string filename_;
  size_t numWords_;
};

// The names of the named cached queries that all sample blobs contain.
const std::vector<std::string> expectedEntryNames{
    "boundaries",       "dp-payload", "geos", "lane-payload",
    "road-ref-to-lane", "roadrefs",   "speed"};

// The IRI prefix of the lanes, which are encoded IRIs in all sample blobs.
constexpr std::string_view lanePrefix = "<http://www.bmw.de/FS/Map#lane_";

// The prefixes of the legacy encoded IRIs that actually occur in the cached
// results of every sample blob. The metadata configures nine schemes, but the
// other five (`range_`, `valRange_`, `laneRef_`, `roadRef_`, `stopLoc_`) do not
// occur in the results, so they are covered by the unit tests in
// `LegacyEncodedIriManagerTest.cpp` only.
const std::vector<std::string> expectedEncodedIriPrefixes{
    "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
    "behaviorMap#speedprofile_",
    "<http://www.bmw.de/FS/Map#lane_", "<http://www.bmw.de/FS/Map#roadPart_",
    "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#dp_"};

// The error message with which the legacy reader rejects invalid input.
constexpr std::string_view notALegacyBlob = "not a blob in the legacy format";

// The metadata of the synthetic legacy blobs written by
// `writeHeaderMetadataAndVocabulary` below: all the keys that
// `IndexImpl::applyConfiguration` requires, the uncompressed in-memory
// vocabulary, and a single plain prefix for encoded IRIs.
nlohmann::json syntheticMetadata() {
  return nlohmann::json::parse(R"({
    "encoded-iri-prefixes":{"prefixes-with-leading-angle-brackets":["<http://p/"]},
    "git-hash":"synthetic","has-all-permutations":false,
    "index-format-version":{"date":"2024-10-22","pull-request-number":1572},
    "locale":{"country":"US","ignore-punctuation":false,"language":"en"},
    "num-blank-nodes-total":10,"num-predicates":{"internal":0,"normal":1},
    "num-triples":{"internal":0,"normal":2},
    "vocabulary-type":"in-memory-uncompressed"})");
}

// The (sorted) words of the vocabulary of the synthetic legacy blobs.
const std::vector<std::string> syntheticWords{"\"lit\"", "<http://x/a>",
                                              "<http://x/b>"};

using Writer = ad_utility::serialization::AlignedByteBufferWriteSerializer;

// Write the header, the `metadata`, and an uncompressed in-memory vocabulary
// with the `words` in the legacy format (an independent reimplementation of
// the legacy writer, see `LegacyBlobReader.h` for the format).
void writeHeaderMetadataAndVocabulary(Writer& writer,
                                      const nlohmann::json& metadata,
                                      const std::vector<std::string>& words) {
  writer << std::string{"QLVUBLOB"};
  writer << uint32_t{1};
  writer << metadata.dump();
  VocabularyInMemory::Words compactWords;
  compactWords.build(words);
  writer << VocabularyInMemory{std::move(compactWords)};
}

// A cached result of a synthetic legacy blob.
struct SyntheticEntry {
  std::string name_;
  std::vector<std::string> variables_;
  // One vector of legacy `Id` bits per column.
  std::vector<std::vector<uint64_t>> columns_;
};

// Write the `entry` in the legacy format (with an empty local vocabulary, no
// sort order, and no geo index).
void writeEntry(Writer& writer, const SyntheticEntry& entry) {
  writer << entry.name_;
  writer << std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>{};
  writer << uint64_t{0};
  writer << static_cast<size_t>(entry.columns_.at(0).size());
  writer << entry.columns_.size();
  for (const auto& column : entry.columns_) {
    writer << column;
  }
  writer << entry.variables_.size();
  for (size_t i = 0; i < entry.variables_.size(); ++i) {
    writer << entry.variables_[i];
    writer << ColumnIndexAndTypeInfo{i, ColumnIndexAndTypeInfo::AlwaysDefined};
  }
  writer << std::vector<ColumnIndex>{};
  writer << std::string{"synthetic cache key"};
  writer << false;
}

// Compress the `uncompressed` legacy blob like the legacy writer did: a ZSTD
// frame followed by the uncompressed size.
std::vector<char> compressLegacy(ql::span<const char> uncompressed) {
  auto result = ZstdWrapper::compress(uncompressed.data(), uncompressed.size());
  uint64_t size = uncompressed.size();
  const char* sizeBytes = reinterpret_cast<const char*>(&size);
  result.insert(result.end(), sizeBytes, sizeBytes + sizeof(size));
  return result;
}

// Write a complete synthetic legacy blob (compressed) with the given
// `metadata` and `entries`.
std::vector<char> writeSyntheticLegacyBlob(
    const nlohmann::json& metadata,
    const std::vector<SyntheticEntry>& entries) {
  Writer writer;
  writeHeaderMetadataAndVocabulary(writer, metadata, syntheticWords);
  writer << entries.size();
  for (const auto& entry : entries) {
    writeEntry(writer, entry);
  }
  auto data = std::move(writer).data();
  return compressLegacy(ql::span<const char>{data.data(), data.size()});
}

// The synthetic cached result used by several tests below: two rows and two
// columns, with a `VocabIndex`, an `Int`, an encoded IRI (plain prefix `p`),
// and another `VocabIndex`.
SyntheticEntry syntheticEntry() {
  uint64_t encodedIri = makeLegacyBits(
      LegacyDatatype::EncodedVal,
      encodedIri::encodeDigits("42", EncodedIriManager::NumBitsEncoding));
  return SyntheticEntry{
      "synthetic",
      {"?s", "?o"},
      {{makeLegacyBits(LegacyDatatype::VocabIndex, 1), encodedIri},
       {Id::makeFromInt(5).getBits(),
        makeLegacyBits(LegacyDatatype::VocabIndex, 2)}}};
}

// Return the number of result rows of a TSV result (excluding the header).
size_t numTsvRows(std::string_view tsv) {
  std::vector<std::string_view> lines = absl::StrSplit(tsv, '\n');
  size_t numNonEmpty = 0;
  for (const auto& line : lines) {
    if (!line.empty()) {
      ++numNonEmpty;
    }
  }
  return numNonEmpty - 1;
}

// Return the query that returns the complete named cached result of the
// `entry`. NOTE: The variables have to be selected explicitly, because the
// empty group of the `SERVICE` clause has no visible variables for `SELECT *`.
std::string cachedResultQuery(const LegacyNamedCacheEntry& entry) {
  std::string query = "SELECT";
  for (const auto& [variable, columnInfo] : entry.variables_) {
    absl::StrAppend(&query, " ", variable);
  }
  absl::StrAppend(&query, " WHERE { SERVICE ql:cached-result-with-name-",
                  entry.name_, " {} }");
  return query;
}

// Return the number of `Id`s with the given legacy datatype in the `entry`.
size_t countLegacyDatatype(const LegacyNamedCacheEntry& entry,
                           LegacyDatatype datatype) {
  size_t count = 0;
  for (const auto& column : entry.columns_) {
    for (uint64_t bits : column) {
      if (legacyDatatypeBits(bits) == static_cast<uint64_t>(datatype)) {
        ++count;
      }
    }
  }
  return count;
}

// Return the index metadata JSON that is stored in a `compressedBlob` of the
// current format.
nlohmann::json metadataOfCurrentBlob(ql::span<const char> compressedBlob) {
  auto uncompressed =
      qlever::NamedCachedQueryBlobManager::decompressBlob(compressedBlob, {});
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{uncompressed}};
  qlever::NamedCachedQueryBlobManager::skipAndVerifyBlobHeader(reader);
  std::string metadata;
  reader >> metadata;
  return nlohmann::json::parse(metadata);
}
}  // namespace

class BlobConverterSampleTest : public TestWithParam<SampleBlob> {};

// _____________________________________________________________________________
// The complete end-to-end test on a sample blob: read the legacy blob, convert
// it, load the converted blob into a fresh `Qlever` instance without any index
// on disk, and query all the named cached results.
TEST_P(BlobConverterSampleTest, convertAndQuery) {
  const auto& sample = GetParam();
  auto legacyBytes = readBinaryFile(legacyBlobDirectory() / sample.filename_);
  ASSERT_FALSE(legacyBytes.empty());

  // Read the legacy blob and check its contents.
  auto legacyBlob = readLegacyBlobFromCompressed(legacyBytes);
  EXPECT_EQ(legacyBlob.numWords(), sample.numWords_);
  EXPECT_EQ(legacyBlob.metadata_["vocabulary-type"], "in-memory-compressed");
  EXPECT_EQ(legacyBlob.metadata_["index-format-version"]["pull-request-number"],
            1572);
  std::vector<std::string> entryNames;
  for (const auto& entry : legacyBlob.entries_) {
    entryNames.push_back(entry.name_);
    EXPECT_EQ(entry.columns_.size(), entry.numColumns_);
    EXPECT_EQ(entry.variables_.size(), entry.numColumns_);
    EXPECT_TRUE(entry.blankNodeBlocks_.empty());
    EXPECT_TRUE(entry.localVocabWords_.empty());
    EXPECT_EQ(entry.geoIndex_.has_value(), entry.name_ == "geos");
    EXPECT_GT(entry.numRows_, 0u);
  }
  EXPECT_THAT(entryNames, UnorderedElementsAreArray(expectedEntryNames));
  // The vocabulary consists of IRIs and literals; check a few words.
  EXPECT_THAT(legacyBlob.word(0), AnyOf(StartsWith("<"), StartsWith("\"")));
  EXPECT_THAT(legacyBlob.word(sample.numWords_ - 1),
              AnyOf(StartsWith("<"), StartsWith("\"")));

  // Convert.
  auto result = convertLegacyBlob(legacyBlob);
  ASSERT_FALSE(result.blob_.empty());
  EXPECT_EQ(result.statistics_.numVocabularyWords_, sample.numWords_);
  EXPECT_EQ(result.statistics_.entries_.size(), expectedEntryNames.size());
  EXPECT_THAT(result.statistics_.toString(),
              AllOf(HasSubstr("Vocabulary: "), HasSubstr("\"geos\""),
                    HasSubstr("with a cached geo index"),
                    HasSubstr("Re-encoded IRIs: ")));

  // The metadata of the converted blob.
  auto metadata = metadataOfCurrentBlob(result.blob_);
  EXPECT_EQ(metadata, result.metadata_);
  EXPECT_EQ(metadata["index-format-version"],
            nlohmann::json(qlever::indexFormatVersion));
  EXPECT_TRUE(metadata["encoded-iri-prefixes"].contains("patterns"));
  // The nine legacy configs become ten patterns (`stopLoc_` has two variants),
  // plus the always-on prefix for new graphs of the current format.
  EXPECT_EQ(metadata["encoded-iri-prefixes"]["patterns"].size(), 11u);
  EXPECT_EQ(metadata["vocabulary-type"], "in-memory-compressed");
  EXPECT_EQ(metadata["locale"], legacyBlob.metadata_["locale"]);
  EXPECT_EQ(metadata["num-blank-nodes-total"],
            legacyBlob.metadata_["num-blank-nodes-total"]);

  // Every encoded IRI decodes to the same string in both formats.
  auto legacyManager = LegacyEncodedIriManager::fromJson(
      legacyBlob.metadata_["encoded-iri-prefixes"]);
  auto currentManager = legacyManager.makeCurrentManager();
  size_t numEncodedIris = 0;
  std::vector<std::string> encodedIriPrefixes;
  for (const auto& entry : legacyBlob.entries_) {
    for (const auto& column : entry.columns_) {
      for (uint64_t bits : column) {
        if (legacyDatatypeBits(bits) !=
            static_cast<uint64_t>(LegacyDatatype::EncodedVal)) {
          continue;
        }
        ++numEncodedIris;
        auto tag = legacyDataBits(bits) >> EncodedIriManager::NumBitsEncoding;
        const auto& prefix = legacyManager.configs().at(tag).prefix_;
        if (!ad_utility::contains(encodedIriPrefixes, prefix)) {
          encodedIriPrefixes.push_back(prefix);
        }
        auto converted = convertId(bits, legacyManager, currentManager);
        ASSERT_EQ(converted.getDatatype(), Datatype::EncodedVal);
        ASSERT_EQ(legacyManager.toStringFromLegacyBits(bits),
                  currentManager.toString(converted));
      }
    }
  }
  EXPECT_GT(numEncodedIris, 0u);
  EXPECT_THAT(encodedIriPrefixes,
              UnorderedElementsAreArray(expectedEncodedIriPrefixes));

  // Load the converted blob into a fresh instance and query it.
  qlever::Qlever target{qlever::EngineConfig{}, /*skipLoading=*/true};
  ASSERT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(result.blob_));
  for (const auto& entry : legacyBlob.entries_) {
    auto tsv =
        target.query(cachedResultQuery(entry), ad_utility::MediaType::tsv);
    EXPECT_EQ(numTsvRows(tsv), entry.numRows_) << entry.name_;
    // The header names all the variables.
    std::string_view header = std::string_view{tsv}.substr(0, tsv.find('\n'));
    for (const auto& [variable, columnInfo] : entry.variables_) {
      EXPECT_THAT(header, HasSubstr(variable)) << entry.name_;
    }
    // The re-encoded IRIs are exported with their original prefix.
    if (countLegacyDatatype(entry, LegacyDatatype::EncodedVal) > 0) {
      EXPECT_THAT(tsv, HasSubstr("bmw")) << entry.name_;
    }
  }

  // The `geos` result has the encoded lane IRIs in its `?lane` column, and a
  // constant lane IRI in a query is encoded with the current patterns and
  // matches the converted `Id`s.
  auto geos = target.query(
      "SELECT ?lane WHERE { SERVICE ql:cached-result-with-name-"
      "geos {} } ORDER BY ?lane LIMIT 1",
      ad_utility::MediaType::tsv);
  std::vector<std::string_view> geoLines = absl::StrSplit(geos, '\n');
  ASSERT_GE(geoLines.size(), 2u);
  std::string firstLane{geoLines[1]};
  EXPECT_THAT(firstLane, StartsWith(lanePrefix));
  auto filtered = target.query(
      absl::StrCat("SELECT ?lane WHERE { SERVICE ql:cached-result-with-name-"
                   "geos {} FILTER(?lane = ",
                   firstLane, ") }"),
      ad_utility::MediaType::tsv);
  EXPECT_GE(numTsvRows(filtered), 1u);
  std::vector<std::string_view> filteredLines = absl::StrSplit(filtered, '\n');
  for (size_t i = 1; i < filteredLines.size(); ++i) {
    if (!filteredLines[i].empty()) {
      EXPECT_EQ(filteredLines[i], firstLane);
    }
  }
  // The vocabulary of the blob resolves the `VocabIndex` `Id`s: the `geos`
  // result contains WKT literals.
  auto geometries = target.query(
      "SELECT ?geom WHERE { SERVICE ql:cached-result-with-name-geos {} } LIMIT "
      "1",
      ad_utility::MediaType::tsv);
  EXPECT_THAT(geometries, HasSubstr("LINESTRING"));

  // Also the direct conversion from the compressed bytes yields the same blob
  // contents.
  auto directResult = convertLegacyBlob(ql::span<const char>{legacyBytes});
  EXPECT_EQ(directResult.metadata_, result.metadata_);
  EXPECT_EQ(directResult.statistics_.toString(), result.statistics_.toString());
}

INSTANTIATE_TEST_SUITE_P(BlobConverter, BlobConverterSampleTest,
                         Values(SampleBlob{"623044854.dat", 3605},
                                SampleBlob{"623044857.dat", 779},
                                SampleBlob{"623044860.dat", 1658},
                                SampleBlob{"623044861.dat", 6946}),
                         [](const TestParamInfo<SampleBlob>& info) {
                           return absl::StrCat(
                               "blob", info.param.filename_.substr(0, 9));
                         });

// _____________________________________________________________________________
TEST(BlobConverter, convertId) {
  auto legacyManager = LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
      R"({"prefix-configs":[{"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#range_","specialEncoding":1}]})"));
  auto currentManager = legacyManager.makeCurrentManager();
  auto convert = [&](LegacyDatatype datatype, uint64_t dataBits) {
    return convertId(makeLegacyBits(datatype, dataBits), legacyManager,
                     currentManager);
  };
  // The datatypes up to `LocalVocabIndex` keep their bits.
  EXPECT_EQ(convert(LegacyDatatype::Undefined, 0), Id::makeUndefined());
  EXPECT_EQ(convert(LegacyDatatype::Bool, 1), Id::makeFromBool(true));
  EXPECT_EQ(convert(LegacyDatatype::Int,
                    Id::makeFromInt(-42).getBits() & ValueId::maxIndex),
            Id::makeFromInt(-42));
  EXPECT_EQ(convert(LegacyDatatype::Double,
                    Id::makeFromDouble(3.5).getBits() & ValueId::maxIndex),
            Id::makeFromDouble(3.5));
  EXPECT_EQ(convert(LegacyDatatype::VocabIndex, 17),
            Id::makeFromVocabIndex(VocabIndex::make(17)));
  // The datatypes after `LocalVocabIndex` are shifted by one.
  EXPECT_EQ(convert(LegacyDatatype::BlankNodeIndex, 99),
            Id::makeFromBlankNodeIndex(BlankNodeIndex::make(99)));
  EXPECT_EQ(convert(LegacyDatatype::TextRecordIndex, 5),
            Id::makeFromTextRecordIndex(TextRecordIndex::make(5)));
  EXPECT_EQ(convert(LegacyDatatype::WordVocabIndex, 7),
            Id::makeFromWordVocabIndex(WordVocabIndex::make(7)));
  EXPECT_EQ(convert(LegacyDatatype::Date, 12345).getDatatype(), Datatype::Date);
  EXPECT_EQ(convert(LegacyDatatype::Date, 12345).getBits() & ValueId::maxIndex,
            12345u);
  EXPECT_EQ(convert(LegacyDatatype::GeoPoint, 12345).getDatatype(),
            Datatype::GeoPoint);
  // Encoded IRIs are re-encoded (the legacy layout is `[29][10][11]`, the
  // current one is `[29][8][8]`).
  std::string iri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_536870917_3_7P>";
  uint64_t legacyEncoded = legacyManager.encode(iri).value();
  EXPECT_EQ(legacyEncoded, (5ULL << 21) | (3ULL << 11) | 7ULL);
  auto converted = convert(LegacyDatatype::EncodedVal, legacyEncoded);
  EXPECT_EQ(converted.getDatatype(), Datatype::EncodedVal);
  auto [tag, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(converted);
  // The tag 0 of the current manager is the always-on prefix for new graphs.
  EXPECT_EQ(tag, 1u);
  EXPECT_EQ(payload, (5ULL << 16) | (3ULL << 8) | 7ULL);
  EXPECT_EQ(currentManager.toString(converted), iri);
  // Error cases.
  AD_EXPECT_THROW_WITH_MESSAGE(convert(LegacyDatatype::LocalVocabIndex, 3),
                               HasSubstr("LocalVocabIndex"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertId(13ULL << ValueId::numDataBits, legacyManager, currentManager),
      HasSubstr("datatype 13"));
  // An encoded IRI that the current manager cannot encode: the tag 0 with a
  // payload whose `num2` exceeds 8 bits (which the legacy layout can hold but
  // the legacy encoder never produced).
  AD_EXPECT_THROW_WITH_MESSAGE(
      convert(LegacyDatatype::EncodedVal, 300ULL << 11),
      HasSubstr("cannot be encoded with the patterns of the current format"));
}

// _____________________________________________________________________________
TEST(BlobConverter, convertMetadata) {
  nlohmann::json legacy = nlohmann::json::parse(
      R"({"encoded-iri-prefixes":{"prefix-configs":[{"prefix":"<http://a/"}]},
          "git-hash":"abc","index-format-version":{"date":"2024-10-22","pull-request-number":1572},
          "locale":{"country":"US","ignore-punctuation":false,"language":"en"},
          "vocabulary-type":"in-memory-compressed"})");
  auto legacyManager =
      LegacyEncodedIriManager::fromJson(legacy["encoded-iri-prefixes"]);
  auto current = convertMetadata(legacy, legacyManager.makeCurrentManager());
  EXPECT_EQ(current["index-format-version"],
            nlohmann::json(qlever::indexFormatVersion));
  EXPECT_EQ(current["git-hash"], "abc");
  EXPECT_EQ(current["locale"], legacy["locale"]);
  EXPECT_EQ(current["vocabulary-type"], "in-memory-compressed");
  // Plain prefixes are written in the list format of the current manager,
  // which additionally contains the always-on prefix for new graphs.
  EXPECT_EQ(current["encoded-iri-prefixes"],
            nlohmann::json(legacyManager.makeCurrentManager()));
  EXPECT_THAT(
      current["encoded-iri-prefixes"]["prefixes-with-leading-angle-brackets"],
      UnorderedElementsAre("<http://a/",
                           absl::StrCat("<", QLEVER_NEW_GRAPH_PREFIX)));
}

// _____________________________________________________________________________
TEST(BlobConverter, rejectInvalidInput) {
  // Garbage that is not a ZSTD frame.
  std::vector<char> garbage(100, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE(decompressLegacyBlob(garbage),
                               HasSubstr(notALegacyBlob));
  AD_EXPECT_THROW_WITH_MESSAGE(convertLegacyBlob(ql::span<const char>{garbage}),
                               HasSubstr(notALegacyBlob));
  // Too short for the trailing size.
  std::vector<char> tooShort(3, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE(decompressLegacyBlob(tooShort),
                               HasSubstr("too short"));

  // A valid ZSTD frame, but with a wrong trailing size.
  auto legacyBytes = readBinaryFile(legacyBlobDirectory() / "623044857.dat");
  auto wrongSize = legacyBytes;
  wrongSize.back() ^= 0x01;
  AD_EXPECT_THROW_WITH_MESSAGE(
      decompressLegacyBlob(wrongSize),
      AllOf(HasSubstr("trailing size"),
            Not(HasSubstr("looks like a blob in the current format"))));

  // A blob in the current format (a bare ZSTD frame) is recognized as such.
  auto currentBlob = convertLegacyBlob(ql::span<const char>{legacyBytes}).blob_;
  AD_EXPECT_THROW_WITH_MESSAGE(
      decompressLegacyBlob(currentBlob),
      AllOf(HasSubstr(notALegacyBlob),
            HasSubstr("looks like a blob in the current format (a bare ZSTD "
                      "frame), which needs no conversion")));
  Writer headerWriter;
  qlever::NamedCachedQueryBlobManager::writeBlobHeader(headerWriter);
  auto smallCurrentBlob = qlever::NamedCachedQueryBlobManager::compressBlob(
      ql::span<const char>{std::move(headerWriter).data()});
  AD_EXPECT_THROW_WITH_MESSAGE(
      decompressLegacyBlob(smallCurrentBlob),
      HasSubstr("looks like a blob in the current format"));

  // A truncated compressed file (cut at several positions).
  for (size_t keep : {size_t{20}, size_t{1000}, legacyBytes.size() - 9}) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        decompressLegacyBlob(ql::span<const char>{legacyBytes.data(), keep}),
        HasSubstr(notALegacyBlob));
  }

  // Truncated decompressed contents (cut at several positions: inside the
  // header, inside the metadata, inside the vocabulary, inside the entries,
  // and just before the end).
  auto decompressed = decompressLegacyBlob(legacyBytes);
  EXPECT_NO_THROW(readLegacyBlob(decompressed));
  for (size_t keep : {size_t{10}, size_t{100}, size_t{5000}, size_t{200000},
                      decompressed.size() - 1}) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        readLegacyBlob(ql::span<const char>{decompressed.data(), keep}),
        HasSubstr(notALegacyBlob));
  }

  // Wrong magic header, wrong version, and trailing bytes.
  auto makeHeader = [](std::string magic, uint32_t version) {
    Writer writer;
    writer << magic;
    writer << version;
    return std::move(writer).data();
  };
  auto wrongMagic = makeHeader("QLVXBLOB", 1);
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{wrongMagic}),
      AllOf(HasSubstr(notALegacyBlob),
            HasSubstr("expected the magic header \"QLVUBLOB\"")));
  auto wrongVersion = makeHeader("QLVUBLOB", 2);
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{wrongVersion}),
      HasSubstr("format version 2"));
  auto wrongMagicLength = makeHeader("QLVUBLOBX", 1);
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{wrongMagicLength}),
      HasSubstr("magic header is missing"));
  auto tooShortHeader = makeHeader("", 1);
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{tooShortHeader}),
      HasSubstr("too short for the header"));

  // A valid header, but an unsupported or missing vocabulary type, or invalid
  // JSON.
  auto withMetadata = [](std::string metadata) {
    Writer writer;
    writer << std::string{"QLVUBLOB"};
    writer << uint32_t{1};
    writer << std::move(metadata);
    return std::move(writer).data();
  };
  auto unsupportedVocab =
      withMetadata(R"({"vocabulary-type":"on-disk-compressed"})");
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{unsupportedVocab}),
      HasSubstr("only the types \"in-memory-uncompressed\" and "
                "\"in-memory-compressed\" are supported"));
  auto missingVocabType = withMetadata(R"({"git-hash":"x"})");
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{missingVocabType}),
      HasSubstr("no \"vocabulary-type\" key"));
  auto invalidJson = withMetadata("{not json");
  AD_EXPECT_THROW_WITH_MESSAGE(
      readLegacyBlob(ql::span<const char>{invalidJson}),
      HasSubstr("not valid JSON"));

  // Corrupt huge counts must lead to the descriptive error, and not to a huge
  // allocation: the number of entries, the number of rows of a column, the
  // number of variables, the length of the metadata string, and the number of
  // vocabulary bytes.
  auto hugeCount = uint64_t{1} << 40;
  using OwnedBlocks = std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>;
  auto expectHugeCountError = [&](const Writer::Storage& data,
                                  std::string_view description) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        readLegacyBlob(ql::span<const char>{data.data(), data.size()}),
        AllOf(HasSubstr(notALegacyBlob), HasSubstr(description)));
  };
  {
    Writer writer;
    writeHeaderMetadataAndVocabulary(writer, syntheticMetadata(),
                                     syntheticWords);
    writer << hugeCount;
    expectHugeCountError(std::move(writer).data(),
                         "number of named cached queries");
  }
  {
    Writer writer;
    writeHeaderMetadataAndVocabulary(writer, syntheticMetadata(),
                                     syntheticWords);
    writer << size_t{1};
    writer << std::string{"entry"};
    writer << OwnedBlocks{};
    writer << uint64_t{0};
    writer << hugeCount;
    writer << size_t{1};
    writer << hugeCount;
    expectHugeCountError(std::move(writer).data(), "rows of a column");
  }
  {
    Writer writer;
    writeHeaderMetadataAndVocabulary(writer, syntheticMetadata(),
                                     syntheticWords);
    writer << size_t{1};
    writer << std::string{"entry"};
    writer << OwnedBlocks{};
    writer << uint64_t{0};
    writer << size_t{0};
    writer << size_t{0};
    writer << hugeCount;
    expectHugeCountError(std::move(writer).data(), "number of variables");
  }
  {
    Writer writer;
    writer << std::string{"QLVUBLOB"};
    writer << uint32_t{1};
    writer << hugeCount;
    expectHugeCountError(std::move(writer).data(), "index metadata JSON");
  }
  {
    Writer writer;
    writer << std::string{"QLVUBLOB"};
    writer << uint32_t{1};
    writer << syntheticMetadata().dump();
    writer << hugeCount;
    expectHugeCountError(std::move(writer).data(), "vocabulary bytes");
  }
}

// _____________________________________________________________________________
// Convert a synthetic legacy blob (written by an independent reimplementation
// of the legacy writer, with an uncompressed in-memory vocabulary), load it,
// and query it.
TEST(BlobConverter, syntheticLegacyBlob) {
  auto compressed =
      writeSyntheticLegacyBlob(syntheticMetadata(), {syntheticEntry()});
  auto legacyBlob = readLegacyBlobFromCompressed(compressed);
  EXPECT_EQ(legacyBlob.numWords(), 3u);
  EXPECT_EQ(legacyBlob.word(1), "<http://x/a>");
  ASSERT_EQ(legacyBlob.entries_.size(), 1u);
  EXPECT_EQ(legacyBlob.entries_[0].numRows_, 2u);
  EXPECT_EQ(legacyBlob.entries_[0].cacheKey_, "synthetic cache key");
  ASSERT_NE(legacyBlob.findEntry("synthetic"), nullptr);
  EXPECT_EQ(legacyBlob.findEntry("other"), nullptr);

  auto result = convertLegacyBlob(ql::span<const char>{compressed});
  EXPECT_EQ(result.metadata_["vocabulary-type"], "in-memory-uncompressed");
  EXPECT_EQ(result.statistics_.entries_.at(0).numReencodedIris_, 1u);
  qlever::Qlever target{qlever::EngineConfig{}, /*skipLoading=*/true};
  ASSERT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(result.blob_));
  auto tsv = target.query(
      "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-synthetic {} }",
      ad_utility::MediaType::tsv);
  EXPECT_EQ(tsv, "?s\t?o\n<http://x/a>\t5\n<http://p/42>\t<http://x/b>\n");
  // The plain prefix is recognized for query constants as well.
  auto filtered = target.query(
      "SELECT ?o WHERE { SERVICE ql:cached-result-with-name-synthetic {} "
      "FILTER(?s = <http://p/42>) }",
      ad_utility::MediaType::tsv);
  EXPECT_EQ(filtered, "?o\n<http://x/b>\n");

  // Metadata without any encoded-IRI configuration: the converted blob has
  // only the always-on prefix of the current format.
  auto metadata = syntheticMetadata();
  metadata.erase("encoded-iri-prefixes");
  SyntheticEntry entry = syntheticEntry();
  entry.columns_[0][1] = makeLegacyBits(LegacyDatatype::VocabIndex, 0);
  auto withoutPrefixes = writeSyntheticLegacyBlob(metadata, {entry});
  auto resultWithoutPrefixes =
      convertLegacyBlob(ql::span<const char>{withoutPrefixes});
  EXPECT_TRUE(
      resultWithoutPrefixes.statistics_.legacyEncodedIriConfig_.is_null());
  EXPECT_EQ(resultWithoutPrefixes.metadata_["encoded-iri-prefixes"],
            nlohmann::json(EncodedIriManager{}));
  qlever::Qlever target2{qlever::EngineConfig{}, /*skipLoading=*/true};
  ASSERT_NO_THROW(target2.deserializeVocabAndNamedCacheFromCompressedBlob(
      resultWithoutPrefixes.blob_));
  EXPECT_EQ(target2.query("SELECT ?s WHERE { SERVICE "
                          "ql:cached-result-with-name-synthetic {} }",
                          ad_utility::MediaType::tsv),
            "?s\n<http://x/a>\n\"lit\"\n");
}

// _____________________________________________________________________________
// A cached result with a non-empty local vocabulary or with blank node blocks
// cannot be converted.
TEST(BlobConverter, rejectLocalVocab) {
  auto makeBlob = []() {
    LegacyBlob blob;
    blob.metadata_ = syntheticMetadata();
    LegacyNamedCacheEntry entry;
    entry.name_ = "withLocalVocab";
    entry.numRows_ = 1;
    entry.numColumns_ = 1;
    entry.columns_ = {{Id::makeFromInt(1).getBits()}};
    entry.variables_ = {{"?x", {0, ColumnIndexAndTypeInfo::AlwaysDefined}}};
    blob.entries_.push_back(std::move(entry));
    return blob;
  };
  auto blob = makeBlob();
  EXPECT_NO_THROW(convertLegacyBlob(blob));
  blob.entries_[0].localVocabWords_.emplace_back(0, "\"word\"");
  AD_EXPECT_THROW_WITH_MESSAGE(convertLegacyBlob(blob),
                               HasSubstr("non-empty local vocabulary"));
  auto blobWithBlocks = makeBlob();
  blobWithBlocks.entries_[0].blankNodeBlocks_.emplace_back();
  AD_EXPECT_THROW_WITH_MESSAGE(convertLegacyBlob(blobWithBlocks),
                               HasSubstr("non-empty local vocabulary"));
}
