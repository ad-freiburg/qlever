// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/GeoCellGrid.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/TypeTraits.h"
#include "util/Views.h"
#include "util/serializer/Serializer.h"

namespace sj {
class GeometryStore;
struct WriteBatch;
}  // namespace sj

// A `GeoVocabulary` holds Well-Known Text (WKT) literals. In contrast to the
// regular vocabulary classes it does not only store the strings. Instead it
// stores both preprocessed and original forms of its input words. Preprocessing
// includes for example the computation of bounding boxes for accelerated
// spatial queries. See the `GeometryInfo` class for details.
//
// NOTE: A `GeoVocabulary` is only suitable for WKT literals, therefore it
// should be used as part of a `SplitVocabulary`.
//
// With a `GeoCellGrid` (see `setGeoCellGrid`), the index of a word is no
// longer its position in the vocabulary: the upper bits of the index hold the
// grid cell of the word, the lower `GeoCellGrid::numPositionBits()` bits hold
// the position. The words must then arrive at the `WordWriter` ordered by
// their grid cell (a follow-up change lets the `TripleComponentComparator`
// produce this order), so that all words of one cell form a contiguous index
// range that can be computed from the cell index alone. This is the basis for
// the geo cell prefilter of spatial joins. The grid itself is not stored by
// this class; the index configuration provides it before the vocabulary is
// opened.
//
// If enabled (see `setBuildGeometryStore`), both writers also parse every
// valid WKT literal into a persistent `sj::GeometryStore` of `libspatialjoin`
// (files with the `geometryStoreInfix`), under a geometry ID (`gid`) derived
// from its position (see `geometryStoreGid`), and store the ID of the parsed
// geometry in its `GeometryInfo` (see
// `GeometryInfo::getGeometryStoreId`). A spatial join can then use the parsed
// geometries without parsing the literals again.
template <typename UnderlyingVocabulary>
class GeoVocabulary {
 private:
  using GeometryInfo = ad_utility::GeometryInfo;
  using GeoCellGrid = ad_utility::GeoCellGrid;

  // The index of a word is computed from its position in the underlying
  // vocabulary (see `indexFromPosition`), so the positions must be contiguous.
  // Add a vocabulary type here only if that holds for it (it does not for the
  // vocabularies with "holes", see `VocabularyInMemoryBinSearch`).
  static_assert(
      ad_utility::SameAsAny<UnderlyingVocabulary, VocabularyInMemory,
                            CompressedVocabulary<VocabularyInternalExternal>>);

  // The underlying vocabulary, which stores the WKT literals as strings.
  UnderlyingVocabulary literals_;

  // The file in which the additional information on the geometries (like
  // bounding box) is stored.
  ad_utility::File geoInfoFile_;

  // The grid, or `std::nullopt` if the index of a word is its position.
  std::optional<GeoCellGrid> grid_;

  // Whether the writers parse the geometries into the `sj::GeometryStore`.
  bool buildGeometryStore_ = false;

  // See `endIndex`, computed once when the vocabulary is opened.
  uint64_t endIndex_ = 0;

  // The `GeometryInfo` version of the opened `geoInfoFile_`, see
  // `GEOMETRY_INFO_VERSION` for the versions that can be read.
  uint64_t geoInfoVersion_ = ad_utility::GEOMETRY_INFO_VERSION;

  // TODO<ullingerc> Possibly add in-memory cache of bounding boxes here

  // Filename suffix for geometry information file
  static constexpr std::string_view geoInfoSuffix = ".geoinfo";

  // Filename infix of the files of the `sj::GeometryStore`, which are named
  // by the `libspatialjoin` type of the geometries they hold.
  static constexpr std::string_view geometryStoreInfix = ".geostore.";
  static constexpr std::array<std::string_view, 6> geometryStoreTypes{
      "points", "areas", "simpleareas", "lines", "simplelines", "multis"};

  // Offset per index inside the geometry information file of the current
  // version. When reading a file of an older version, the offset is
  // `geometryInfoSize(geoInfoVersion_)`.
  static constexpr size_t geoInfoOffset = sizeof(GeometryInfo);

  // Serialized version of `GeometryInfo`
  using GeometryInfoBuffer = std::array<uint8_t, geoInfoOffset>;

  // For an invalid WKT literal, the serialized geometry info is all-zero
  static constexpr GeometryInfoBuffer invalidGeoInfoBuffer = {};

  // Offset for the header of the geometry information file
  static constexpr size_t geoInfoHeader =
      sizeof(ad_utility::GEOMETRY_INFO_VERSION);

  // Compute the serialized `GeometryInfo` of the WKT literal `word`, or the
  // `invalidGeoInfoBuffer` if the `word` cannot be parsed. Increment the
  // respective counter for an invalid geometry and for a polygon whose area
  // could not be computed. This is the per-word work that the `WordWriter` and
  // the `BlockWriter` share.
  static GeometryInfoBuffer computeGeoInfoRecord(
      std::string_view word, std::atomic<size_t>& numInvalidGeometries,
      std::atomic<size_t>& numInvalidPolygonArea);

  // The geometry info records of consecutive words, starting at
  // `firstPosition_`, together with their parsed geometries, which are not yet
  // added to the `sj::GeometryStore` (see `parseGeometries`).
  struct ComputedBatch {
    uint64_t firstPosition_;
    std::vector<GeometryInfoBuffer> records_;
    std::unique_ptr<sj::WriteBatch> geometries_;
  };

  // Create the `sj::GeometryStore` that the writers of the vocabulary with the
  // given `filename` write to.
  static std::unique_ptr<sj::GeometryStore> makeGeometryStoreForWriting(
      const std::string& filename);

  // Parse the WKT literals `words` at the consecutive positions starting at
  // `firstPosition` for the `store`, each under its `geometryStoreGid`.
  // Skip the invalid geometries, whose `records` are the
  // `invalidGeoInfoBuffer`, and all geometries if not `buildGeometryStore`.
  // This is the expensive part of the work for the `store`, so it is
  // thread-safe.
  template <typename Words>
  static std::unique_ptr<sj::WriteBatch> parseGeometries(
      bool buildGeometryStore, sj::GeometryStore& store, const Words& words,
      uint64_t firstPosition, ql::span<const GeometryInfoBuffer> records);

  // Add the parsed geometries of the `batch` to the `store` and write the ID
  // of each of them to its record.
  //
  // NOTE: The store assigns the IDs in the order of the calls, so the writers
  // call this in the order of the positions, for a deterministic index.
  static void addToGeometryStore(sj::GeometryStore& store,
                                 ComputedBatch& batch);

 public:
  // The constructor is defined in the `.cpp` file, where it checks the
  // underlying vocabulary type of the explicit instantiations (the check
  // cannot be in this header, because the header of the concept it uses
  // includes the `SplitVocabulary`, which includes this class).
  GeoVocabulary();

  // Load the precomputed `GeometryInfo` object for the literal with
  // the given index from disk. Return `std::nullopt` for invalid geometries.
  std::optional<GeometryInfo> getGeoInfo(uint64_t index) const {
    return geoInfoAtPosition(positionFromIndex(index));
  }

  // Construct a filename for the geo info file by appending a suffix to the
  // given filename.
  static std::string getGeoInfoFilename(std::string_view filename) {
    return absl::StrCat(filename, geoInfoSuffix);
  }

  // The prefix of the files of the `sj::GeometryStore` (see the class
  // comment) of the vocabulary with the given filename.
  static std::string getGeometryStorePrefix(std::string_view filename) {
    return absl::StrCat(filename, geometryStoreInfix);
  }

  // The `gid` of the word at the given position in the `sj::GeometryStore`,
  // which `libspatialjoin` reports for the results of a spatial join.
  //
  // NOTE: The prefix `v` distinguishes these `gid`s from those of geometries
  // that a spatial join parses itself (for example literals from a
  // `LocalVocab`), which are plain numbers.
  static std::string geometryStoreGid(uint64_t position) {
    return absl::StrCat("v", position);
  }

  // The position of the word with the given `gid` (see `geometryStoreGid`),
  // or `std::nullopt` if the `gid` is not of this form.
  static std::optional<uint64_t> positionOfGeometryStoreGid(
      std::string_view gid);

  // Set the grid. Must be called before `open` (when loading a vocabulary
  // that was built with this grid) or before `makeDiskWriterPtr` (when
  // building one). Changing the grid of an opened vocabulary would change the
  // meaning of all its indices, so this is an error.
  void setGeoCellGrid(std::optional<GeoCellGrid> grid) {
    AD_CONTRACT_CHECK(!geoInfoFile_.isOpen(),
                      "The geo cell grid must be set before the vocabulary is "
                      "opened");
    grid_ = grid;
  }

  // The grid, or `std::nullopt` if the index of a word is its position.
  const std::optional<GeoCellGrid>& getGeoCellGrid() const { return grid_; }

  // Let the writers created after this call parse the geometries into the
  // `sj::GeometryStore` (see the class comment). Without this, the files of
  // the store are empty.
  //
  // NOTE: This is opt-in, because it is expensive for very large polygons
  // (the box IDs of `libspatialjoin`).
  void setBuildGeometryStore(bool buildGeometryStore) {
    buildGeometryStore_ = buildGeometryStore;
  }

  // The position in the vocabulary of the word with the given index (the
  // identity without a grid).
  uint64_t positionFromIndex(uint64_t index) const {
    return grid_.has_value() ? grid_->positionOfIndex(index) : index;
  }

  // The index of the word at the given position (the identity without a
  // grid). The grid cell of the word is recomputed from its precomputed
  // bounding box, or from the word itself if the geometry is invalid, by the
  // same rule that the `WordWriter` used to assign it.
  uint64_t indexFromPosition(uint64_t position, std::string_view word) const;

  // The index that is larger than the index of every word in this vocabulary
  // (used to represent "past the end" positions of binary searches, and as
  // the upper bound for valid indices).
  uint64_t endIndex() const { return endIndex_; }

  // The standard vocabulary operations. They translate between indices and
  // positions (see above) and otherwise forward to the underlying vocabulary,
  // see there for details.

  // ___________________________________________________________________________
  decltype(auto) operator[](uint64_t id) const {
    auto position = positionFromIndex(id);
    AD_CONTRACT_CHECK(position < size());
    return literals_[position];
  }

  //____________________________________________________________________________
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const;

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(VocabLookupInput input) const {
    return ad_utility::vocabulary::lookupBatchesStreamed(*this,
                                                         std::move(input));
  }

  // Iterate over all words together with their index.
  //
  // NOTE: With a grid, this reads the geometry info of every word from disk
  // to compute its index, one small read per word. If a scan of a large
  // vocabulary with a grid ever matters, read the geometry info in blocks.
  auto scanAll() const {
    return ad_utility::OwningView{literals_.scanAll()} |
           ql::views::transform([this](IndexAndWord indexAndWord) {
             indexAndWord.index_ =
                 indexFromPosition(indexAndWord.index_, indexAndWord.word_);
             return indexAndWord;
           });
  }

  // ___________________________________________________________________________
  [[nodiscard]] uint64_t size() const { return literals_.size(); }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return withIndexFromPosition(literals_.lower_bound(word, comparator));
  }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return withIndexFromPosition(literals_.upper_bound(word, comparator));
  }

  // ___________________________________________________________________________
  UnderlyingVocabulary& getUnderlyingVocabulary() { return literals_; }

  // ___________________________________________________________________________
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return literals_;
  }

  // ___________________________________________________________________________
  void open(const std::string& filename);

  // Custom word writer, which precomputes and writes geometry info along with
  // the words. With a grid, it also checks that the words arrive ordered by
  // cell and puts the cell index into the upper bits of the returned indices.
  //
  // The `GeometryInfo` of a word (which for a large polygon costs a parse and
  // several s2 computations) is not computed on the calling thread, but in
  // batches on the global thread pool (see `util/GlobalExecutor.h`): the
  // position of a word in the `geoInfoFile_` is known as soon as the word
  // arrives, so its record can be written whenever its batch is done. The
  // calling thread only computes the cell of a word (with a grid), which
  // needs no more than the bounding box.
  class WordWriter : public WordWriterBase {
   private:
    // The writer of the underlying vocabulary, which stores the strings.
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    // The file for the geometry info, one record per word.
    ad_utility::File geoInfoFile_;
    // The parsed geometries, see the class comment.
    std::unique_ptr<sj::GeometryStore> geometryStore_;
    bool buildGeometryStore_;
    // The grid, or `std::nullopt` if the index of a word is its position.
    std::optional<GeoCellGrid> grid_;
    // The cell of the previous word (only with a grid), to check the order.
    std::optional<GeoCellGrid::CellIndex> lastCellIndex_;
    // The number of words written so far (the position of the next word).
    uint64_t numWords_ = 0;
    // Counters for the warnings that `finishImpl` prints. Atomic, because the
    // batches on the thread pool increment them.
    std::atomic<size_t> numInvalidGeometries_ = 0;
    std::atomic<size_t> numInvalidPolygonArea_ = 0;
    // The words whose `GeometryInfo` has not been computed yet, and the
    // position (in the `geoInfoFile_`) of the first of them.
    std::vector<std::string> currentBatch_;
    uint64_t firstPositionOfCurrentBatch_ = 0;
    size_t currentBatchSize_ = 0;
    // The batches that are being computed on the thread pool, oldest first.
    std::deque<std::future<ComputedBatch>> pendingBatches_;

   public:
    // Initialize the `geoInfoFile_` by writing its header and open a word
    // writer on the underlying vocabulary.
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename, std::optional<GeoCellGrid> grid,
               bool buildGeometryStore);

    // Add the next literal to the vocabulary, precompute additional information
    // using `GeometryInfo` and return the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on the underlying writer and close the `geoInfoFile_`
    // file handle. After this no more calls to `operator()` are allowed.
    void finishImpl() override;

    ~WordWriter() override;

   private:
    // Hand the `currentBatch_` to the thread pool, which computes the
    // `GeometryInfo` of its words and parses them. Before that, write the
    // oldest pending batches if too many of them are in flight, so that the
    // memory stays bounded and an exception from a batch does not go unnoticed
    // for long.
    void flushBatch();

    // Wait for the oldest pending batch, add its geometries to the
    // `geometryStore_` and write its records to the `geoInfoFile_`.
    void writeOldestBatch();
  };

  // The block-wise counterpart of the `WordWriter` (see `BlockWriterBase`),
  // which writes the same files. The geometry info record of a word is
  // computed by `precomputePayload` and arrives with the block as its payload.
  // The append step writes the records of a block to their position in the
  // `geoInfoFile_`, by a task on the global thread pool.
  //
  // NOTE: With a grid, `indexOf` computes the cell of a word from the literal
  // (exactly as `WordWriter::operator()`). The order of the cells is checked
  // in `prepare` within a block and in the append step across blocks.
  class BlockWriter : public BlockWriterBase {
   private:
    std::unique_ptr<BlockWriterBase> underlyingWriter_;
    ad_utility::File geoInfoFile_;
    std::unique_ptr<sj::GeometryStore> geometryStore_;
    bool buildGeometryStore_;
    std::optional<GeoCellGrid> grid_;
    std::optional<GeoCellGrid::CellIndex> lastCellIndex_;
    uint64_t numWords_ = 0;
    // Counters for the warnings that `finishImpl` prints. Atomic, because
    // `precomputePayload` runs on any thread.
    mutable std::atomic<size_t> numInvalidGeometries_ = 0;
    mutable std::atomic<size_t> numInvalidPolygonArea_ = 0;
    // The writes of the records that are running on the pool, oldest first;
    // bounded, see `appendRecords`.
    std::deque<std::future<void>> pendingWrites_;

   public:
    // Constructor, see `WordWriter`.
    BlockWriter(const UnderlyingVocabulary& vocabulary,
                const std::string& filename, std::optional<GeoCellGrid> grid,
                bool buildGeometryStore);

    size_t blockSize() const override { return underlyingWriter_->blockSize(); }
    uint64_t indexOf(uint64_t position, std::string_view word) const override;
    // The geometry info record of a word, see `precomputePayload`.
    size_t precomputedPayloadSize() const override { return geoInfoOffset; }
    void precomputePayload(std::string_view word,
                           ql::span<char> payload) const override;
    AppendBlock prepare(WordBlock block) override;

    // Finish if that has not happened yet, so that no write is left running
    // on the pool.
    ~BlockWriter() override;

   private:
    void finishImpl() override;

    // The append step of a block (see `prepare`): check the cells of its
    // first and last word (only with a grid) against the previous block,
    // append the block to the underlying vocabulary (via `appendUnderlying`),
    // add the geometries of the `batch` to the `geometryStore_`, and write its
    // records to their position in the `geoInfoFile_`.
    void appendRecords(ComputedBatch batch,
                       std::optional<GeoCellGrid::CellIndex> firstCellIndex,
                       std::optional<GeoCellGrid::CellIndex> lastCellIndex,
                       AppendBlock appendUnderlying);
  };

  // The files of the underlying vocabulary, which is stored under the base
  // filename itself, plus the file with the geometry information and the
  // files of the `sj::GeometryStore`.
  static FileSuffixes fileSuffixes() {
    FileSuffixes suffixes = UnderlyingVocabulary::fileSuffixes();
    // NOTE: The explicit `std::string` avoids a false positive
    // `-Warray-bounds` of GCC 13 for `emplace_back(geoInfoSuffix)`.
    suffixes.push_back(std::string{geoInfoSuffix});
    for (std::string_view type : geometryStoreTypes) {
      suffixes.push_back(absl::StrCat(geometryStoreInfix, type));
    }
    return suffixes;
  }

  // ___________________________________________________________________________
  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(literals_, filename, grid_,
                                        buildGeometryStore_);
  }

  // ___________________________________________________________________________
  std::unique_ptr<BlockWriterBase> makeBlockWriterPtr(
      const std::string& filename) const {
    return std::make_unique<BlockWriter>(literals_, filename, grid_,
                                         buildGeometryStore_);
  }

  // ___________________________________________________________________________
  void close();

  // Generic serialization support.
  AD_SERIALIZE_FRIEND_FUNCTION(GeoVocabulary) {
    (void)serializer;
    (void)arg;
    throw std::runtime_error(
        "Generic serialization is not implemented for GeoVocabulary.");
  }

 private:
  // The grid cell of a word, given its precomputed geometry info (if the
  // geometry is valid) and the word itself. This single rule is used by the
  // `WordWriter` to assign the cell and by `indexFromPosition` to recompute
  // it, so the two always agree.
  static GeoCellGrid::CellIndex cellIndexOfWord(
      const GeoCellGrid& grid, const std::optional<GeometryInfo>& info,
      std::string_view word) {
    // When the `GeometryInfo` is valid, its bounding box is the one that
    // `cellIndexFromWktLiteral` would compute, so it can be reused; otherwise
    // `cellIndexFromWktLiteral` can still assign a regular cell in corner
    // cases where only parts of the `GeometryInfo` computation failed.
    return info.has_value()
               ? grid.cellIndexFromBoundingBox(info->getBoundingBox())
               : grid.cellIndexFromWktLiteral(word);
  }

  // The precomputed `GeometryInfo` of the word at the given position.
  std::optional<GeometryInfo> geoInfoAtPosition(uint64_t position) const;

  // Compute `endIndex_` for the opened vocabulary.
  uint64_t computeEndIndex() const;

  // Replace the position in a non-end `WordAndIndex` by the index.
  WordAndIndex withIndexFromPosition(WordAndIndex wordAndIndex) const {
    if (wordAndIndex.isEnd() || !grid_.has_value()) {
      return wordAndIndex;
    }
    return {wordAndIndex.word(),
            indexFromPosition(wordAndIndex.index(), wordAndIndex.word())};
  }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_GEOVOCABULARY_H
