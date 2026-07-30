// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/AuxIndexBuilder.h"

#include <absl/strings/str_cat.h>

#include <fstream>

#include "backports/algorithm.h"
#include "global/Constants.h"
#include "global/FileSuffixConstants.h"
#include "index/DeltaTriples.h"
#include "index/ExternalSortFunctors.h"
#include "index/IndexImpl.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/Log.h"
#include "util/Serializer/FileSerializer.h"

namespace qlever {

// ____________________________________________________________________________
std::optional<Id> AuxIndexIdMapping::map(Id id) const {
  auto datatype = id.getDatatype();
  if (datatype == Datatype::AuxVocabIndex) {
    AD_CORRECTNESS_CHECK(previousAuxVocab_ != nullptr);
    auto offset = previousAuxVocab_->offsetOf(id.getAuxVocabIndex());
    AD_CORRECTNESS_CHECK(offset < newIdForOldAuxOffset_.size());
    return newIdForOldAuxOffset_[offset];
  }
  if (datatype == Datatype::LocalVocabIndex) {
    auto it = newIdForLocalVocabEntry_.find(id.getLocalVocabIndex());
    if (it == newIdForLocalVocabEntry_.end()) {
      return std::nullopt;
    }
    return it->second;
  }
  return id;
}

namespace {

// The number of columns of the permutations of an auxiliary index. The columns
// are the three index columns, the graph column, and the column that stores
// whether a triple is inserted or deleted.
constexpr size_t numColumns = AuxIndex::numColumns;
static_assert(numColumns == NumColumnsIndexBuilding + 1);

// A triple of an update, together with the information whether it is inserted
// or deleted. The `Id`s are in the order of the permutation that the triples
// are merged in (see `mergeUpdateTriples`), so *not* necessarily `(S, P, O,
// G)`.
struct UpdateTriple {
  IdTriple<0> triple_;
  bool insertOrDelete_;
};

// The permutation whose order the triples are merged in. There is no `SPO`
// permutation for the internal triples, so the internal ones are merged in
// `PSO` order.
Permutation::Enum mergeOrder(bool isInternal) {
  return isInternal ? Permutation::PSO : Permutation::SPO;
}

// The inverse of the `KeyOrder` of the given permutation, that is, the
// permutation that maps a quad from the order of `permutation` back to
// `(S, P, O, G)`.
KeyOrder inverseKeyOrder(Permutation::Enum permutation) {
  auto keys = Permutation::toKeyOrder(permutation).keys();
  KeyOrder::Array inverse{};
  for (size_t i = 0; i < keys.size(); ++i) {
    inverse.at(keys.at(i)) = static_cast<KeyOrder::T>(i);
  }
  return KeyOrder{inverse.at(0), inverse.at(1), inverse.at(2), inverse.at(3)};
}

// All delta triples of the given `locatedTriplesState`, in the order of the
// permutation that they are merged in, see `mergeOrder`.
template <bool isInternal>
std::vector<UpdateTriple> collectDeltaTriples(
    const LocatedTriplesState& locatedTriplesState) {
  const auto& locatedTriples =
      locatedTriplesState.getLocatedTriplesForPermutation<isInternal>(
          mergeOrder(isInternal));
  // Computing the difference to an empty set of located triples yields all of
  // them, split into insertions and deletions, each sorted.
  LocatedTriplesPerBlock empty;
  auto [inserted, deleted] = locatedTriples.computeDiff(empty);
  std::vector<UpdateTriple> result;
  result.reserve(inserted.size() + deleted.size());
  // Merge the insertions and the deletions into a single sorted sequence. A
  // triple is never both inserted and deleted, so there are no ties.
  auto addInserted = [&result](const IdTriple<0>& triple) {
    result.push_back({triple, true});
  };
  auto addDeleted = [&result](const IdTriple<0>& triple) {
    result.push_back({triple, false});
  };
  auto itInserted = inserted.begin();
  auto itDeleted = deleted.begin();
  while (itInserted != inserted.end() && itDeleted != deleted.end()) {
    if (*itInserted < *itDeleted) {
      addInserted(*itInserted++);
    } else {
      AD_CORRECTNESS_CHECK(*itDeleted < *itInserted,
                           "A triple must not be inserted and deleted at the "
                           "same time");
      addDeleted(*itDeleted++);
    }
  }
  ql::ranges::for_each(ql::ranges::subrange{itInserted, inserted.end()},
                       addInserted);
  ql::ranges::for_each(ql::ranges::subrange{itDeleted, deleted.end()},
                       addDeleted);
  return result;
}

// Compare the triples of two `UpdateTriple`s (ignoring their insert/delete
// flags). Both the delta triples and the triples of an auxiliary index are
// sorted this way in the order that they are merged in.
bool tripleLess(const IdTriple<0>& a, const IdTriple<0>& b) { return a < b; }

// Call `callback(const UpdateTriple&)` for the merged sequence of the delta
// triples in `deltaTriples` (which have to be sorted) and the triples of the
// corresponding permutation of `previousAux` (which may be `nullptr`), in
// ascending order. If both of them contain the same triple, the one from the
// delta triples wins, because it is the more recent one.
template <typename Callback>
void mergeUpdateTriples(
    const std::vector<UpdateTriple>& deltaTriples, const AuxIndex* previousAux,
    bool isInternal,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    Callback callback) {
  auto itDelta = deltaTriples.begin();
  auto emitDelta = [&callback](const UpdateTriple& triple) {
    callback(triple);
  };
  if (previousAux == nullptr || previousAux->isEmpty()) {
    ql::ranges::for_each(deltaTriples, emitDelta);
    return;
  }

  auto [reader, blocks] = previousAux->scanFull(mergeOrder(isInternal),
                                                isInternal, cancellationHandle);

  for (const IdTable& block : blocks) {
    cancellationHandle->throwIfCancelled();
    for (size_t row = 0; row < block.numRows(); ++row) {
      UpdateTriple fromAux{
          IdTriple<0>{std::array<Id, 4>{block(row, 0), block(row, 1),
                                        block(row, 2), block(row, 3)}},
          block(row, AuxIndex::insertOrDeleteColumn) == AuxIndex::insertedId()};
      // Emit all delta triples that are smaller, and skip the triple from the
      // auxiliary index if the delta triples also have an entry for it.
      while (itDelta != deltaTriples.end() &&
             tripleLess(itDelta->triple_, fromAux.triple_)) {
        emitDelta(*itDelta++);
      }
      if (itDelta != deltaTriples.end() &&
          !tripleLess(fromAux.triple_, itDelta->triple_)) {
        emitDelta(*itDelta++);
        continue;
      }
      callback(fromAux);
    }
  }
  ql::ranges::for_each(ql::ranges::subrange{itDelta, deltaTriples.end()},
                       emitDelta);
}

// The classification of an `Id` that is used by the delta triples or by the
// previous generation of the auxiliary index.
struct WordSource {
  std::string word_;
  uint8_t marker_;
  // Exactly one of the two is set: the local vocab entry that the word comes
  // from, or the `Id` that it has in the previous generation of the auxiliary
  // vocabulary.
  LocalVocabIndex localVocabEntry_ = nullptr;
  Id previousAuxId_ = Id::makeUndefined();
};

// The words that need an `Id` in the new generation of the auxiliary
// vocabulary, plus the local vocab entries whose word is already representable
// in the main index (in its vocabulary or as an encoded value), which are
// resolved directly.
struct CollectedWords {
  std::vector<WordSource> sources_;
  ad_utility::HashMap<LocalVocabIndex, Id> resolvedLocalVocabEntries_;
};

// Collect the words of all `Id`s of the merged update triples that are not part
// of the vocabulary of the main index, see `CollectedWords`.
class WordCollector {
 private:
  const IndexImpl& index_;
  const AuxIndex* previousAux_;
  ad_utility::HashSet<LocalVocabIndex> seenLocalVocabEntries_;
  ad_utility::HashSet<uint64_t> seenPreviousAuxOffsets_;
  CollectedWords collected_;

 public:
  WordCollector(const IndexImpl& index, const AuxIndex* previousAux)
      : index_{index}, previousAux_{previousAux} {}

  // Process all `Id`s of `triple`.
  void operator()(const UpdateTriple& triple) {
    for (Id id : triple.triple_.ids()) {
      addId(id);
    }
  }

  CollectedWords getResult() && { return std::move(collected_); }

 private:
  // Add the word of a single `Id`, if it needs an `Id` in the new generation.
  void addId(Id id) {
    auto datatype = id.getDatatype();
    if (datatype == Datatype::AuxVocabIndex) {
      addPreviousAuxId(id);
    } else if (datatype == Datatype::LocalVocabIndex) {
      addLocalVocabEntry(id.getLocalVocabIndex());
    }
  }

  // ___________________________________________________________________________
  void addPreviousAuxId(Id id) {
    AD_CORRECTNESS_CHECK(previousAux_ != nullptr);
    const auto& vocab = previousAux_->vocab();
    auto auxIndex = id.getAuxVocabIndex();
    if (!seenPreviousAuxOffsets_.insert(vocab.offsetOf(auxIndex)).second) {
      return;
    }
    std::string word{vocab[auxIndex]};
    collected_.sources_.push_back(
        {word, index_.getVocab().getMarkerForWord(word), nullptr, id});
  }

  // ___________________________________________________________________________
  void addLocalVocabEntry(LocalVocabIndex entry) {
    if (!seenLocalVocabEntries_.insert(entry).second) {
      return;
    }
    auto [lowerBound, upperBound] = entry->positionInVocab();
    if (lowerBound != upperBound) {
      // The word is already representable in the main index: it is either
      // contained in its vocabulary, or it is an encoded value. Note that it
      // may also be contained in the previous generation of the auxiliary
      // vocabulary, in which case it is resolved to that `Id`, which is then
      // itself remapped when the mapping is assembled.
      auto id = Id::fromBits(lowerBound.get());
      collected_.resolvedLocalVocabEntries_.emplace(entry, id);
      if (id.getDatatype() == Datatype::AuxVocabIndex) {
        addPreviousAuxId(id);
      }
      return;
    }
    std::string word = entry->toStringRepresentation();
    collected_.sources_.push_back({word,
                                   index_.getVocab().getMarkerForWord(word),
                                   entry, Id::makeUndefined()});
  }
};

// Write the vocabulary of the new generation from the collected `words` and
// return the mapping into the `Id` space of that new generation.
AuxIndexIdMapping writeVocabularyAndMakeMapping(
    const IndexImpl& index, const AuxIndex* previousAux,
    const std::string& basename, ad_utility::VocabularyType vocabularyType,
    CollectedWords words, size_t* numVocabWords) {
  const auto& mainVocab = index.getVocab();
  // The words have to be pushed grouped by their sub-vocabulary, and ascending
  // within each group, see `AuxVocabulary::WordWriter`.
  auto less = [&mainVocab](const WordSource& a, const WordSource& b) {
    if (a.marker_ != b.marker_) {
      return a.marker_ < b.marker_;
    }
    return mainVocab.getCaseComparator()(a.word_, b.word_,
                                         RdfsVocabulary::SortLevel::TOTAL);
  };
  ql::ranges::sort(words.sources_, less);

  std::vector<Id> newIdForOldAuxOffset(
      previousAux == nullptr ? 0 : previousAux->vocab().numWords(),
      Id::makeUndefined());
  ad_utility::HashMap<LocalVocabIndex, Id> newIdForLocalVocabEntry;

  {
    AuxVocabulary::WordWriter writer{mainVocab, basename, vocabularyType};
    std::optional<std::string> previousWord;
    Id currentId = Id::makeUndefined();
    for (const auto& source : words.sources_) {
      // Several sources can yield the same word, in which case they all get the
      // same `Id`. Note that the words are sorted, so equal words are adjacent.
      if (!previousWord.has_value() || previousWord.value() != source.word_) {
        currentId = Id::makeFromAuxVocabIndex(writer(source.word_));
        previousWord = source.word_;
      }
      if (source.localVocabEntry_ != nullptr) {
        newIdForLocalVocabEntry.insert_or_assign(source.localVocabEntry_,
                                                 currentId);
      } else {
        AD_CORRECTNESS_CHECK(previousAux != nullptr);
        auto offset = previousAux->vocab().offsetOf(
            source.previousAuxId_.getAuxVocabIndex());
        newIdForOldAuxOffset.at(offset) = currentId;
      }
    }
    *numVocabWords = writer.numWords();
    writer.finish();
  }

  // Add the local vocab entries whose word is already representable in the main
  // index. Note that this has to happen *after* the loop above, because those
  // entries whose word is stored in the previous generation of the auxiliary
  // vocabulary need the mapping of that generation, which the loop above is
  // what fills in. They are kept in a separate container until here, because
  // otherwise they could not be told apart from the entries that the loop above
  // has already mapped to an `Id` of the *new* generation.
  for (const auto& [entry, resolvedId] : words.resolvedLocalVocabEntries_) {
    Id id = resolvedId;
    if (id.getDatatype() == Datatype::AuxVocabIndex) {
      AD_CORRECTNESS_CHECK(previousAux != nullptr);
      auto offset = previousAux->vocab().offsetOf(id.getAuxVocabIndex());
      id = newIdForOldAuxOffset.at(offset);
      AD_CORRECTNESS_CHECK(!id.isUndefined());
    }
    auto [it, inserted] = newIdForLocalVocabEntry.emplace(entry, id);
    (void)it;
    AD_CORRECTNESS_CHECK(inserted,
                         "A local vocab entry must be resolved in exactly one "
                         "way");
  }

  return AuxIndexIdMapping{
      std::move(newIdForOldAuxOffset), std::move(newIdForLocalVocabEntry),
      previousAux == nullptr ? nullptr : &previousAux->vocab()};
}

// The filename of the given permutation of the auxiliary index with the given
// base name.
std::string permutationFilename(const std::string& basename,
                                Permutation::Enum permutation,
                                bool isInternal) {
  std::string base = isInternal
                         ? absl::StrCat(basename, QLEVER_INTERNAL_INDEX_INFIX)
                         : basename;
  return Permutation::fileNames(permutation, base).at(0).string();
}

// Write the pair of permutations `permutation1` and `permutation2` (which have
// to be twins, for example `PSO` and `POS`) of the auxiliary index with the
// given base name, from the `sortedTriples` (which have to be sorted by the key
// order of `permutation1`).
void writePermutationPair(
    const std::string& basename, Permutation::Enum permutation1,
    Permutation::Enum permutation2, bool isInternal,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
    ad_utility::MemorySize blocksizePerColumn) {
  auto makeWriterAndCallback = [&blocksizePerColumn](
                                   IndexMetaData& metaData,
                                   const std::string& filename) {
    auto writer = std::make_unique<CompressedRelationWriter>(
        numColumns, ad_utility::File(filename, "w"), blocksizePerColumn);
    CompressedRelationWriter::MetadataCallback callback =
        [&metaData](ql::span<const CompressedRelationMetadata> mds) {
          for (const auto& md : mds) {
            metaData.add(md);
          }
        };
    return CompressedRelationWriter::WriterAndCallback{std::move(writer),
                                                       std::move(callback)};
  };

  std::string filename1 =
      permutationFilename(basename, permutation1, isInternal);
  std::string filename2 =
      permutationFilename(basename, permutation2, isInternal);
  IndexMetaData metaData1;
  IndexMetaData metaData2;
  auto [numDistinctCol0, blockData1, blockData2] =
      CompressedRelationWriter::createPermutationPair(
          filename1, makeWriterAndCallback(metaData1, filename1),
          makeWriterAndCallback(metaData2, filename2), std::move(sortedTriples),
          Permutation::toKeyOrder(permutation1), {});
  metaData1.blockData() = std::move(blockData1);
  metaData2.blockData() = std::move(blockData2);

  auto finalize = [numDistinctCol0 = numDistinctCol0](
                      IndexMetaData& metaData, const std::string& filename) {
    metaData.calculateStatistics(numDistinctCol0);
    metaData.setName(filename);
    ad_utility::File file{filename, "r+"};
    ad_utility::File metaFile{absl::StrCat(filename, META_FILE_SUFFIX), "w"};
    metaData.appendToFile(file, metaFile);
  };
  finalize(metaData1, filename1);
  finalize(metaData2, filename2);
}

}  // namespace

// ____________________________________________________________________________
AuxIndexBuildResult buildAuxIndex(
    const IndexImpl& index, const LocatedTriplesState& locatedTriplesState,
    ad_utility::MemorySize memoryLimit,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  const AuxIndex* previousAux = index.auxIndex();
  size_t generation =
      previousAux == nullptr ? 0 : previousAux->generation() + 1;
  std::string basename =
      AuxIndex::makeBasename(index.getOnDiskBase(), generation);
  AD_LOG_INFO << "Building the auxiliary index (generation " << generation
              << ") at " << basename << " ..." << std::endl;

  auto deltaTriples = collectDeltaTriples<false>(locatedTriplesState);
  auto internalDeltaTriples = collectDeltaTriples<true>(locatedTriplesState);
  AD_LOG_INFO << "The auxiliary index is built from " << deltaTriples.size()
              << " delta triples (plus " << internalDeltaTriples.size()
              << " internal ones)"
              << (previousAux == nullptr
                      ? ""
                      : " and the previous generation of the auxiliary index")
              << std::endl;

  // Pass one: determine the words that need an `Id` in the new generation.
  auto vocabularyType = index.getVocabularyType();
  AuxIndexIdMapping idMapping;
  size_t numVocabWords = 0;
  {
    WordCollector collector{index, previousAux};
    mergeUpdateTriples(deltaTriples, previousAux, false, cancellationHandle,
                       std::ref(collector));
    mergeUpdateTriples(internalDeltaTriples, previousAux, true,
                       cancellationHandle, std::ref(collector));
    idMapping = writeVocabularyAndMakeMapping(
        index, previousAux, basename, vocabularyType,
        std::move(collector).getResult(), &numVocabWords);
  }
  AD_LOG_INFO << "The vocabulary of the auxiliary index has " << numVocabWords
              << " words" << std::endl;

  // Pass two: remap the `Id`s and write the permutations. The triples are
  // pushed into the external sorters in blocks of bounded size, so that only
  // one such block is held in RAM at a time.
  size_t numInserted = 0;
  size_t numDeleted = 0;
  auto memoryPerSorter = memoryLimit / 4;
  static constexpr size_t blocksize = 100'000;

  // Feed all triples of `triples` merged with the corresponding permutation of
  // the previous generation (see `mergeUpdateTriples`) into all the `sorters`.
  auto pushTriples = [&](const std::vector<UpdateTriple>& triples,
                         bool isInternal, auto&... sorters) {
    auto inverse = inverseKeyOrder(mergeOrder(isInternal));
    IdTableStatic<0> block{numColumns,
                           ad_utility::makeUnlimitedAllocator<Id>()};
    block.reserve(blocksize);
    auto flush = [&block, &sorters...]() {
      if (block.empty()) {
        return;
      }
      // Each sorter takes ownership of the block it is given, so all but the
      // last one get a copy.
      (..., sorters.pushBlock(block.clone()));
      block.clear();
    };
    auto pushRow = [&](const UpdateTriple& triple) {
      std::array<Id, 4> quad = triple.triple_.ids();
      for (Id& id : quad) {
        auto mapped = idMapping.map(id);
        AD_CORRECTNESS_CHECK(
            mapped.has_value(),
            "A local vocab entry was used by a delta triple that is not part "
            "of "
            "the snapshot that the auxiliary index is built from");
        id = mapped.value();
      }
      quad = inverse.permuteTuple(quad);
      block.emplace_back();
      auto row = block.numRows() - 1;
      for (size_t i = 0; i < quad.size(); ++i) {
        block(row, i) = quad.at(i);
      }
      block(row, AuxIndex::insertOrDeleteColumn) = triple.insertOrDelete_
                                                       ? AuxIndex::insertedId()
                                                       : AuxIndex::deletedId();
      if (!isInternal) {
        ++(triple.insertOrDelete_ ? numInserted : numDeleted);
      }
      if (block.numRows() >= blocksize) {
        flush();
      }
    };
    mergeUpdateTriples(triples, previousAux, isInternal, cancellationHandle,
                       pushRow);
    flush();
  };

  auto blocksizePerColumn = index.blocksizePermutationPerColumn();
  {
    ad_utility::CompressedExternalIdTableSorter<SortBySPO, numColumns>
        spoSorter{absl::StrCat(basename, ".spo-sorter.dat"), memoryPerSorter,
                  index.allocator()};
    ad_utility::CompressedExternalIdTableSorter<SortByOSP, numColumns>
        ospSorter{absl::StrCat(basename, ".osp-sorter.dat"), memoryPerSorter,
                  index.allocator()};
    ad_utility::CompressedExternalIdTableSorter<SortByPSO, numColumns>
        psoSorter{absl::StrCat(basename, ".pso-sorter.dat"), memoryPerSorter,
                  index.allocator()};
    pushTriples(deltaTriples, false, spoSorter, ospSorter, psoSorter);
    writePermutationPair(basename, Permutation::SPO, Permutation::SOP, false,
                         spoSorter.getSortedBlocks<0>(), blocksizePerColumn);
    writePermutationPair(basename, Permutation::OSP, Permutation::OPS, false,
                         ospSorter.getSortedBlocks<0>(), blocksizePerColumn);
    writePermutationPair(basename, Permutation::PSO, Permutation::POS, false,
                         psoSorter.getSortedBlocks<0>(), blocksizePerColumn);
  }
  {
    ad_utility::CompressedExternalIdTableSorter<SortByPSO, numColumns>
        internalPsoSorter{absl::StrCat(basename, ".internal-pso-sorter.dat"),
                          memoryPerSorter, index.allocator()};
    pushTriples(internalDeltaTriples, true, internalPsoSorter);
    writePermutationPair(basename, Permutation::PSO, Permutation::POS, true,
                         internalPsoSorter.getSortedBlocks<0>(),
                         blocksizePerColumn);
  }

  // Write the metadata last, such that an interrupted build leaves behind an
  // auxiliary index that is never used, see `AuxIndex::generationsOnDisk`.
  AuxIndexMetadata metadata{generation, numInserted, numDeleted, numVocabWords,
                            vocabularyType};
  {
    std::ofstream metadataFile{
        absl::StrCat(basename, AUX_INDEX_CONFIGURATION_FILE)};
    AD_CORRECTNESS_CHECK(metadataFile.is_open());
    nlohmann::json metadataJson = metadata;
    metadataFile << metadataJson.dump(2);
  }
  AD_LOG_INFO << "Done building the auxiliary index (generation " << generation
              << "): " << numInserted << " inserted and " << numDeleted
              << " deleted triples" << std::endl;
  return {std::move(basename), metadata, std::move(idMapping)};
}

}  // namespace qlever
