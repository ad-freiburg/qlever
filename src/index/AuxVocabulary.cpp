// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/AuxVocabulary.h"

#include <absl/strings/str_cat.h>

#include "backports/algorithm.h"
#include "global/FileSuffixConstants.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "util/Exception.h"
#include "util/FilesystemHelpers.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeVector.h"

// The name of the file that stores the positions in the main vocabulary and the
// offsets of the sub-vocabularies.
static std::string positionsFilename(const std::string& basename) {
  return absl::StrCat(basename, AUX_VOCAB_POSITIONS_SUFFIX);
}

// ____________________________________________________________________________
std::vector<std::string> AuxVocabulary::fileNames(const std::string& basename) {
  std::vector<std::string> result;
  for (const auto& file :
       qlever::util::filesWithBaseNameAndSuffix(basename, VOCAB_SUFFIX)) {
    result.push_back(file.string());
  }
  return result;
}

// ____________________________________________________________________________
void AuxVocabulary::open(const std::string& basename,
                         ad_utility::VocabularyType type,
                         const std::string& language,
                         const std::string& country, bool ignorePunctuation) {
  vocabulary_.resetToType(type);
  vocabulary_.setLocale(language, country, ignorePunctuation);
  vocabulary_.readFromFile(absl::StrCat(basename, VOCAB_SUFFIX));
  marker_ =
      VocabIndexMarker{PolymorphicVocabulary::numberOfSubVocabularies(type)};

  ad_utility::serialization::FileReadSerializer reader{
      positionsFilename(basename)};
  reader >> positionsInMainVocab_;
  reader >> markerOffsets_;
  AD_CORRECTNESS_CHECK(
      positionsInMainVocab_.size() == vocabulary_.size(),
      "The number of positions in the main vocabulary does not match the "
      "number of words of the auxiliary vocabulary");
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(positionsInMainVocab_),
                       "The positions in the main vocabulary must be sorted");
}

// ____________________________________________________________________________
std::pair<AuxVocabIndex, AuxVocabIndex> AuxVocabulary::getPositionOfWord(
    std::string_view word) const {
  return vocabulary_.getPositionOfWord(word);
}

// ____________________________________________________________________________
std::optional<AuxVocabIndex> AuxVocabulary::getId(std::string_view word) const {
  auto [lower, upper] = getPositionOfWord(word);
  if (lower == upper) {
    return std::nullopt;
  }
  AD_CORRECTNESS_CHECK(upper.get() - lower.get() == 1);
  return lower;
}

// ____________________________________________________________________________
uint64_t AuxVocabulary::numWordsSmallerThan(std::string_view word) const {
  // `getPositionOfWord` returns the position within the sub-vocabulary that
  // `word` belongs to (marker-encoded), so it has to be turned into an offset,
  // which is dense and ordered across all sub-vocabularies. Note that the
  // returned position may be the end of that sub-vocabulary, for which there is
  // no word and hence no `offsetOf`; in that case the offset is the start of
  // the next sub-vocabulary.
  auto lower = getPositionOfWord(word).first;
  auto marker = marker_.getMarker(lower.get());
  uint64_t indexInSubVocab = marker_.getIndexWithoutMarker(lower.get());
  return markerOffsets_.at(marker) + indexInSubVocab;
}

// ____________________________________________________________________________
AuxVocabulary::WordWriter::WordWriter(const RdfsVocabulary& mainVocab,
                                      const std::string& basename,
                                      ad_utility::VocabularyType type)
    : mainVocab_{mainVocab},
      basename_{basename},
      marker_{VocabIndexMarker{
          PolymorphicVocabulary::numberOfSubVocabularies(type)}} {
  // The `PolymorphicVocabulary` can create a writer for an arbitrary type
  // without holding a vocabulary of that type, which is what we need here.
  wordWriter_ = PolymorphicVocabulary::makeDiskWriterPtr(
      absl::StrCat(basename, VOCAB_SUFFIX), type);
}

// ____________________________________________________________________________
AuxVocabulary::WordWriter::~WordWriter() {
  if (!finishWasCalled_ && wordWriter_ != nullptr) {
    // The `WordWriterBase` destructor throws if `finish` was not called, which
    // would be a bug in our code, but throwing from a destructor during stack
    // unwinding must be avoided. `finish` of the underlying writer is safe to
    // call here, we only skip writing the positions file (which then simply
    // does not exist, so the incomplete auxiliary index cannot be opened).
    wordWriter_->finish();
  }
}

// ____________________________________________________________________________
AuxVocabIndex AuxVocabulary::WordWriter::operator()(std::string_view word) {
  AD_CONTRACT_CHECK(!finishWasCalled_);
  const auto& comparator = mainVocab_.getCaseComparator();

  // The words have to arrive grouped by their sub-vocabulary, in ascending
  // order of the markers, and ascending within each group.
  auto [lower, upper] = mainVocab_.getPositionOfWord(word);
  AD_CONTRACT_CHECK(lower == upper,
                    "A word that is contained in the vocabulary of the main "
                    "index must not be added to an auxiliary vocabulary");
  auto position = Id::makeFromVocabIndex(lower);
  uint8_t marker = marker_.getMarker(position);
  AD_CONTRACT_CHECK(
      marker >= currentMarker_,
      "The words of an auxiliary vocabulary have to be pushed grouped by their "
      "sub-vocabulary, in ascending order of the markers of those "
      "sub-vocabularies");
  if (marker != currentMarker_) {
    // A new sub-vocabulary starts at the current offset. Note that the
    // sub-vocabularies in between (if any) are empty, so they get the same
    // offset.
    for (uint8_t m = currentMarker_ + 1; m <= marker; ++m) {
      markerOffsets_.at(m) = positionsInMainVocab_.size();
    }
    currentMarker_ = marker;
    previousWord_.reset();
  } else if (previousWord_.has_value()) {
    AD_CONTRACT_CHECK(
        comparator(previousWord_.value(), word,
                   RdfsVocabulary::SortLevel::TOTAL),
        "The words of an auxiliary vocabulary have to be pushed in strictly "
        "ascending order within each sub-vocabulary");
  }
  previousWord_ = std::string{word};
  positionsInMainVocab_.push_back(position.getBits());

  uint64_t index = (*wordWriter_)(word, mainVocab_.shouldBeExternalized(word));
  // The index that the underlying (possibly split) vocabulary assigns has to
  // agree with the offset that we computed from the position in the main
  // vocabulary, else the two vocabularies split their words differently.
  AD_CORRECTNESS_CHECK(
      marker_.getMarker(index) == marker,
      "The auxiliary vocabulary and the vocabulary of the main index assign a "
      "word to different sub-vocabularies. They have to use the same "
      "vocabulary type");
  AD_CORRECTNESS_CHECK(
      markerOffsets_.at(marker) + marker_.getIndexWithoutMarker(index) + 1 ==
          positionsInMainVocab_.size(),
      "The indices assigned by the word writer of an auxiliary vocabulary must "
      "be consecutive within each sub-vocabulary");
  return AuxVocabIndex::make(index);
}

// ____________________________________________________________________________
void AuxVocabulary::WordWriter::finish() {
  AD_CONTRACT_CHECK(!finishWasCalled_);
  finishWasCalled_ = true;
  // All sub-vocabularies after the last one that received a word are empty and
  // start at the end.
  for (uint8_t m = currentMarker_ + 1; m < marker_.numMarkers(); ++m) {
    markerOffsets_.at(m) = positionsInMainVocab_.size();
  }
  wordWriter_->finish();
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(positionsInMainVocab_),
                       "The positions in the main vocabulary must be sorted");
  ad_utility::serialization::FileWriteSerializer writer{
      positionsFilename(basename_)};
  writer << positionsInMainVocab_;
  writer << markerOffsets_;
}
