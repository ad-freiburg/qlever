// Copyright 2015 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de>
//          Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/IndexImpl.h"

#include <absl/strings/str_split.h>

#include <charconv>
#include <ranges>
#include <tuple>
#include <utility>

#include "backports/algorithm.h"
#include "index/FTSAlgorithms.h"
#include "index/SortedIdTableMerge.h"
#include "index/TextIndexReadWrite.h"
#include "parser/WordsAndDocsFileParser.h"
#include "util/MmapVector.h"

// _____________________________________________________________________________
void IndexImpl::addTextFromOnDiskIndex() {
  // Read the text vocabulary (into RAM).
  textVocab_.readFromFile(onDiskBase_ + ".text.vocabulary");

  // Initialize the text index.
  std::string textIndexFileName = onDiskBase_ + ".text.index";
  LOG(INFO) << "Reading metadata from file " << textIndexFileName << " ..."
            << std::endl;
  textIndexFile_.open(textIndexFileName.c_str(), "r");
  AD_CONTRACT_CHECK(textIndexFile_.isOpen());
  off_t metaFrom;
  [[maybe_unused]] off_t metaTo = textIndexFile_.getLastOffset(&metaFrom);
  ad_utility::serialization::FileReadSerializer serializer(
      std::move(textIndexFile_));
  serializer.setSerializationPosition(metaFrom);
  serializer >> textMeta_;
  textIndexFile_ = std::move(serializer).file();
  LOG(INFO) << "Registered text index: " << textMeta_.statistics() << std::endl;
  // Initialize the text records file aka docsDB. NOTE: The search also works
  // without this, but then there is no content to show when a text record
  // matches. This is perfectly fine when the text records come from IRIs or
  // literals from our RDF vocabulary.
  std::string docsDbFileName = onDiskBase_ + ".text.docsDB";
  std::ifstream f(docsDbFileName.c_str());
  if (f.good()) {
    f.close();
    docsDB_.init(string(onDiskBase_ + ".text.docsDB"));
    LOG(INFO) << "Registered text records: #records = " << docsDB_._size
              << std::endl;
  } else {
    LOG(DEBUG) << "No file \"" << docsDbFileName
               << "\" with additional text records" << std::endl;
    f.close();
  }
}

// _____________________________________________________________________________
TextBlockIndex IndexImpl::getWordBlockId(WordIndex wordIndex) const {
  return std::lower_bound(blockBoundaries_.begin(), blockBoundaries_.end(),
                          wordIndex) -
         blockBoundaries_.begin();
}

// _____________________________________________________________________________
void IndexImpl::openTextFileHandle() {
  AD_CONTRACT_CHECK(!onDiskBase_.empty());
  textIndexFile_.open(string(onDiskBase_ + ".text.index").c_str(), "r");
}

// _____________________________________________________________________________
std::string_view IndexImpl::wordIdToString(WordIndex wordIndex) const {
  return textVocab_[WordVocabIndex::make(wordIndex)];
}

// _____________________________________________________________________________
IdTable IndexImpl::getWordPostingsForTerm(
    const string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  LOG(DEBUG) << "Getting word postings for term: " << term << '\n';
  IdTable idTable{allocator};
  auto optionalTbmd = getTextBlockMetadataForWordOrPrefix(term);
  if (!optionalTbmd.has_value()) {
    idTable.setNumColumns(term.ends_with(PREFIX_CHAR) ? 3 : 2);
    return idTable;
  }

  IdTable output = mergeTextBlockResults(
      textIndexReadWrite::readWordCl, optionalTbmd.value(), allocator, false);

  LOG(DEBUG) << "Word postings for term: " << term
             << ": cids: " << output.getColumn(0).size() << '\n';
  return output;
}

// _____________________________________________________________________________
IdTable IndexImpl::getEntityMentionsForWord(
    const string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  auto optTbmd = getTextBlockMetadataForWordOrPrefix(term);
  if (!optTbmd.has_value()) {
    return IdTable{allocator};
  }
  return mergeTextBlockResults(textIndexReadWrite::readWordEntityCl,
                               optTbmd.value(), allocator, true);
}

// _____________________________________________________________________________
template <typename Reader>
IdTable IndexImpl::mergeTextBlockResults(
    Reader reader, std::vector<TextBlockMetadataAndWordInfo> tbmds,
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    bool isEntitySearch) const {
  // Collect all blocks as IdTables
  vector<IdTable> partialResults;
  for (const auto& tbmd : tbmds) {
    IdTable partialResult{allocator};
    partialResult =
        reader(tbmd.tbmd_, allocator, textIndexFile_, textScoringMetric_);
    if (!isEntitySearch && tbmd.hasToBeFiltered_) {
      partialResult =
          FTSAlgorithms::filterByRange(tbmd.idRange_, partialResult);
    }
    partialResults.push_back(std::move(partialResult));
  }
  return SortedIdTableMerge::mergeIdTables(std::move(partialResults), allocator,
                                           memoryLimitIndexBuilding());
}

// _____________________________________________________________________________
size_t IndexImpl::getIndexOfBestSuitedElTerm(
    const vector<string>& terms) const {
  // It is beneficial to choose a term where no filtering by regular word id
  // is needed. Then the entity lists can be read directly from disk.
  // For others it is always necessary to reach wordlist and filter them
  // if such an entity list is taken, another intersection is necessary.

  // Apart from that, entity lists are usually larger by a factor.
  // Hence it makes sense to choose the smallest.

  // Heuristic: Always prefer no-filtering terms over others, then
  // pick the one with the smallest EL block to be read.
  std::vector<std::tuple<size_t, bool, size_t>> toBeSorted;
  for (size_t i = 0; i < terms.size(); ++i) {
    auto optTbmd = getTextBlockMetadataForWordOrPrefix(terms[i]);
    if (!optTbmd.has_value()) {
      return i;
    }
    for (const auto& tbmdAndWordInfo : optTbmd.value()) {
      const auto& tbmd = tbmdAndWordInfo.tbmd_;
      toBeSorted.emplace_back(i, tbmd._firstWordId == tbmd._lastWordId,
                              tbmd._entityCl._nofElements);
    }
  }
  std::sort(toBeSorted.begin(), toBeSorted.end(),
            [](const std::tuple<size_t, bool, size_t>& a,
               const std::tuple<size_t, bool, size_t>& b) {
              if (std::get<1>(a) == std::get<1>(b)) {
                return std::get<2>(a) < std::get<2>(b);
              } else {
                return std::get<1>(a);
              }
            });
  return std::get<0>(toBeSorted[0]);
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeOfTextBlockForEntities(const string& word) const {
  if (word.empty()) {
    return 0;
  }
  auto optTbmd = getTextBlockMetadataForWordOrPrefix(word);
  if (!optTbmd.has_value()) {
    return 0;
  }
  size_t sum = 0;
  for (const auto& tbmdAndWordInfo : optTbmd.value()) {
    sum += tbmdAndWordInfo.tbmd_._entityCl._nofElements;
  }
  return sum;
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeOfTextBlockForWord(const string& word) const {
  if (word.empty()) {
    return 0;
  }
  auto optTbmd = getTextBlockMetadataForWordOrPrefix(word);
  if (!optTbmd.has_value()) {
    return 0;
  }
  size_t sum = 0;
  for (const auto& tbmdAndWordInfo : optTbmd.value()) {
    sum += tbmdAndWordInfo.tbmd_._cl._nofElements;
  }
  return sum;
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeEstimate(const string& words) const {
  // TODO vector can be of type std::string_view if called functions
  //  are updated to accept std::string_view instead of const std::string&
  std::vector<std::string> terms = absl::StrSplit(words, ' ');
  if (terms.empty()) {
    return 0;
  }
  auto termToEstimate = [&](const std::string& term) -> size_t {
    auto optTbmd = getTextBlockMetadataForWordOrPrefix(term);
    // TODO<C++23> Use `std::optional::transform`.
    if (!optTbmd.has_value()) {
      return 0;
    }
    return ql::ranges::min(
        optTbmd.value() | ql::views::transform([](const auto& tbmdAndWordInfo) {
          return 1 + tbmdAndWordInfo.tbmd_._entityCl._nofElements / 100;
        }));
  };
  return ql::ranges::min(terms | ql::views::transform(termToEstimate));
}

// _____________________________________________________________________________
void IndexImpl::setTextName(const string& name) { textMeta_.setName(name); }

// _____________________________________________________________________________
auto IndexImpl::getTextBlockMetadataForWordOrPrefix(const std::string& word)
    const -> std::optional<std::vector<TextBlockMetadataAndWordInfo>> {
  AD_CORRECTNESS_CHECK(!word.empty());
  IdRange<WordVocabIndex> idRange;
  if (word.ends_with(PREFIX_CHAR)) {
    auto idRangeOpt = textVocab_.getIdRangeForFullTextPrefix(word);
    if (!idRangeOpt.has_value()) {
      LOG(INFO) << "Prefix: " << word << " not in vocabulary\n";
      return std::nullopt;
    }
    idRange = idRangeOpt.value();
  } else {
    WordVocabIndex idx;
    if (!textVocab_.getId(word, &idx)) {
      LOG(INFO) << "Term: " << word << " not in vocabulary\n";
      return std::nullopt;
    }
    idRange = IdRange{idx, idx};
  }
  auto tbmdVector = textMeta_.getBlockInfoByWordRange(idRange.first().get(),
                                                      idRange.last().get());

  bool hasToBeFiltered = false;
  std::vector<TextBlockMetadataAndWordInfo> output;
  for (auto tbmd : tbmdVector) {
    hasToBeFiltered = tbmd.get()._cl.hasMultipleWords() &&
                      !(tbmd.get()._firstWordId == idRange.first().get() &&
                        tbmd.get()._lastWordId == idRange.last().get());
    output.emplace_back(
        TextBlockMetadataAndWordInfo{tbmd.get(), hasToBeFiltered, idRange});
  }
  return std::optional{output};
}

// _____________________________________________________________________________
void IndexImpl::storeTextScoringParamsInConfiguration(
    TextScoringMetric scoringMetric, float b, float k) {
  configurationJson_["text-scoring-metric"] = scoringMetric;
  textScoringMetric_ = scoringMetric;
  auto bAndK = [b, k, this]() {
    if (0 <= b && b <= 1 && 0 <= k) {
      return std::pair{b, k};
    } else {
      if (textScoringMetric_ == TextScoringMetric::BM25) {
        throw std::runtime_error{absl::StrCat(
            "Invalid values given for BM25 score: `b=", b, "` and `k=", k,
            "`, `b` must be in [0, 1] and `k` must be >= 0 ")};
      }
      return std::pair{0.75f, 1.75f};
    }
  }();
  bAndKParamForTextScoring_ = bAndK;
  configurationJson_["b-and-k-parameter-for-text-scoring"] = bAndK;
  writeConfiguration();
}
