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
#include "index/TextIndexReadWrite.h"
#include "parser/WordsAndDocsFileParser.h"
#include "util/MmapVector.h"
#include "util/TransparentFunctors.h"

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
    docsDB_.init(std::string(onDiskBase_ + ".text.docsDB"));
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
  textIndexFile_.open(std::string(onDiskBase_ + ".text.index").c_str(), "r");
}

// _____________________________________________________________________________
std::string_view IndexImpl::wordIdToString(WordIndex wordIndex) const {
  return textVocab_[WordVocabIndex::make(wordIndex)];
}

// _____________________________________________________________________________
IdTable IndexImpl::getWordPostingsForTerm(
    const std::string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  LOG(DEBUG) << "Getting word postings for term: " << term << '\n';
  IdTable idTable{allocator};
  auto tbmds = getTextBlockMetadataForWordOrPrefix(term);
  if (tbmds.empty()) {
    idTable.setNumColumns(term.ends_with(PREFIX_CHAR) ? 3 : 2);
    return idTable;
  }

  IdTable result = mergeTextBlockResults(textIndexReadWrite::readWordCl, tbmds,
                                         allocator, TextScanMode::WordScan);

  LOG(DEBUG) << "Word postings for term: " << term
             << ": cids: " << result.getColumn(0).size() << '\n';
  return result;
}

// _____________________________________________________________________________
IdTable IndexImpl::getEntityMentionsForWord(
    const std::string& term,
    const ad_utility::AllocatorWithLimit<Id>& allocator) const {
  auto tbmds = getTextBlockMetadataForWordOrPrefix(term);
  AD_CORRECTNESS_CHECK(!tbmds.empty());
  return mergeTextBlockResults(textIndexReadWrite::readWordEntityCl, tbmds,
                               allocator, TextScanMode::EntityScan);
}

// _____________________________________________________________________________
size_t IndexImpl::getIndexOfBestSuitedElTerm(
    const vector<std::string>& terms) const {
  // Heuristic: Pick the term with the smallest number of entries to be read.
  // Note that
  // 1. The entries can be spread over multiple blocks and
  // 2. The heuristic might be off since it doesn't account for duplicates which
  // are later filtered out.
  std::vector<std::pair<size_t, size_t>> toBeSorted;
  for (size_t i = 0; i < terms.size(); ++i) {
    auto tbmds = getTextBlockMetadataForWordOrPrefix(terms[i]);
    if (tbmds.empty()) {
      return i;
    }
    toBeSorted.emplace_back(
        i, getSizeOfTextBlocksSum(tbmds, TextScanMode::EntityScan));
  }
  ql::ranges::sort(toBeSorted, std::less<>{}, ad_utility::second);
  return std::get<0>(toBeSorted.at(0));
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeOfTextBlocksSum(const std::string& word,
                                         TextScanMode textScanMode) const {
  if (word.empty()) {
    return 0;
  }
  auto tbmds = getTextBlockMetadataForWordOrPrefix(word);
  if (tbmds.empty()) {
    return 0;
  }
  return getSizeOfTextBlocksSum(tbmds, textScanMode);
}

// _____________________________________________________________________________
size_t IndexImpl::getSizeOfTextBlocksSum(
    const vector<TextBlockMetadataAndWordInfo>& tbmds,
    TextScanMode textScanMode) {
  auto addSizeOfBlock =
      [&textScanMode](size_t acc,
                      const TextBlockMetadataAndWordInfo& tbmdAndWordInfo) {
        if (textScanMode == TextScanMode::WordScan) {
          return acc + tbmdAndWordInfo.tbmd_._cl._nofElements;
        }
        return acc + tbmdAndWordInfo.tbmd_._entityCl._nofElements;
      };
  return std::accumulate(tbmds.begin(), tbmds.end(), size_t{0}, addSizeOfBlock);
};

// _____________________________________________________________________________
void IndexImpl::setTextName(const std::string& name) {
  textMeta_.setName(name);
}

// _____________________________________________________________________________
auto IndexImpl::getTextBlockMetadataForWordOrPrefix(const std::string& word)
    const -> std::vector<TextBlockMetadataAndWordInfo> {
  AD_CORRECTNESS_CHECK(!word.empty());
  IdRange<WordVocabIndex> idRange;
  if (word.ends_with(PREFIX_CHAR)) {
    auto idRangeOpt = textVocab_.getIdRangeForFullTextPrefix(word);
    if (!idRangeOpt.has_value()) {
      LOG(INFO) << "Prefix: " << word << " not in vocabulary\n";
      return {};
    }
    idRange = idRangeOpt.value();
  } else {
    WordVocabIndex idx;
    if (!textVocab_.getId(word, &idx)) {
      LOG(INFO) << "Term: " << word << " not in vocabulary\n";
      return {};
    }
    idRange = IdRange{idx, idx};
  }
  auto tbmdVector = textMeta_.getBlockInfoByWordRange(idRange.first().get(),
                                                      idRange.last().get());

  std::vector<TextBlockMetadataAndWordInfo> result;
  for (auto tbmd : tbmdVector) {
    result.emplace_back(tbmd.get(), idRange);
  }
  return result;
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
