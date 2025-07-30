// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "index/TextMetaData.h"

#include "global/Constants.h"
#include "util/ReadableNumberFacet.h"

// _____________________________________________________________________________
std::vector<std::reference_wrapper<const TextBlockMetaData>>
TextMetaData::getBlockInfoByWordRange(const WordVocabIndex lower,
                                      const WordVocabIndex upper) const {
  AD_CONTRACT_CHECK(upper >= lower);
  AD_CORRECTNESS_CHECK(!_blocks.empty());

  auto projection = &TextBlockMetaData::_lastWordId;

  // Binary search in the sorted _blocks vector using the lastWordIds of the
  // blocks. This points to the first block having a lastWordId >= lower.
  auto it = ql::ranges::lower_bound(_blocks, lower, std::less<>{}, projection);
  // If the word would be behind all that, return the last block
  if (it == _blocks.end()) {
    return {_blocks.back()};
  }

  // Binary search in the sorted _blocks vector using the lastWordIds of the
  // blocks. This points to the first block having a lastWordId > upper. We want
  // this block since it potentially contains elements of the range.
  // Since the range is [lower, upper] as opposed to `[lower, upper)`.
  // TODO<joka921, flixtastic> fix this inconsistency with the usual C++
  // conventions.
  auto upperIt =
      ql::ranges::upper_bound(_blocks, upper, std::less<>{}, projection);
  if (upperIt == _blocks.end()) {
    --upperIt;
  }

  // Convert iterators to vector indices
  auto startIndex = std::distance(_blocks.begin(), it);
  auto endIndex = std::distance(_blocks.begin(), upperIt);

  // Collect all blocks
  std::vector<std::reference_wrapper<const TextBlockMetaData>> output;
  ql::ranges::copy(ql::ranges::subrange(_blocks.begin() + startIndex,
                                        _blocks.begin() + endIndex + 1),
                   std::back_inserter(output));

  // Look if the last block actually contains WordVocabIndices in range and if
  // not remove the last block
  if (!(output.back().get()._firstWordId <= upper)) {
    output.pop_back();
  }
  return output;
}

// _____________________________________________________________________________
size_t TextMetaData::getBlockCount() const { return _blocks.size(); }

// _____________________________________________________________________________
std::string TextMetaData::statistics() const {
  // TODO: What does totalElementsEntityLists count?
  size_t totalElementsClassicLists = 0;
  // size_t totalElementsEntityLists = 0;
  for (size_t i = 0; i < _blocks.size(); ++i) {
    const ContextListMetaData& wcl = _blocks[i]._cl;
    totalElementsClassicLists += wcl._nofElements;
    // const ContextListMetaData& ecl = _blocks[i]._entityCl;
    // totalElementsEntityLists += ecl._nofElements;
  }
  // Show abbreviated statistics (like for the permutations, which used to have
  // very verbose statistics, too).
  std::ostringstream os;
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  os.imbue(locWithNumberGrouping);
  os << "#words = " << totalElementsClassicLists
     << ", #blocks = " << _blocks.size();
  return std::move(os).str();
}

// _____________________________________________________________________________
void TextMetaData::addBlock(const TextBlockMetaData& md) {
  _blocks.push_back(md);
}

// _____________________________________________________________________________
off_t TextMetaData::getOffsetAfter() const {
  return _blocks.back()._entityCl._lastByte + 1;
}
