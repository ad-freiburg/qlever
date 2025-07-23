// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "index/TextMetaData.h"

#include "global/Constants.h"
#include "util/ReadableNumberFacet.h"

// _____________________________________________________________________________
std::vector<std::reference_wrapper<const TextBlockMetaData>>
TextMetaData::getBlockInfoByWordRange(const uint64_t lower,
                                      const uint64_t upper) const {
  AD_CONTRACT_CHECK(upper >= lower);
  assert(_blocks.size() > 0);
  assert(_blocks.size() == _blockUpperBoundWordIds.size());

  // Binary search in the sorted _blockUpperBoundWordIds vector.
  auto it = std::lower_bound(_blockUpperBoundWordIds.begin(),
                             _blockUpperBoundWordIds.end(), lower);
  // If the word would be behind all that, return the last block
  if (it == _blockUpperBoundWordIds.end()) {
    --it;
  }

  // Binary search in the sorted _blockUpperBoundWordIds vector.
  auto upperIt = std::lower_bound(_blockUpperBoundWordIds.begin(),
                                  _blockUpperBoundWordIds.end(), upper);
  // Same as for normal it. This has to be done since the range is [lower,
  // upper] as opposed to `[lower, upper)`.
  // TODO<joka921, flixtastic> fix this inconsistency with the usual C++
  // conventions.
  if (upperIt == _blockUpperBoundWordIds.end()) {
    --upperIt;
  }

  // Convert iterators to indices
  auto startIndex =
      static_cast<size_t>(std::distance(_blockUpperBoundWordIds.begin(), it));
  auto endIndex = static_cast<size_t>(
      std::distance(_blockUpperBoundWordIds.begin(), upperIt));

  // Collect all blocks
  std::vector<std::reference_wrapper<const TextBlockMetaData>> output;
  ql::ranges::copy(ql::ranges::subrange(_blocks.begin() + startIndex,
                                        _blocks.begin() + endIndex + 1),
                   std::back_inserter(output));
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
  _blockUpperBoundWordIds.push_back(md._lastWordId);
}

// _____________________________________________________________________________
off_t TextMetaData::getOffsetAfter() {
  return _blocks.back()._entityCl._lastByte + 1;
}
