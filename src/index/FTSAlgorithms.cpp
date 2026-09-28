// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "index/FTSAlgorithms.h"

#include <utility>

#include "util/HashSet.h"
// _____________________________________________________________________________
IdTable FTSAlgorithms::filterByRange(const IdRange<WordVocabIndex>& idRange,
                                     const IdTable& idTablePreFilter) {
  AD_CONTRACT_CHECK(idTablePreFilter.numColumns() == 3);
  AD_LOG_DEBUG << "Filtering " << idTablePreFilter.getColumn(0).size()
               << " elements by ID range...\n";

  IdTable idTableResult{idTablePreFilter.getAllocator()};
  idTableResult.setNumColumns(3);
  idTableResult.resize(idTablePreFilter.getColumn(0).size());

  decltype(auto) resultCidColumn = idTableResult.getColumn(0);
  decltype(auto) resultWidColumn = idTableResult.getColumn(1);
  decltype(auto) resultSidColumn = idTableResult.getColumn(2);
  size_t nofResultElements = 0;
  decltype(auto) preFilterCidColumn = idTablePreFilter.getColumn(0);
  decltype(auto) preFilterWidColumn = idTablePreFilter.getColumn(1);
  decltype(auto) preFilterSidColumn = idTablePreFilter.getColumn(2);
  // TODO<C++23> Use views::zip.
  for (size_t i = 0; i < preFilterWidColumn.size(); ++i) {
    // TODO<joka921> proper Ids for the text stuff.
    // The mapping from words that appear in text records to `WordIndex`es is
    // stored in a `Vocabulary` that stores `VocabIndex`es, so we have to
    // convert between those two types.
    // TODO<joka921> Can we make the returned `IndexType` a template parameter
    // of the vocabulary, s.t. we have a vocabulary that stores `WordIndex`es
    // directly?
    if (preFilterWidColumn[i].getWordVocabIndex() >= idRange.first() &&
        preFilterWidColumn[i].getWordVocabIndex() <= idRange.last()) {
      resultCidColumn[nofResultElements] = preFilterCidColumn[i];
      resultWidColumn[nofResultElements] = preFilterWidColumn[i];
      resultSidColumn[nofResultElements] = preFilterSidColumn[i];
      nofResultElements++;
    }
  }

  idTableResult.resize(nofResultElements);

  AD_LOG_DEBUG << "Filtering by ID range done. Result has "
               << idTableResult.numRows() << " elements.\n";
  return idTableResult;
}
