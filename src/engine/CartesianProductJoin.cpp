//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "CartesianProductJoin.h"

ResultTable CartesianProductJoin::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  std::vector<std::shared_ptr<const ResultTable>> subResults;
  std::ranges::for_each(children_, [&subResults](auto& child) {
    subResults.push_back(child->getResult());
  });

  auto sizesView = std::views::transform(
      subResults, [](const auto& child) { return child->size(); });
  auto totalSize = std::accumulate(sizesView.begin(), sizesView.end(), 1ul,
                                   std::multiplies{});
  idTable.resize(totalSize);

  size_t stride = 1;
  size_t columnIndex = 0;
  for (auto& subResultPtr : subResults) {
    const auto& input = subResultPtr->idTable();
    for (const auto& column : input.getColumns()) {
      decltype(auto) target = idTable.getColumn(columnIndex);
      // TODO<C++23> Use `std::views::stride`
      for (size_t i = 0, k = 0; i < column.size() && k < target.size();
           ++i, k += stride) {
        target[k] = column[i];
      }
      ++columnIndex;
    }
    stride *= input.numRows();
  }
  auto subResultsDeref = std::views::transform(
      subResults, [](auto& x) -> decltype(auto) { return *x; });
  return {std::move(idTable), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(subResultsDeref)};
}
