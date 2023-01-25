// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/OrderBy.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Comparators.h"
#include "engine/QueryExecutionTree.h"
#include "global/ValueIdComparators.h"

using std::string;

// _____________________________________________________________________________
size_t OrderBy::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
OrderBy::OrderBy(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 vector<pair<size_t, bool>> sortIndices)
    : Operation{qec},
      subtree_{std::move(subtree)},
      sortIndices_{std::move(sortIndices)} {
  AD_CHECK(std::ranges::all_of(
      sortIndices_,
      [this](ColumnIndex index) { return index < getResultWidth(); },
      ad_utility::first));
}

// _____________________________________________________________________________
string OrderBy::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "ORDER BY on columns:";

  // TODO<joka921> This produces exactly the same format as SORT operations
  // which is crucial for caching. Please refactor those classes to one class
  // (this is only an optimization for sorts on a single column)
  for (auto ind : sortIndices_) {
    os << (ind.second ? "desc(" : "asc(") << ind.first << ") ";
  }
  os << "\n" << subtree_->asString(indent);
  return std::move(os).str();
}

// _____________________________________________________________________________
string OrderBy::getDescriptor() const {
  std::string orderByVars;
  for (const auto& p : subtree_->getVariableColumns()) {
    for (auto oc : sortIndices_) {
      if (oc.first == p.second) {
        if (oc.second) {
          orderByVars += " DESC(" + p.first.name() + ")";
        } else {
          orderByVars += " ASC(" + p.first.name() + ")";
        }
      }
    }
  }
  return "OrderBy on" + orderByVars;
}

// _____________________________________________________________________________
vector<size_t> OrderBy::resultSortedOn() const {
  // This function refers to the `internal` sorting by ID value. This is
  // different from the `semantic` sorting that this class creates.
  return {};
}

// _____________________________________________________________________________
void OrderBy::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for OrderBy result computation..." << endl;
  AD_CHECK(!sortIndices_.empty());
  shared_ptr<const ResultTable> subRes = subtree_->getResult();

  // TODO<joka921> proper timeout for sorting operations
  auto remainingTime = _timeoutTimer->wlock()->remainingTime();
  auto sortEstimateCancellationFactor =
      RuntimeParameters().get<"sort-estimate-cancellation-factor">();
  if (getExecutionContext()->getSortPerformanceEstimator().estimatedSortTime(
          subRes->size(), subRes->width()) >
      remainingTime * sortEstimateCancellationFactor) {
    // The estimated time for this sort is much larger than the actually
    // remaining time, cancel this operation
    throw ad_utility::TimeoutException(
        "OrderBy operation was canceled, because time estimate exceeded "
        "remaining time by a factor of " +
        std::to_string(sortEstimateCancellationFactor));
  }

  LOG(DEBUG) << "OrderBy result computation..." << endl;
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;

  result->_idTable = subRes->_idTable.clone();

  int width = result->_idTable.numColumns();

  // TODO<joka921> Measure (as soon as we have the benchmark merged)
  // whether it is beneficial to manually instantiate the comparison for
  // the case of sorting by only one or two columns.

  // TODO<joka921> In the case of a single variable, it might be more efficient
  // to first sort by the ID values and then "repair" the resulting range by
  // some O(n) algorithms, or even by returning lazy generators that yield
  // the repaired order.

  // TODO<joka921> For proper sorting of the local vocab we also need to
  // add some logic for the proper sorting.

  // TODO<joka921> If we know, that all the sort columns contain only datatypes
  // for which the `internal` order is also the `semantic` order, or if a column
  // only contains a single datatype, then we can use more efficient
  // implementations here.
  auto comparison = [this](const auto& a, const auto& b) {
    auto f = [](Id a, Id b) -> bool {
      return valueIdComparators::compareIds(a, b,
                                            valueIdComparators::Comparison::LT);
    };
    for (auto& [column, isDescending] : sortIndices_) {
      if (a[column] == b[column]) {
        continue;
      }
      return f(a[column], b[column]) != isDescending;
    }
    return false;
  };

  // We cannot use the `CALL_FIXED_SIZE` macro here because the `sort` function
  // is templated not only on the integer `I` (which the `callFixedSize`
  // function deals with) but also on the `comparison`.
  DISABLE_WARNINGS_CLANG_13
  ad_utility::callFixedSize(width, [&result, &comparison]<int I>() {
    Engine::sort<I>(&result->_idTable, comparison);
  });
  ENABLE_WARNINGS_CLANG_13
  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "OrderBy result computation done." << endl;
}
