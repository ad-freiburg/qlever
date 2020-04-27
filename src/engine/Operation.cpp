// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach  (johannes.kalmbach@gmail.com)

#include "Operation.h"
#include "QueryExecutionTree.h"

template <typename F>
void Operation::forAllDescendants(F f) {
  static_assert(
      std::is_same_v<void, std::invoke_result_t<F, QueryExecutionTree*>>);
  for (auto ptr : getChildren()) {
    if (ptr) {
      f(ptr);
      ptr->forAllDescendants(f);
    }
  }
}

template <typename F>
void Operation::forAllDescendants(F f) const {
  static_assert(
      std::is_same_v<void, std::invoke_result_t<F, const QueryExecutionTree*>>);
  for (auto ptr : getChildren()) {
    if (ptr) {
      f(ptr);
      ptr->forAllDescendants(f);
    }
  }
}

// __________________________________________________________________________________________________________
vector<string> Operation::collectWarnings() const {
  vector<string> res = getWarnings();
  for (auto child : getChildren()) {
    if (!child) {
      continue;
    }
    auto recursive = child->collectWarnings();
    res.insert(res.end(), std::make_move_iterator(recursive.begin()),
               std::make_move_iterator(recursive.end()));
  }

  return res;
}

// Get the result for the subtree rooted at this element.
// Use existing results if they are already available, otherwise
// trigger computation.
shared_ptr<const ResultTable> Operation::getResult(bool isRoot) {
  ad_utility::Timer timer;
  timer.start();
  auto& cache = _executionContext->getQueryTreeCache();
  const string cacheKey = asString();
  const bool pinChildIndexScanSizes = _executionContext->_pinResult && isRoot;
  const bool pinResult =
      _executionContext->_pinSubtrees || pinChildIndexScanSizes;
  LOG(TRACE) << "Check cache for Operation result" << endl;
  LOG(TRACE) << "Using key: \n" << cacheKey << endl;
  auto [newResult, existingResult] =
      (pinResult)
          ? cache.tryEmplacePinned(cacheKey, _executionContext->getAllocator())
          : cache.tryEmplace(cacheKey, _executionContext->getAllocator());

  if (pinChildIndexScanSizes) {
    auto lock = getExecutionContext()->getPinnedSizes().wlock();
    forAllDescendants([&lock](QueryExecutionTree* child) {
      if (child->getType() == QueryExecutionTree::OperationType::SCAN) {
        (*lock)[child->getRootOperation()->asString()] =
            child->getSizeEstimate();
      }
    });
  }

  if (newResult) {
    LOG(TRACE) << "Not in the cache, need to compute result" << endl;
    LOG(DEBUG) << "Available memory (in MB) before operation: "
               << (_executionContext->getAllocator().numFreeBytes() >> 20)
               << std::endl;
    // Passing the raw pointer here is ok as the result shared_ptr remains
    // in scope
    try {
      computeResult(newResult->_resTable.get());
    } catch (const ad_semsearch::AbortException& e) {
      // A child Operation was aborted, abort this Operation
      // as well. The child already printed
      abort(newResult, false);
      // Continue unwinding the stack
      throw;
    } catch (const std::exception& e) {
      // We are in the innermost level of the exception, so print
      abort(newResult, true);
      // Rethrow as QUERY_ABORTED allowing us to print the Operation
      // only at innermost failure of a recursive call
      throw ad_semsearch::AbortException(e);
    } catch (...) {
      // We are in the innermost level of the exception, so print
      abort(newResult, true);
      LOG(ERROR) << "WEIRD_EXCEPTION not inheriting from std::exception"
                 << endl;
      // Rethrow as QUERY_ABORTED allowing us to print the Operation
      // only at innermost failure of a recursive call
      throw ad_semsearch::AbortException("WEIRD_EXCEPTION");
    }
    timer.stop();
    _runtimeInfo.setRows(newResult->_resTable->size());
    _runtimeInfo.setCols(getResultWidth());
    _runtimeInfo.setDescriptor(getDescriptor());
    _runtimeInfo.setColumnNames(getVariableColumns());

    _runtimeInfo.setTime(timer.msecs());
    _runtimeInfo.setWasCached(false);
    // cache the runtime information for the execution as well
    newResult->_runtimeInfo = _runtimeInfo;
    // Only now we can let other threads access the result
    // and runtime information
    newResult->_resTable->finish();
    return newResult->_resTable;
  }

  existingResult->_resTable->awaitFinished();
  if (existingResult->_resTable->status() == ResultTable::ABORTED) {
    LOG(ERROR) << "Operation aborted while awaiting result" << endl;
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "Operation aborted while awaiting result");
  }
  timer.stop();
  _runtimeInfo = existingResult->_runtimeInfo;
  // We need to update column names and descriptor as we may have cached with
  // different variable names
  _runtimeInfo.setDescriptor(getDescriptor());
  _runtimeInfo.setColumnNames(getVariableColumns());
  _runtimeInfo.setTime(timer.msecs());
  _runtimeInfo.setWasCached(true);
  _runtimeInfo.addDetail("original_total_time",
                         existingResult->_runtimeInfo.getTime());
  _runtimeInfo.addDetail("original_operation_time",
                         existingResult->_runtimeInfo.getOperationTime());
  return existingResult->_resTable;
}
