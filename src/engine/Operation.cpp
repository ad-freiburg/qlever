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
  const bool pinFinalResultButNotSubtrees =
      _executionContext->_pinResult && isRoot;
  const bool pinResult =
      _executionContext->_pinSubtrees || pinFinalResultButNotSubtrees;

  // When we pin the final result but no subtrees, we need to remember the sizes
  // of all involved index scans that have only one free variable. Note that
  // these index scans are executed already during query planning because they
  // have to be executed anyway, for any query plan. If we don't remember these
  // sizes here, future queries that take the result from the cache would redo
  // these index scans. Note that we do not need to remember the multiplicity
  // (and distinctness) because the multiplicity for an index scan with a single
  // free variable is always 1.
  if (pinFinalResultButNotSubtrees) {
    auto lock = getExecutionContext()->getPinnedSizes().wlock();
    forAllDescendants([&lock](QueryExecutionTree* child) {
      if (child->getType() == QueryExecutionTree::OperationType::SCAN &&
          child->getResultWidth() == 1) {
        (*lock)[child->getRootOperation()->asString()] =
            child->getSizeEstimate();
      }
    });
  }

  try {
    auto computeLambda = [this] {
      CacheValue val(getExecutionContext()->getAllocator());
      computeResult(val._resultTable.get());
      val._runtimeInfo = getRuntimeInfo();
      return val;
    };

    auto result = (pinResult) ? cache.computeOncePinned(cacheKey, computeLambda)
                              : cache.computeOnce(cacheKey, computeLambda);

    timer.stop();
    createRuntimeInformation(result, timer.msecs());
    return result._resultPointer->_resultTable;
  } catch (const ad_semsearch::AbortException& e) {
    // A child Operation was aborted, do not print the information again.
    throw;
  } catch (const ad_utility::WaitedForResultWhichThenFailedException& e) {
    LOG(ERROR) << "Waited for a result from another thread which then failed"
               << endl;
    LOG(ERROR) << asString();
    throw ad_semsearch::AbortException(e);
  } catch (const std::exception& e) {
    // We are in the innermost level of the exception, so print
    LOG(ERROR) << "Aborted Operation:" << endl;
    LOG(ERROR) << asString() << endl;
    LOG(ERROR) << e.what() << endl;
    // Rethrow as QUERY_ABORTED allowing us to print the Operation
    // only at innermost failure of a recursive call
    throw ad_semsearch::AbortException(e);
  } catch (...) {
    // We are in the innermost level of the exception, so print
    LOG(ERROR) << "Aborted Operation:" << endl;
    LOG(ERROR) << asString() << endl;
    LOG(ERROR)
        << "Unexpected exception that is not a subclass of std::exception"
        << endl;
    // Rethrow as QUERY_ABORTED allowing us to print the Operation
    // only at innermost failure of a recursive call
    throw ad_semsearch::AbortException(
        "Unexpected expection that is not a subclass of std::exception");
  }
}
