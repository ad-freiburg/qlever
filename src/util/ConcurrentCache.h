// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#ifndef QLEVER_CONCURRENTCACHE_H
#define QLEVER_CONCURRENTCACHE_H
#include <condition_variable>
#include <memory>
#include <mutex>
#include <utility>

#include "../util/HashMap.h"
#include "./Synchronized.h"

namespace ad_utility {

using std::make_shared;
using std::shared_ptr;

/** This exception is thrown if we are waiting for a computation result,
 * which is computed by a different thread and the computation in this
 * other thread fails.
 */
class WaitedForResultWhichThenFailedException : public std::exception {
  const char* what() const noexcept override {
    return "Waited for a result from another thread which then failed";
  }
};

// Implementation details, do not call them from outside this module.
namespace ConcurrentCacheDetail {

/**
 * @brief A result of an expensive computation, that is only computed once
 * @tparam Value - The result type of the computation.
 *
 * Usage: Multiple threads who wait for the same computational result of type
 *        Value hold a pointer to this class. Exactly one of them actually
 *        computes the result and supplies it via the finish() method, or calls
 *        abort() to signal that the computation has failed. The other threads
 *        may only call getResult(). This call blocks, until finish() or
 *        abort() is called from the computing threads. If the result is
 *        aborted, the call to getResult() will throw an
 *        WaitedForResultWhichThenFailedException
 *
 *       This class is thread-safe
 */
template <typename Value>
class ResultInProgress {
 public:
  ResultInProgress() = default;

  // Distribute the computation results to all the threads that at some point
  // have called or will call getResult(). Check that none of the other threads
  // waiting for the result have already finished or were aborted.
  void finish(shared_ptr<Value> result) {
    std::lock_guard lockGuard(_mutex);
    AD_CHECK(_status == Status::IN_PROGRESS);
    _status = Status::FINISHED;
    _result = std::move(result);
    _conditionVariable.notify_all();
  }

  // Signal the failure of the computation to all the threads that at some point
  // have called or will call getResult().
  // If the total number of calls to finish() or abort() exceeds 1, the program
  // will terminate.
  void abort() {
    AD_CHECK(_status == Status::IN_PROGRESS);
    std::lock_guard lockGuard(_mutex);
    _status = Status::ABORTED;
    _conditionVariable.notify_all();
  }

  // Wait for another thread to finish the computation and obtain the result.
  // If the computation is aborted, this function throws an
  // AbortedInOtherThreadException
  shared_ptr<const Value> getResult() {
    std::unique_lock uniqueLock(_mutex);
    _conditionVariable.wait(uniqueLock,
                            [this] { return _status != Status::IN_PROGRESS; });
    if (_status == ResultInProgress::Status::ABORTED) {
      throw WaitedForResultWhichThenFailedException{};
    }
    return _result;
  }

 private:
  enum class Status { IN_PROGRESS, FINISHED, ABORTED };
  shared_ptr<const Value> _result;
  // See this SO answer for why mutable is ok here
  // https://stackoverflow.com/questions/3239905/c-mutex-and-const-correctness
  mutable std::condition_variable _conditionVariable;
  mutable std::mutex _mutex;
  Status _status = Status::IN_PROGRESS;
};
}  // namespace ConcurrentCacheDetail

/**
 * @brief Makes sure that an expensive, deterministic computation result is
 * reused, if it is already cached or currently being computed by another
 * thread. Also allows transparent access to the underlying cache.
 * @tparam Cache The underlying cache type. The cache implementations from the
 *         ad_utility namespace all fulfill the requirements.
 */
template <typename Cache>
class ConcurrentCache {
 public:
  using Value = typename Cache::value_type;
  using Key = typename Cache::key_type;

  /// Constructor: all arguments are forwarded to the underlying cache type.
  template <typename... CacheArgs>
  ConcurrentCache(CacheArgs&&... cacheArgs)
      : _cacheAndInProgressMap{std::forward<CacheArgs>(cacheArgs)...} {}

  struct ResultAndCacheStatus {
    shared_ptr<const Value> _resultPointer;
    bool _wasCached;
  };

 public:
  /**
   * @brief Obtain the result of an expensive computation. Do not recompute the
   * result if it is cached or currently being computed by another thread.
   * @tparam ComputeFunction A callable whose operator() takes no argument and
   * produces the computation result.
   * @param key  A key that can uniquely identify a computation. For equal keys,
   * the associated computeFunctions must yield the same results.
   * @param computeFunction The actual computation. If the result has to be
   * computed, computeFunction() is called.
   * @return A shared_ptr to the computation result.
   *
   */
  template <class ComputeFunction>
  ResultAndCacheStatus computeOnce(const Key& key,
                                   ComputeFunction computeFunction) {
    return computeOnceImpl(false, key, std::move(computeFunction));
  }

  /// Similar to computeOnce, with the following addition: After the call
  /// completes, the result will be pinned in the underlying cache.
  template <class ComputeFunction>
  ResultAndCacheStatus computeOncePinned(const Key& key,
                                         ComputeFunction computeFunction) {
    return computeOnceImpl(true, key, std::move(computeFunction));
  }

  /// Clear the cache (but not the pinned entries)
  void clearUnpinnedOnly() {
    _cacheAndInProgressMap.wlock()->_cache.clearUnpinnedOnly();
  }

  /// Clear the cache, including the pinned entries.
  void clearAll() { _cacheAndInProgressMap.wlock()->_cache.clearAll(); }

  /// The number of non-pinned entries in the cache
  auto numNonPinnedEntries() const {
    return _cacheAndInProgressMap.wlock()->_cache.numNonPinnedEntries();
  }

  /// The number of pinned entries in the underlying cache
  auto numPinnedEntries() const {
    return _cacheAndInProgressMap.wlock()->_cache.numPinnedEntries();
  }

  /// Total size of the non-pinned entries in the cache (the unit depends on
  /// the cache's configuration)
  auto nonPinnedSize() const {
    return _cacheAndInProgressMap.wlock()->_cache.nonPinnedSize();
  }

  /// Total size of the non-pinned entries in the cache (the unit depends on
  /// the cache's configuration)
  auto pinnedSize() const {
    return _cacheAndInProgressMap.wlock()->_cache.pinnedSize();
  }

  /// only for testing: get access to the implementation
  auto& getStorage() { return _cacheAndInProgressMap; }

  // is key in cache (not in progress), used for testing
  bool cacheContains(const Key& k) const {
    return _cacheAndInProgressMap.wlock()->_cache.contains(k);
  }

  // Get entry from cache by its key. If the key is not present in the cache,
  // the behavior depends on the behavior of operator[] of _cache. The
  // ad_utility Cache returns a nullptr in that case. The return type is then
  // shared_ptr.
  auto resultAt(const Key& k) {
    return _cacheAndInProgressMap.wlock()->_cache[k];
  }

 private:
  using ResultInProgress = ConcurrentCacheDetail::ResultInProgress<Value>;

  // We hold a cache, and a hashtable in which we store all the computations
  // that are currently in progress (only finished results are added to the
  // cache)
  struct CacheAndInProgressMap {
    Cache _cache;
    // Values that are currently being computed. The bool tells us whether this
    // result will be pinned in the cache.
    HashMap<Key, std::pair<bool, shared_ptr<ResultInProgress>>> _inProgress;
    template <typename... Args>
    CacheAndInProgressMap(Args&&... args)
        : _cache{std::forward<Args>(args)...} {}
  };

  // make the whole class thread-safe by making all the data members thread-safe
  using SyncCache = ad_utility::Synchronized<CacheAndInProgressMap, std::mutex>;

  // delete the operation with the key from the hash map of the operations that
  // are in progress, and add it to the cache using the computationResult
  // Will crash if the key cannot be found in the hash map
  void moveFromInProgressToCache(Key key, shared_ptr<Value> computationResult) {
    // Obtain a lock for the whole operation, making it atomic.
    auto lockPtr = _cacheAndInProgressMap.wlock();
    AD_CHECK(lockPtr->_inProgress.contains(key));
    bool pinned = lockPtr->_inProgress[key].first;
    if (pinned) {
      lockPtr->_cache.insertPinned(std::move(key),
                                   std::move(computationResult));
    } else {
      lockPtr->_cache.insert(std::move(key), std::move(computationResult));
    }
    lockPtr->_inProgress.erase(key);
  }

 private:
  // implementation for computeOnce (pinned and normal variant).
  template <class ComputeFunction>
  ResultAndCacheStatus computeOnceImpl(bool pinned, const Key& key,
                                       ComputeFunction computeFunction) {
    bool mustCompute;
    shared_ptr<ResultInProgress> resultInProgress;
    // first determine whether we have to compute the result,
    // this is done atomically by locking the storage for the whole time
    {
      auto lockPtr = _cacheAndInProgressMap.wlock();
      bool contained = pinned
                           ? lockPtr->_cache.containsAndMakePinnedIfExists(key)
                           : lockPtr->_cache.contains(key);
      if (contained) {
        // the result is in the cache, simply return it.
        return {static_cast<shared_ptr<const Value>>(lockPtr->_cache[key]),
                true};
      } else if (lockPtr->_inProgress.contains(key)) {
        // the result is not cached, but someone else is computing it.
        // it is important, that we do not immediately call getResult() since
        // this call blocks and we currently hold a lock.

        // if we want to pin the result, but the computing thread doesn't,
        // inform them about this
        lockPtr->_inProgress[key].first |= pinned;
        mustCompute = false;
        // store a handle to the computation.
        resultInProgress = lockPtr->_inProgress[key].second;
      } else {
        // we are the first to compute this result, setup a blank
        // result to which we can write.
        mustCompute = true;
        resultInProgress = make_shared<ResultInProgress>();
        lockPtr->_inProgress[key] = std::pair(pinned, resultInProgress);
      }
    }  // release the lock, it is not required while we are computing
    if (mustCompute) {
      LOG(TRACE) << "Not in the cache, need to compute result" << std::endl;
      try {
        // The actual computation
        shared_ptr<Value> result = make_shared<Value>(computeFunction());
        moveFromInProgressToCache(key, result);
        // Signal other threads who are waiting for the results.
        resultInProgress->finish(result);
        // result was not cached
        return {std::move(result), false};
      } catch (...) {
        // Other threads may try this computation again in the future
        _cacheAndInProgressMap.wlock()->_inProgress.erase(key);
        // Result computation has failed, signal the other threads,
        resultInProgress->abort();
        throw;
      }
    } else {
      // someone else is computing the result, wait till it is finished and
      // return the result, we do not count this case as "cached" as we had to
      // wait.
      return {resultInProgress->getResult(), false};
    }
  }

  // Data members
  SyncCache _cacheAndInProgressMap;  // the data storage
};
}  // namespace ad_utility

#endif  // QLEVER_CONCURRENTCACHE_H
