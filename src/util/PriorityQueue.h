/**
 * @copyright 2020, University of Freiburg, Chair of Algorithms and Data
 * @author Johannes Kalmbach (johannes.kalmbach@gmail.com)
 * @file Contains two implementations of Priority Queues that support updateKey
 * operations Both of them can be used as the first template parameter to the
 * ad_utility::FlexibleCache class template
 *
 * HeapBasedPQ is based on a heap(std::priority_queue). UpdateKey will cause
 * duplicates in the internal data structure which might make later pop
 * operations more expensive
 *
 * TreeBasedPQ is based on a balanced tree (std::multiset). It has stronger
 * asymptotic guarantees, but the constant factor of the log(n) operations might
 * be bigger because of the more complex tree structure
 */

#pragma once

#include <algorithm>
#include <memory>
#include <queue>
#include <set>
#include <variant>

#include "./Exception.h"
#include "./HashMap.h"
#include "./Log.h"

namespace ad_utility {
using std::make_shared;
using std::shared_ptr;

class EmptyPopException : public std::exception {
 public:
  EmptyPopException() = default;
  const char* what() const noexcept override {
    return "Pop was called on an empty priority queue!";
  }
};

class NotInPQException : public std::exception {
 public:
  NotInPQException() = default;
  const char* what() const noexcept override {
    return "PQ operation called on a handle that is not part of the PQ anymore";
  }
};

/**
 * A Priority Queue that supports updating Keys.
 * Is implemented as a balanced tree (std::multiset) under the  cover. All
 * operations have guaranteed Logarithmic time in the size of the map as long as
 * for Key there is only a constant number of different values with that key in
 * the queue.
 *
 * @tparam Score The Key on which the order of elements in the heap is decided.
 * Must be copyable and copying should be cheap.
 * @tparam Value The actual payload of the nodes. Must be moveable.
 * @tparam Comp  A comparator that overloads operator()(Score a, Score b). If it
 * returns true, then a is popped before b from the Queue
 */
template <class Score, class Value, class Comp = std::less<>>
class TreeBasedPQ {
 public:
  // we have to store Score-Value pairs in the tree, use a named struct for this
  struct Handle {
    Score mScore;
    shared_ptr<Value> mValue;
    const Score& score() const { return mScore; }
    Score& score() { return mScore; }
    const Value& value() const { return *mValue; }
    Value& value() { return *mValue; }
  };

 private:
  // lift a function that compares Scores to a function that compares handles by
  // looking only at their scores
  static auto makeComparator(Comp comp) {
    return [comp](const auto& a, const auto& b) {
      return comp(a.mScore, b.mScore);
    };
  }

  // the actual tree that stores our priorityQueue
  using MapType = std::multiset<
      Handle, std::decay_t<decltype(makeComparator(std::declval<Comp>()))>>;
  MapType mMap;

 public:
  /**
   * @brief Construct a Priority Queue
   * @param c If omitted will default-construct a Comparator object (will become
   * useful with default-constructible lambdas in C++20
   */
  TreeBasedPQ(Comp c = Comp()) : mMap(makeComparator(c)) {}

  /**
   * @brief Delete all the contents of the Priority Queue
   *
   * Note that the stored values are only deleted once all external
   * handles to them have been destroyed
   */
  void clear() { mMap.clear(); }

  /**
   * insert the score-value pair into the queue
   * @return  a handle to the inserted value that can be used to update the key
   */
  Handle insert(Score s, Value v) {
    Handle handle{std::move(s), make_shared<Value>(std::move(v))};
    mMap.insert(handle);
    return handle;
  }

  /**
   * Remove the element with the "smallest" key according to the comparison
   * function from the queue
   * @return  a handle to the removed node
   * @throws EmptyPopException if this Priority Queue is empty
   */
  Handle pop() {
    if (!size()) {
      throw EmptyPopException{};
    }
    auto handle = std::move(*mMap.begin());
    mMap.erase(mMap.begin());
    return handle;
  }

  /**
   * @brief erase a single value from the priority queue
   *
   * The value will only be destructed once all handles pointing to it go out of
   * scope
   * @throws NotInPQException if the handle does not point into the PQ (because
   * of pop or erase operations)
   */
  void erase(const Handle& handle) {
    auto it = find(handle);
    if (it != mMap.end()) {
      mMap.erase(it);
    } else {
      throw NotInPQException{};
    }
  }

  /// Returns true iff the handle still points into the PQ. Mostly used for unit
  /// tests
  bool contains(const Handle& handle) const {
    return find(handle) != mMap.end();
  }

  /**
   * Update the key of a value in the HeapBasedPQ
   * @param newKey
   * @param handle in/out parameter, will be changed to be a valid handle after
   * the operation has finished.
   * @throws NotInPQException if the handle does not point into the PQ (because
   * of pop or erase operations)
   */
  void updateKey(Score newKey, Handle* handle) {
    auto it = find(*handle);
    if (it != mMap.end()) {
      mMap.erase(it);
    } else {
      throw NotInPQException{};
    }
    handle->mScore = newKey;
    mMap.insert(*handle);
  }

  /// return the size of the map
  [[nodiscard]] size_t size() const { return mMap.size(); }

  /// is this pq empty
  [[nodiscard]] bool empty() const { return size() == 0; }

 private:
  // return an iterator into mMap where we find the handle that was passed in
  // returns mMap.end() if this handle is not valid anymore
  typename MapType::iterator find(const Handle& handle) const {
    auto [beg, end] = mMap.equal_range(handle);  // this only looks at the score
    // for the same score range, check for the equal values
    auto it = std::find_if(
        beg, end, [&](const auto& a) { return a.mValue == handle.mValue; });
    if (it == end) {
      it = mMap.end();
    }
    return it;
  }
};

/**
 * @brief A Priority Queue that also supports the updateKey operation
 *
 * It is built upon std::priority_queue (binary heap) by adding another level of
 * indirection. UpdateKey is handled by inserting another node into the heap
 * that has another score/key but points to the same internal handle. The handle
 * also stores the key (in addition to the value) so that we can decide whether
 * a node in the heap is still valid (the keys match) or if it has been
 * invalidated by a call to updateKey.
 * @tparam Score The Key on which the order of elements in the heap is decided.
 * Must be copyable and copying should be cheap.
 * @tparam Value The actual payload of the nodes. Must be moveable.
 * @tparam Comp  A comparator that overloads operator()(Score a, Score b). If it
 * returns true, then a is popped before b from the Queue
 */
template <class Score, class Value, class Comp = std::less<>>
class HeapBasedPQ {
 private:
  using InternalScore =
      std::variant<std::monostate,
                   Score>;  // monostate means "minimal key, erase immediately
  // The actual internal nodes of the Priority Queue storing the Score and the
  // Value
  struct PqNode {
    PqNode(Score s, Value&& v) : mScore(std::move(s)), mValue(std::move(v)) {}
    InternalScore mScore;
    Value mValue;
    // keep Track of whether this Node is currently a part of the PQ or not
    // (because of pop or erase  operations)
    bool mIsInPQ = true;
  };

 public:
  /**
   * These handles are returned by all the PriorityQueue operations (except
   * clear). They need to be passed in for the updateKey operation
   */
  class Handle {
    shared_ptr<PqNode> mData;  // correctly handle lifetime/ownership
   public:
    /// Take ownership of content
    explicit Handle(shared_ptr<PqNode>&& content) noexcept
        : mData(std::move(content)) {}
    /// default constructor currently needed for the Cache
    Handle() = default;
    /// simple accessors to the associated score and value of the content
    const Score& score() const { return std::get<Score>(mData->mScore); }
    Score& score() { return std::get<Score>(mData->mScore); }
    const Value& value() const { return mData->mValue; }
    Value& value() { return mData->mValue; }

    // was the value associated with this handle already deleted because of an
    // erase operation
    bool isValid() const { return (mData->mScore).index() == 1; };

    // does this handle point into the pq or was it popped/erased
    bool isInPq() const { return mData->mIsInPQ; }

    // signal that this handle is not part of the PQ anymore
    void erase() { mData->mIsInPQ = false; }

    // signal that this is not part of the PQ + reset the value to save memory
    void eraseAndInvalidate() {
      (mData->mScore).template emplace<0>();
      mData->mValue = Value{};
      mData->mIsInPQ = false;
    }
  };

 private:
  // These are the entries for the std::priority_queue binary heap
  // they store the score with which they were inserted and a handle
  // to the actual payload
  struct PqEntry {
    PqEntry(Score s, const Handle& handle) : mScore(s), mHandle(handle) {}
    Score mScore;
    Handle mHandle;
  };

  // lift the comparison function (which works on scores) to work on the entries
  // of the binary heap (that store such scores).
  static auto makeComparator(Comp comp) {
    // std::priority_queue is a max_heap by default, this makes it a min_heap as
    // documented, s.t. the two implementations behave similarly
    return [comp](const PqEntry& a, const PqEntry& b) {
      return !comp(a.mScore, b.mScore);
    };
  }

 private:
  // data members
  // type of the actually used comparison
  using InternalComparator =
      std::decay_t<decltype(makeComparator(std::declval<Comp>()))>;
  // storing the comparator is currently needed to implement the clear() method;
  const InternalComparator mComp;

  // std::priority_queue has no clear method, patch it in.
  struct ClearablePQ : public std::priority_queue<PqEntry, std::vector<PqEntry>,
                                                  InternalComparator> {
    using Base =
        std::priority_queue<PqEntry, std::vector<PqEntry>, InternalComparator>;
    using Base::Base;
    void clear() { this->c.clear(); }
  };

  ClearablePQ _pq;  // the actual priority queue
  // manually keep track of the actual number of different entries, since the
  // _pq member might have duplicates
  size_t mSize = 0;

 public:
  HeapBasedPQ(Comp comp = Comp{}) : mComp(makeComparator(comp)), _pq(mComp) {}

  /**
   * @brief delete all the content from the HeapBasedPQ
   *
   * Handles that were previously extracted/stored might still be used after
   * this, e.g. to extract the stored value/payload
   */
  void clear() {
    _pq.clear();
    mSize = 0;
  }

  /**
   * @brief erase the node associated with the handle.
   *
   * This invalidates the handle and sets the underlying value to a default
   * constructed one which possibly saves memory
   * @throws NotInPQException if the handle does not point into the PQ (because
   * of pop or erase operations)
   */
  void erase(Handle&& handle) {
    assertValidHandle(handle);
    handle.eraseAndInvalidate();
    mSize--;
  }

  /// Insert overload for rvalue references
  /// Insert with a score and a value, return a handle that can be later used to
  /// change the score
  Handle insert(Score s, Value v) {
    Handle handle{make_shared<PqNode>(s, std::move(v))};
    auto entry = PqEntry(std::move(s), handle);
    _pq.emplace(std::move(entry));
    mSize++;
    return handle;
  }

  /**
   * @brief Remove the node with the "smallest" score according to the held
   * comparator and return a handle to it
   *
   * Note: This might have non-constant complexity since we might encounter some
   * duplicate nodes which are "silently" removed first
   * TODO<joka921> check if the returning of the Handle is optimized out if we
   * don't use it.
   * @return A Handle to the node that was removed. It is save to look at this
   * handle's value
   * @throws EmptyPopException if this Priority Queue is empty
   */
  Handle pop() {
    // TODO<joka921> Discuss the handling of "Pop is invalid on empty PQs"
    pruneChangedKeys();
    if (_pq.empty()) {
      throw EmptyPopException{};
    }
    auto handle = std::move(_pq.top().mHandle);
    _pq.pop();
    mSize--;
    handle.erase();
    return handle;
  }

  /**
   * @brief Update (not necessarily decrease) the score/key of the value
   * associated with the handle
   *
   * @param newKey
   * @param handle in/out parameter: Updated s.t. it stays valid.
   * @throws NotInPQException if the handle does not point into the PQ (because
   * of pop or erase operations)
   */
  void updateKey(const Score& newKey, Handle* handle) {
    assertValidHandle(*handle);
    if (newKey == handle->score()) {
      return;
    }
    handle->score() = newKey;
    _pq.emplace(newKey, *handle);
  }

  /**
   * Returns the size of the internal std::priority_queue. Might be bigger than
   * the actual size due to multiple entries for the same value with "old" keys.
   */
  size_t technicalSize() const { return _pq.size(); }

  /// The logical size of the HeapBasedPQ (number of entries that are actually
  /// valid)
  size_t size() const { return mSize; }

  /// true iff size is 0
  [[nodiscard]] bool empty() const { return size() == 0; }

 private:
  /* helper function that pops all the internal nodes that have become invalid
     because
     of the updateKey operation until there is a valid node at _pq.top() */
  void pruneChangedKeys() {
    while (!_pq.empty()) {
      const auto& top = _pq.top();
      if (top.mHandle.isValid() && top.mScore == top.mHandle.score()) {
        return;
      }
      // a mismatch means, that somebody has changed the key, and this value is
      // no longer valid.
      _pq.pop();
    }
  }

  // throw if the handle is not a part of the pq
  void assertValidHandle(const Handle& h) {
    if (!h.isInPq()) {
      throw NotInPQException();
    }
  }
};

}  // namespace ad_utility
