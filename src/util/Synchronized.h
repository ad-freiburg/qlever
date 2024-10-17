// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)
// Created on 12.04.20.
// Inspired by Facebooks folly::Synchronized.

#ifndef QLEVER_SYNCHRONIZED_H
#define QLEVER_SYNCHRONIZED_H

#include <atomic>
#include <concepts>
#include <condition_variable>
#include <shared_mutex>

#include "absl/cleanup/cleanup.h"
#include "util/Forward.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"

namespace ad_utility {

// An empty tag-type
struct ConstructWithMutex {};

/// Does type M have a lock() and unlock() member function (behaves like a
/// mutex)
template <typename M, typename = void>
struct AllowsLocking : std::false_type {};

template <typename M>
struct AllowsLocking<M, std::void_t<decltype(std::declval<M&>().lock()),
                                    decltype(std::declval<M&>().unlock())>>
    : std::true_type {};

/// Does type M have lock(), unlock(), lock_shared() and unlock_shared() member
/// functions (behaves like a shared_mutex)
template <typename M, typename = void>
struct AllowsSharedLocking : std::false_type {};

template <typename M>
struct AllowsSharedLocking<
    M, std::void_t<AllowsLocking<M>, decltype(std::declval<M&>().lock_shared()),
                   decltype(std::declval<M&>().unlock_shared())>>
    : std::true_type {};

/// A very simple spin lock, stolen from cppreference. A spin lock is simply a
/// lock that actively waits (as long as is necessary) for the lock to be
/// released before locking it. In particular, this is OK when using it for
/// serializing simple and fast concurrent accesses to an object.
class SpinLock {
 private:
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;

 public:
  void lock() {
    while (lock_.test_and_set(std::memory_order_acquire)) {
    }  // spin
  }
  void unlock() { lock_.clear(std::memory_order_release); }
};

// forward declaration
template <class S, bool b, bool c>
class LockPtr;

/**
 * @brief Combines an arbitrary type with a mutex to only perform locked
 * operations on the underlying data
 *
 * Inspired and similar interface to facebooks folly::synchronized (but much
 * simpler to only meet our needs)
 *
 * @tparam T The actual type that is stored
 * @tparam Mutex A Mutex like type (e.g. std::mutex or std::shared_mutex).
 * Defaults to std::shared_mutex
 */
template <typename T, typename Mutex = std::shared_mutex,
          typename = std::enable_if_t<AllowsLocking<Mutex>::value>>
class Synchronized {
 public:
  using value_type = std::remove_reference_t<T>;

  /// is this a shared_mutex?
  constexpr static bool isShared = AllowsSharedLocking<Mutex>::value;

  /// Not copyable because of the Mutex
  Synchronized(const Synchronized&) = delete;
  Synchronized& operator=(const Synchronized&) = delete;
  /// default Movable
  Synchronized(Synchronized&&) noexcept = default;
  Synchronized& operator=(Synchronized&&) noexcept = default;

  Synchronized() requires std::default_initializable<T> = default;
  ~Synchronized() = default;

  /// Constructor that is not copy or move, tries to instantiate the underlying
  /// type via perfect forwarding (this includes the default constructor)
  template <typename Arg, typename... Args>
  requires(!std::same_as<std::remove_cvref_t<Arg>, Synchronized>)
  explicit(sizeof...(Args) == 0) Synchronized(Arg&& arg, Args&&... args)
      : data_{AD_FWD(arg), AD_FWD(args)...}, m_{} {}

  template <typename... Args>
  Synchronized(ConstructWithMutex, Mutex mutex, Args&&... args)
      : data_{std::forward<Args>(args)...}, m_{mutex} {}

  /** @brief Obtain an exclusive lock and then call f() on the underlying data
   * type, return the result.
   *
   * Note that return type deduction is done via auto which means,
   * that no references are passed out. This happens deliberately as
   * passing out references to the underlying type would ignore the locking.
   */
  template <typename F>
  auto withWriteLock(F f) {
    std::lock_guard l(m_);
    return f(data_);
  }

  /// const overload of `withWriteLock`
  template <typename F>
  auto withWriteLock(F f) const {
    std::lock_guard l(mutex());
    return f(data_);
  }

  /// Similar to `withWriteLock`, but additionally guarantees that the request
  /// with requestNumber 0 is performed first, then comes requestNumber 1 etc.
  /// If a request number in the range [0...k] is missing, then the program will
  /// deadlock. See `/src/index/Index.cpp` for an example.
  template <typename F>
  auto withWriteLockAndOrdered(F f, size_t requestNumber) {
    std::unique_lock l(m_);
    // It is important to create this AFTER the lock, s.t. the
    // nextOrderedRequest_ update is still protected. We must give it a name,
    // s.t. it is not destroyed immediately.
    auto od = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([&]() {
      ++nextOrderedRequest_;
      l.unlock();
      requestCv_.notify_all();
    });
    requestCv_.wait(l, [&]() { return requestNumber == nextOrderedRequest_; });
    return f(data_);
  }

  /** @brief Obtain a shared lock and then call f() on the underlying data type,
   * return the result.
   *
   * Note that return type deduction is done via auto which means,
   * that no references are passed out. This happens deliberately as
   * passing out references to the underlying type would ignore the locking.
   * Only supported if the mutex allows shared locking and the
   * function type F only treats its object as const.
   */
  template <typename F, bool s = isShared,
            typename Res = std::invoke_result_t<F, const T&>>
  std::enable_if_t<s, Res> withReadLock(F f) const {
    std::shared_lock l(mutex());
    return f(data_);
  }

  /**
   * @brief Obtain a handle that can be treated like a T* to the underlying type
   * with exclusive access.
   *
   *
   * If the Return value is stored, then the T will remain locked until the
   * return value is destroyed. If the return value outlives the Synchronized
   * element by which it was obtained, the behavior is undefined.
   *
   * Examples:
   * Synchronized<std::vector<int>> s;
   * s.wlock()->push_back(3);  // obtain lock, push back, release lock
   * {
   *   auto l = s.wlock(); // s is now locked by l;
   *   l->push_back(5); // push back, remain locked.
   *   // s.wlock()->push_back(0) // would deadlock, because s is still locked
   * by l; } // l goes out of scope and unlocks s again;
   * s.wlock()->push_back(7);
   *
   */
  auto wlock() { return LockPtr<Synchronized, false, false>{this}; }

  // exclusive lock, but only const access to underlying data allowed
  auto wlock() const { return LockPtr<Synchronized, false, true>{this}; }

  /**
   * @brief Obtain a handle that can be treated like a const T* to the
   * underlying type with shared access. Only works if the Mutex type allows
   * shared locking.
   *
   * If the Return value is stored, then the T will remain shared_locked until
   * the return value is destroyed. If the return value outlives the
   * Synchronized element by which it was obtained, the behavior is undefined.
   *
   * Examples:
   * Synchronized<std::vector<int>> s;
   * cout << s.rlock()->size();  // obtain shared_lock, obtain size, release
   * shared_lock
   * // s.rlock()->push_back(3);  // does not compile, because rlock() only
   * allows access to const members
   * {
   *   auto l = s.rlock(); // s is now shared_locked by l;
   *   cout << l->size(); // get size, remain shared_locked.
   *   // s.wlock()->push_back(0) // would deadlock, because s is still locked
   * by l; cout << s.rlock()->size(); // works because multiple shared locks at
   * the same time are ok } // l goes out of scope and unlocks s again;
   * s.wlock()->push_back(7);
   *
   */
  template <typename M = Mutex>
  std::enable_if_t<AllowsSharedLocking<M>::value,
                   LockPtr<Synchronized, true, true>>
  rlock() const {
    return LockPtr<Synchronized, true, true>{this};
  };

  // Return a `Synchronized` that uses a reference to this `Synchronized`'s
  // `_data` and `mutext_`. The reference is a reference of the Base class U.
  template <typename U>
  requires std::is_base_of_v<U, T> Synchronized<U&, Mutex&> toBaseReference() {
    return {ConstructWithMutex{}, mutex(), data_};
  }

 private:
  T data_{};  // The data to which we synchronize the access.
  Mutex m_;   // The used mutex

  Mutex& mutex() const { return const_cast<Mutex&>(m_); }

  // These are used for the withWriteLockAndOrdered function
  size_t nextOrderedRequest_ = 0;
  std::condition_variable_any requestCv_;

  template <class S, bool b, bool c>
  friend class LockPtr;  // The LockPtr implementation requires private access
};

/// handle to a locked Synchronized class
template <class S, bool isSharedLock, bool isConst>
class LockPtr {
  using value_type = typename S::value_type;
  // either store a Pointer or a Pointer to const
  using ptr_type = std::conditional_t<isConst, const S* const, S* const>;

 private:
  // construction is private and only allowed by the Synchronized class
  template <typename T, typename M, typename>
  friend class Synchronized;

  // store a pointer to the parent object and immediately lock it.
  explicit LockPtr(ptr_type s) : s_(s) {
    static_assert(isConst ||
                  !isSharedLock);  // if we only have a shared lock, we may only
                                   // perform const operations
    if constexpr (isSharedLock) {
      s_->mutex().lock_shared();
    } else {
      s_->mutex().lock();
    }
  }

 public:
  // destructor releases the lock
  ~LockPtr() {
    if constexpr (isSharedLock) {
      s_->mutex().unlock_shared();
    } else {
      s_->mutex().unlock();
    }
  }

  /// Access to underlying data. Non const access is only allowed for exclusive
  /// locks that are not const.
  template <bool s = isConst>
  std::enable_if_t<!s, value_type&> operator*() {
    return s_->data_;
  }

  /// Access to underlying data.
  const value_type& operator*() const { return s_->data_; }

  /// Access to underlying data. Non const access is only allowed for exclusive
  /// locks that are not const.
  template <bool s = isConst>
  std::enable_if_t<!s, value_type*> operator->() {
    return &s_->data_;
  }

  /// Access to underlying data.
  const value_type* operator->() const { return &s_->data_; }

 private:
  ptr_type s_;
};

}  // namespace ad_utility

#endif  // QLEVER_SYNCHRONIZED_H
