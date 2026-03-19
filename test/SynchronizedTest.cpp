//
// Created by johannes on 12.04.20.
//

#include <gtest/gtest.h>

#include "util/Synchronized.h"

using ad_utility::Synchronized;

TEST(Synchronized, TypeTraits) {
  static_assert(ad_utility::AllowsLocking<std::mutex>::value);
  static_assert(ad_utility::AllowsLocking<std::shared_mutex>::value);
  static_assert(!ad_utility::AllowsLocking<std::vector<int>>::value);
  static_assert(!ad_utility::AllowsLocking<float>::value);
  static_assert(ad_utility::AllowsSharedLocking<std::shared_mutex>::value);
  static_assert(!ad_utility::AllowsSharedLocking<std::mutex>::value);
}

TEST(Synchronized, Exclusive) {
  Synchronized<int, std::mutex> i;
  i.withWriteLock([](auto& i) { i += 2; });
  *i.wlock() += 2;
  {
    auto lc = i.wlock();
    auto& intval = *lc;
    intval += 2;
  }
  const auto& i2 = i;
  auto res = i2.withWriteLock([](const auto& x) { return x; });
  ASSERT_EQ(res, 6);
}

// Test the move semantics of the `wlock()` objects, a moved from object neither
// owns the lock, nor does it try to (wrongly) delete it in its destructor.
TEST(Synchronized, MovingOfLockObjects) {
  Synchronized<int> i{3};
  {
    auto lock = i.wlock();
    auto lock2 = std::move(lock);
    {
      // At this point, `lock3` own the lock, and will release it when
      // begin destroyed.
      auto lock3 = std::move(lock2);
    }

    lock = i.wlock();
    *lock = 42;
  }
  ASSERT_EQ(*i.rlock(), 42);
}

TEST(Synchronized, Shared) {
  Synchronized<int, std::shared_mutex> i;
  i.withWriteLock([](auto& i) { i += 2; });
  *i.wlock() += 2;
  {
    auto lc = i.wlock();
    auto& intval = *lc;
    intval += 2;
  }
  const auto& i2 = i;
  auto res = i2.withWriteLock([](const auto& x) { return x; });
  ASSERT_EQ(res, 6);
  ASSERT_EQ(*i.rlock(), 6);
}

TEST(Synchronized, Vector) {
  {
    // test correct promotion of references
    Synchronized<std::vector<int>, std::mutex> s;
    s.wlock()->push_back(3);
    ASSERT_EQ(s.wlock()->size(), 1u);
    ASSERT_EQ((*s.wlock())[0], 3);

    (*s.wlock())[0] = 5;
    ASSERT_EQ(s.wlock()->size(), 1u);
    ASSERT_EQ((*s.wlock())[0], 5);

    decltype(auto) res = s.withWriteLock([](auto& v) {
      v.push_back(7);
      return v.back();
    });
    // we don't pass references out without locking;
    ASSERT_EQ(7, res);
    static_assert(std::is_same_v<int, decltype(res)>);
    ASSERT_EQ(s.wlock()->size(), 2u);
    ASSERT_EQ(s.wlock()->back(), 7);
  }

  {
    // test correct promotion of references
    Synchronized<std::vector<int>, std::shared_mutex> s;
    s.wlock()->push_back(3);
    ASSERT_EQ(s.wlock()->size(), 1u);
    ASSERT_EQ((*s.rlock())[0], 3);

    (*s.wlock())[0] = 5;
    ASSERT_EQ(s.rlock()->size(), 1u);
    ASSERT_EQ((*s.rlock())[0], 5);

    s.wlock()->push_back(7);
    decltype(auto) res = s.withReadLock([](const auto& v) { return v.back(); });
    // we don't pass references out without locking;
    ASSERT_EQ(7, res);
    static_assert(std::is_same_v<int, decltype(res)>);
    ASSERT_EQ(s.wlock()->size(), 2u);
    ASSERT_EQ(s.wlock()->back(), 7);
  }
}

TEST(Synchronized, MutexReference) {
  std::shared_mutex m;
  int i = 0;
  ad_utility::Synchronized<int&, std::shared_mutex&> s{
      ad_utility::ConstructWithMutex{}, m, i};

  *(s.wlock()) = 4;

  ASSERT_EQ(i, 4);
  ASSERT_EQ(*(s.rlock()), 4);
}

TEST(Synchronized, ToBaseReference) {
  struct A {
    virtual void f() = 0;
    [[nodiscard]] virtual int g() const = 0;
    virtual ~A() = default;
  };

  struct B : A {
    int x = 0;
    void f() override { x += 3; }
    [[nodiscard]] int g() const override { return x; }
  };

  ad_utility::Synchronized<B> bSync;
  auto aSync = bSync.toBaseReference<A>();
  aSync.wlock()->f();
  bSync.wlock()->f();
  ASSERT_EQ(aSync.rlock()->g(), 6);
  ASSERT_EQ(bSync.rlock()->g(), 6);

  // TODO<joka921>: Test, that the locking was indeed successful.
}

TEST(Synchronized, Copyable) {
  Synchronized<int> s1{3};
  Synchronized<int> s2{s1};
  Synchronized<int> s3{0};
  s3 = s1;
  EXPECT_EQ(*s2.wlock(), 3);
  EXPECT_EQ(*s3.wlock(), 3);

  *s1.wlock() = 42;
  EXPECT_EQ(*s1.wlock(), 42);
  EXPECT_EQ(*s2.wlock(), 3);
  EXPECT_EQ(*s3.wlock(), 3);
}

template <typename T, typename = void>
struct AllowsNonConstExclusiveAccess : public std::false_type {};

template <typename T>
struct AllowsNonConstExclusiveAccess<
    T, std::void_t<decltype(std::declval<T>().wlock()->push_back(3))>>
    : public std::true_type {};

template <typename T, typename = void>
struct AllowsConstExclusiveAccess : public std::false_type {};

template <typename T>
struct AllowsConstExclusiveAccess<
    T, std::void_t<decltype(std::declval<T>().wlock()->size())>>
    : public std::true_type {};

template <typename T, typename = void>
struct AllowsConstSharedAccess : public std::false_type {};

template <typename T>
struct AllowsConstSharedAccess<
    T, std::void_t<decltype(std::declval<T>().rlock()->size())>>
    : public std::true_type {};

template <typename T, typename = void>
struct AllowsNonConstSharedAccess : public std::false_type {};

template <typename T>
struct AllowsNonConstSharedAccess<
    T, std::void_t<decltype(std::declval<T>().rlock()->push_back(3))>>
    : public std::true_type {};

struct NC {
  auto operator()(std::vector<int>& v) const { v.push_back(3); }
};

struct C {
  template <class V>
  size_t operator()(const V& v) const {
    return v.size();
  }
};

template <typename T, typename = void>
struct AllowsNonConstExclusiveAccessLambda : public std::false_type {};

template <typename T>
struct AllowsNonConstExclusiveAccessLambda<
    T,
    std::void_t<decltype(std::declval<T>().withWriteLock(std::declval<NC>()))>>
    : public std::true_type {};

template <typename T, typename = void>
struct AllowsConstExclusiveAccessLambda : public std::false_type {};

template <typename T>
struct AllowsConstExclusiveAccessLambda<
    T,
    std::void_t<decltype(std::declval<T>().withWriteLock(std::declval<C>()))>>
    : public std::true_type {};

template <typename T, typename = void>
struct AllowsConstSharedAccessLambda : public std::false_type {};

template <typename T>
struct AllowsConstSharedAccessLambda<
    T, std::void_t<decltype(std::declval<T>().withReadLock(std::declval<C>()))>>
    : public std::true_type {};

template <typename T, typename = void>
struct AllowsNonConstSharedAccessLambda : public std::false_type {};

template <typename T>
struct AllowsNonConstSharedAccessLambda<
    T,
    std::void_t<decltype(std::declval<T>().withReadLock(std::declval<NC>()))>>
    : public std::true_type {};

using Vec = std::vector<int>;
TEST(Synchronized, SFINAE) {
  static_assert(
      AllowsNonConstExclusiveAccess<Synchronized<Vec, std::mutex>>::value);
  static_assert(
      AllowsConstExclusiveAccess<Synchronized<Vec, std::mutex>>::value);
  static_assert(!AllowsConstSharedAccess<Synchronized<Vec, std::mutex>>::value);
  static_assert(
      AllowsNonConstExclusiveAccess<Synchronized<Vec, std::mutex>>::value);
  static_assert(AllowsNonConstExclusiveAccessLambda<
                Synchronized<Vec, std::mutex>>::value);
  static_assert(
      AllowsConstExclusiveAccessLambda<Synchronized<Vec, std::mutex>>::value);
  static_assert(
      !AllowsConstSharedAccessLambda<Synchronized<Vec, std::mutex>>::value);
  static_assert(AllowsNonConstExclusiveAccessLambda<
                Synchronized<Vec, std::mutex>>::value);

  static_assert(AllowsNonConstExclusiveAccess<
                Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(
      AllowsConstExclusiveAccess<Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(
      AllowsConstSharedAccess<Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(
      !AllowsNonConstSharedAccess<Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(AllowsNonConstExclusiveAccessLambda<
                Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(AllowsConstExclusiveAccessLambda<
                Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(AllowsConstSharedAccessLambda<
                Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(!AllowsNonConstSharedAccessLambda<
                Synchronized<Vec, std::shared_mutex>>::value);

  static_assert(!AllowsNonConstExclusiveAccess<
                const Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(AllowsConstExclusiveAccess<
                const Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(AllowsConstSharedAccess<
                const Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(!AllowsNonConstSharedAccess<
                const Synchronized<Vec, std::shared_mutex>>::value);

  // Outcommenting this does not compile and cannot be brought to compile
  // without making usage of the Synchronized classes "withWriteLock" method
  // unnecessarily hard.
  // static_assert(!AllowsNonConstExclusiveAccessLambda<const Synchronized<Vec,
  // std::shared_mutex>>::value);
  static_assert(AllowsConstExclusiveAccessLambda<
                const Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(AllowsConstSharedAccessLambda<
                const Synchronized<Vec, std::shared_mutex>>::value);
  static_assert(!AllowsNonConstSharedAccessLambda<
                const Synchronized<Vec, std::shared_mutex>>::value);

  static_assert(!AllowsNonConstExclusiveAccess<
                const Synchronized<Vec, std::mutex>>::value);
  static_assert(
      AllowsConstExclusiveAccess<const Synchronized<Vec, std::mutex>>::value);
  static_assert(
      !AllowsConstSharedAccess<const Synchronized<Vec, std::mutex>>::value);
  static_assert(
      !AllowsNonConstSharedAccess<const Synchronized<Vec, std::mutex>>::value);

  // Outcommenting this does not compile and cannot be brought to compile
  // without making usage of the Synchronized classes "withWriteLock" method
  // unnecessarily hard.
  // static_assert(!AllowsNonConstExclusiveAccessLambda<const Synchronized<Vec,
  // std::mutex>>::value);
  static_assert(AllowsConstExclusiveAccessLambda<
                const Synchronized<Vec, std::mutex>>::value);
  static_assert(!AllowsConstSharedAccessLambda<
                const Synchronized<Vec, std::mutex>>::value);
  static_assert(!AllowsNonConstSharedAccessLambda<
                const Synchronized<Vec, std::mutex>>::value);
}
