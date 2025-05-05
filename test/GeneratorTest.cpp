//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "util/Generator.h"

struct Details {
  bool begin_ = false;
  bool end_ = false;
  bool operator==(const Details&) const = default;
};

// A simple generator that first yields three numbers and then adds a detail
// value, that we can then extract after iterating over it.
cppcoro::generator<int, Details> simpleGen() {
  Details& details = co_await cppcoro::getDetails;
  details.begin_ = true;
  co_yield 1;
  co_yield 42;
  co_yield 43;
  details.end_ = true;
};

// Test the behavior of the `simpleGen` above.
TEST(Generator, details) {
  auto gen = simpleGen();
  int result{};
  // `details().begin_` is only set to true after the call to `begin()` in the
  // for loop below
  ASSERT_FALSE(gen.details().begin_);
  ASSERT_FALSE(gen.details().end_);
  for (int i : gen) {
    result += i;
    ASSERT_TRUE(gen.details().begin_);
    ASSERT_FALSE(gen.details().end_);
  }
  ASSERT_EQ(result, 86);
  ASSERT_TRUE(gen.details().begin_);
  ASSERT_TRUE(gen.details().end_);
}

// Test the behavior of the `simpleGen` with an explicit external details
// object.
TEST(Generator, externalDetails) {
  auto gen = simpleGen();
  Details details{};
  ASSERT_NE(&details, &gen.details());
  gen.setDetailsPointer(&details);
  ASSERT_EQ(&details, &gen.details());
  int result{};
  // `details().begin_` is only set to true after the call to `begin()` in the
  // for loop below
  ASSERT_FALSE(gen.details().begin_);
  ASSERT_FALSE(details.begin_);
  ASSERT_FALSE(gen.details().end_);
  ASSERT_FALSE(details.end_);
  for (int i : gen) {
    result += i;
    ASSERT_TRUE(gen.details().begin_);
    ASSERT_TRUE(details.begin_);
    ASSERT_FALSE(gen.details().end_);
    ASSERT_FALSE(details.end_);
  }
  ASSERT_EQ(result, 86);
  ASSERT_TRUE(gen.details().begin_);
  ASSERT_TRUE(details.begin_);
  ASSERT_TRUE(gen.details().end_);
  ASSERT_TRUE(details.end_);
  ASSERT_EQ(&details, &gen.details());

  // Setting a `nullptr` is illegal
  ASSERT_ANY_THROW(gen.setDetailsPointer(nullptr));
}

// Test that a default-constructed generator still has a valid `Details` object.
TEST(Generator, detailsForDefaultConstructedGenerator) {
  cppcoro::generator<int, Details> gen;
  ASSERT_EQ(gen.details(), Details());
  ASSERT_EQ(std::as_const(gen).details(), Details());
}

TEST(Generator, getSingleElement) {
  // The generator yields more than a single element -> throw
  EXPECT_ANY_THROW(cppcoro::getSingleElement(simpleGen()));

  // The generator yields exactly one element -> return the element
  auto gen2 = []() -> cppcoro::generator<int> { co_yield 1; }();
  EXPECT_EQ(1, cppcoro::getSingleElement(std::move(gen2)));
  auto gen3 = []() -> cppcoro::generator<int> {
    co_yield 1;
    co_yield 3;
  }();
  EXPECT_ANY_THROW(cppcoro::getSingleElement(std::move(gen3)));
}

struct HandleFrame {
  using F = void(void*);
  void* target;
  F* resumeFunc;
};

template <typename Promise = void>
struct Handle {
  HandleFrame* ptr;
  void resume() { ptr->resumeFunc(ptr->target); }
  static Handle from_promise(Promise& p) {
    // TODO<joka921> That is not correct.
    auto ptr = reinterpret_cast<HandleFrame*>(reinterpret_cast<char*>(&p) -
                                              sizeof(HandleFrame));
    std::cerr << "Address of frame computed " << reinterpret_cast<intptr_t>(ptr)
              << std::endl;
    return Handle{ptr};
  }

  operator bool() { return static_cast<bool>(ptr); }

  // TODO
  bool done() const { return false; }

  Promise& promise() {
    return *reinterpret_cast<Promise*>(reinterpret_cast<char*>(ptr) +
                                       sizeof(HandleFrame));
  }
  const Promise& promise() const {
    return *reinterpret_cast<Promise*>(reinterpret_cast<char*>(ptr) +
                                       sizeof(HandleFrame));
  }

  // TODO<joka921> Don't leak money.
  void destroy() { return; }
};

/*
template<>
struct Handle<void> {
  void* ptr;
};
 */

using PromiseType = cppcoro::generator<int, int, Handle>::promise_type;
struct GeneratorStateMachine {
  size_t curState = 0;
  int payLoad = 0;
  HandleFrame frm;
  PromiseType pt;

  static void resume(void* blubb) {
    static_cast<GeneratorStateMachine*>(blubb)->doStep();
  }

  GeneratorStateMachine() {
    frm.target = this;
    frm.resumeFunc = &GeneratorStateMachine::resume;
    std::cerr << "Address of frame actually "
              << reinterpret_cast<intptr_t>(&frm) << std::endl;
  }

  void doStep() {
    switch (curState) {
      case 0:
        payLoad++;
        pt.yield_value(payLoad);
        curState = 1;
        return;
      case 1:
        payLoad += 2;
        pt.yield_value(payLoad);
        curState = 0;
        return;
    }
  }
};

struct DummyGen {
  GeneratorStateMachine ptr;
  decltype(ptr.pt.get_return_object()) pt = ptr.pt.get_return_object();
};

TEST(NewGenerator, Blubb) {
  std::vector<int> v;
  DummyGen gen;
  auto it = gen.pt.begin();
  for (size_t i = 0; i < 5; ++i) {
    v.push_back(*it);
    std::cerr << "got value " << *it << std::endl;
    ++it;
  }
  EXPECT_THAT(v, ::testing::ElementsAre(0, 1, 0, 3));
}
