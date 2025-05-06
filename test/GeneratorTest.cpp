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
  using B = bool(void*);
  void* target;
  F* resumeFunc;
  F* destroyFunc;
  B* doneFunc;
};

template <typename Promise = void>
struct Handle {
  HandleFrame* ptr;
  void resume() { ptr->resumeFunc(ptr->target); }
  static Handle from_promise(Promise& p) {
    // TODO<joka921> That is not correct.
    auto ptr = reinterpret_cast<HandleFrame*>(reinterpret_cast<char*>(&p) -
                                              sizeof(HandleFrame));
    /*
    std::cerr << "Address of frame computed " << reinterpret_cast<intptr_t>(ptr)
              << std::endl;
              */
    return Handle{ptr};
  }

  operator bool() const { return static_cast<bool>(ptr); }

  bool done() const { return ptr->doneFunc(ptr->target); }

  // TODO<joka921> This has to take into account the alignment.
  Promise& promise() {
    return *reinterpret_cast<Promise*>(reinterpret_cast<char*>(ptr) +
                                       sizeof(HandleFrame));
  }
  const Promise& promise() const {
    return *reinterpret_cast<Promise*>(reinterpret_cast<char*>(ptr) +
                                       sizeof(HandleFrame));
  }

  // TODO<joka921> Don't leak money.
  void destroy() { ptr->destroyFunc(ptr->target); }
};

bool co_await_impl(auto&& awaiter, auto handle) {
  if (awaiter.await_ready()) {
    return true;
  }
  using type = decltype(awaiter.await_suspend(handle));
  static_assert(std::is_void_v<decltype(awaiter.await_resume())>);
  if constexpr (std::is_void_v<type>) {
    awaiter.await_suspend(handle);
    return false;
  } else if constexpr (std::same_as<type, bool>) {
    return !awaiter.await_suspend(handle);
  } else {
    static_assert(ad_utility::alwaysFalse<type>,
                  "await_suspend with symmetric transfer is not yet supported");
  }
}
#define CO_YIELD(value)                                   \
  {                                                       \
    auto&& awaiter = promise().yield_value(value);        \
    curState = __LINE__;                                  \
    if (!co_await_impl(awaiter, Hdl::from_promise(pt))) { \
      return;                                             \
    }                                                     \
  }                                                       \
  [[fallthrough]];                                        \
  case __LINE__:

template <typename Derived, typename PromiseType, typename... payloadVars>
struct GeneratorStateMachineMixin {
  HandleFrame frm;
  PromiseType pt;
  size_t curState = 0;
  std::tuple<payloadVars...> payLoad;
  using Hdl = Handle<PromiseType>;

  PromiseType& promise() { return pt; }

  static auto cast(void* blubb) {
    return static_cast<Derived*>(reinterpret_cast<GeneratorStateMachineMixin*>(
        reinterpret_cast<char*>(blubb) -
        offsetof(GeneratorStateMachineMixin, frm)));
  }

  static void resume(void* blubb) { cast(blubb)->doStep(); }

  // TODO<joka921> Allocator support.
  static void destroy(void* blubb) { delete (cast(blubb)); }
  static bool done([[maybe_unused]] void* blubb) {
    // TODO extend to more general things.
    return false;
  }

  GeneratorStateMachineMixin() {
    frm.target = this;
    frm.resumeFunc = &GeneratorStateMachineMixin::resume;
    frm.destroyFunc = &GeneratorStateMachineMixin::destroy;
    frm.doneFunc = &GeneratorStateMachineMixin::done;
    std::cerr << "Address of frame actually "
              << reinterpret_cast<intptr_t>(&frm) << std::endl;
  }

  static auto make() {
    // TODO allocator support
    auto* frame = new Derived;
    return frame->pt.get_return_object();
  }
};

cppcoro::generator<int, int, Handle> dummyGen() {
  using PromiseType = cppcoro::generator<int, int, Handle>::promise_type;
  struct GeneratorStateMachine
      : GeneratorStateMachineMixin<GeneratorStateMachine, PromiseType, int> {
    auto& payload() { return std::get<0>(payLoad); };
    void doStep() {
      switch (curState) {
        case 0:
          while (true) {
            payload()++;
            CO_YIELD(payload());
            payload() += 2;
            CO_YIELD(payload());
          }
      }
    }
  };
  return GeneratorStateMachine::make();
};

TEST(NewGenerator, Blubb) {
  std::vector<int> v;
  auto gen = dummyGen();
  for (auto i : gen | ql::views::take(5)) {
    v.push_back(i);
  }
  EXPECT_THAT(v, ::testing::ElementsAre(1, 3, 4, 6, 7));
}

// A simple allocator that logs all allocations and deallocations to stderr
template <typename T>
struct LoggingAllocator {
  using value_type = T;

  LoggingAllocator() = default;

  template <typename U>
  LoggingAllocator(const LoggingAllocator<U>&) {}

  T* allocate(size_t n) {
    auto p = static_cast<T*>(::operator new(n * sizeof(T)));
    std::cerr << "Allocating " << n << " elements of size " << sizeof(T)
              << " at " << static_cast<void*>(p) << std::endl;
    return p;
  }

  void deallocate(T* p, size_t n) {
    std::cerr << "Deallocating " << n << " elements at "
              << static_cast<void*>(p) << std::endl;
    ::operator delete(p);
  }

  template <typename U>
  bool operator==(const LoggingAllocator<U>&) const {
    return true;
  }
};
