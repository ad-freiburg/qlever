///////////////////////////////////////////////////////////////////////////////
// Copyright (c) Lewis Baker, Johannes Kalmbach (functionality to add details).
// Licenced under MIT license. See LICENSE.txt for details.
///////////////////////////////////////////////////////////////////////////////
#ifndef CPPCORO_GENERATOR_HPP_INCLUDED
#define CPPCORO_GENERATOR_HPP_INCLUDED

#include <coroutine>
#include <exception>
#include <functional>
#include <utility>

#include "Iterators.h"
#include "backports/algorithm.h"
#include "backports/iterator.h"
#include "backports/type_traits.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace cppcoro {

struct SuspendAlways {
  constexpr bool await_ready() const noexcept { return false; }
  constexpr void await_suspend([[maybe_unused]] auto handle) const noexcept {}
  constexpr void await_resume() const noexcept {}
};
// This struct can be `co_await`ed inside a `generator` to obtain a reference to
// the details object (the value of which is a template parameter to the
// generator). For an example see `GeneratorTest.cpp`.
struct GetDetails {};
static constexpr GetDetails getDetails;

// This struct is used as the default of the details object for the case that
// there are no details
struct NoDetails {};

template <typename T, typename Details = NoDetails,
          template <typename...> typename GeneratorHandle =
              std::coroutine_handle>
class generator;

namespace detail {
template <typename T, typename Details = NoDetails,
          template <typename...> typename GeneratorHandle =
              std::coroutine_handle>
class generator_promise {
 public:
  // Even if the generator only yields `const` values, the `value_type`
  // shouldn't be `const` because otherwise several static checks when
  // interacting with the STL fail.
  using value_type = ql::remove_cvref_t<T>;
  using reference_type = std::conditional_t<std::is_reference_v<T>, T, T&>;
  using pointer_type = std::remove_reference_t<T>*;

  generator_promise() = default;

  generator<T, Details, GeneratorHandle> get_return_object() noexcept;

  constexpr cppcoro::SuspendAlways initial_suspend() const noexcept {
    return {};
  }
  constexpr cppcoro::SuspendAlways final_suspend() const noexcept { return {}; }

  template <typename U = T,
            std::enable_if_t<!std::is_rvalue_reference<U>::value, int> = 0>
  cppcoro::SuspendAlways yield_value(
      std::remove_reference_t<T>& value) noexcept {
    m_value = std::addressof(value);
    return {};
  }

  cppcoro::SuspendAlways yield_value(
      std::remove_reference_t<T>&& value) noexcept {
    m_value = std::addressof(value);
    return {};
  }

  void unhandled_exception() { m_exception = std::current_exception(); }

  void return_void() {}

  reference_type value() const noexcept {
    return static_cast<reference_type>(*m_value);
  }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  CPP_template_2(typename U)(
      requires CPP_NOT(ad_utility::SimilarTo<GetDetails, U>)) std::suspend_never
      await_transform(U&& value) = delete;

  void rethrow_if_exception() const {
    if (m_exception) {
      std::rethrow_exception(m_exception);
    }
  }

  // The machinery to expose the stored `Details` via
  // `co_await cppcoro::getDetails`.
  struct DetailAwaiter {
    generator_promise& promise_;
    bool await_ready() const { return true; }
    bool await_suspend(GeneratorHandle<>) const noexcept { return false; }
    Details& await_resume() noexcept { return promise_.details(); }
  };

  CPP_template(typename DetailT)(
      requires ad_utility::SimilarTo<GetDetails, DetailT>) DetailAwaiter
      await_transform([[maybe_unused]] DetailT&& detail) {
    return {*this};
  }

  static constexpr bool hasDetails = !std::is_same_v<Details, NoDetails>;
  Details& details() requires hasDetails {
    return std::holds_alternative<Details>(m_details)
               ? std::get<Details>(m_details)
               : *std::get<Details*>(m_details);
  }

  void setDetailsPointer(Details* pointer) requires hasDetails {
    AD_CONTRACT_CHECK(pointer != nullptr);
    m_details = pointer;
  }

 private:
  pointer_type m_value;
  std::exception_ptr m_exception;

  using DetailStorage =
      std::conditional_t<hasDetails, std::variant<Details, Details*>, Details>;
  // If the `Details` type is empty, we don't need it to occupy any space.
  [[no_unique_address]] DetailStorage m_details{};
};

struct generator_sentinel {};

template <typename T, typename Details,
          template <typename...> typename GeneratorHandle,
          bool ConstDummy = false>
class generator_iterator {
  using promise_type = generator_promise<T, Details, GeneratorHandle>;
  using coroutine_handle = GeneratorHandle<promise_type>;

 public:
  using iterator_category = std::input_iterator_tag;
  // What type should we use for counting elements of a potentially infinite
  // sequence?
  using difference_type = std::ptrdiff_t;
  using value_type = typename promise_type::value_type;
  using reference = typename promise_type::reference_type;
  using pointer = typename promise_type::pointer_type;

  // Iterator needs to be default-constructible to satisfy the Range concept.
  generator_iterator() noexcept : m_coroutine(nullptr) {}

  explicit generator_iterator(coroutine_handle coroutine) noexcept
      : m_coroutine(coroutine) {}

  friend bool operator==(const generator_iterator& it,
                         generator_sentinel) noexcept {
    return !it.m_coroutine || it.m_coroutine.done();
  }

  friend bool operator!=(const generator_iterator& it,
                         generator_sentinel s) noexcept {
    return !(it == s);
  }

  friend bool operator==(generator_sentinel s,
                         const generator_iterator& it) noexcept {
    return (it == s);
  }

  friend bool operator!=(generator_sentinel s,
                         const generator_iterator& it) noexcept {
    return it != s;
  }

  generator_iterator& operator++() {
    m_coroutine.resume();
    if (m_coroutine.done()) {
      m_coroutine.promise().rethrow_if_exception();
    }

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  void operator++(int) { (void)operator++(); }

  reference operator*() const noexcept { return m_coroutine.promise().value(); }

  pointer operator->() const noexcept { return std::addressof(operator*()); }

 private:
  coroutine_handle m_coroutine;
};
}  // namespace detail

template <typename T, typename Details,
          template <typename...> typename GeneratorHandle>
class [[nodiscard]] generator {
 public:
  using promise_type = detail::generator_promise<T, Details, GeneratorHandle>;
  using iterator =
      detail::generator_iterator<T, Details, GeneratorHandle, false>;
  // TODO<joka921> Check if this fixes anything wrt ::ranges
  // using const_iterator = detail::generator_iterator<T, Details, true>;
  using value_type = typename iterator::value_type;

  generator() noexcept : m_coroutine(nullptr) {}

  generator(generator&& other) noexcept : m_coroutine(other.m_coroutine) {
    other.m_coroutine = nullptr;
  }

  generator(const generator& other) = delete;

  ~generator() {
    if (m_coroutine) {
      m_coroutine.destroy();
    }
  }

  generator& operator=(generator other) noexcept {
    swap(other);
    return *this;
  }

  iterator begin() {
    if (m_coroutine) {
      m_coroutine.resume();
      if (m_coroutine.done()) {
        m_coroutine.promise().rethrow_if_exception();
      }
    }

    return iterator{m_coroutine};
  }

  /*
  iterator begin() const;
  detail::generator_sentinel end() const;
  */

  detail::generator_sentinel end() noexcept {
    return detail::generator_sentinel{};
  }

  /*
  // Not defined and not useful, but required for range-v3
  const_iterator begin() const;
  const_iterator end() const;
  */

  void swap(generator& other) noexcept {
    std::swap(m_coroutine, other.m_coroutine);
  }

  const Details& details() const {
    return m_coroutine ? m_coroutine.promise().details()
                       : m_details_if_default_constructed;
  }
  Details& details() {
    return m_coroutine ? m_coroutine.promise().details()
                       : m_details_if_default_constructed;
  }

  void setDetailsPointer(Details* pointer) {
    m_coroutine.promise().setDetailsPointer(pointer);
  }

 private:
  friend class detail::generator_promise<T, Details, GeneratorHandle>;

  // In the case of an empty, default-constructed `generator` object we still
  // want the call to `details` to return a valid object that in this case is
  // owned directly by the generator itself.
  [[no_unique_address]] Details m_details_if_default_constructed;

  explicit generator(GeneratorHandle<promise_type> coroutine) noexcept
      : m_coroutine(coroutine) {}

  GeneratorHandle<promise_type> m_coroutine;
};

template <typename T, typename D, template <typename...> typename H>
void swap(generator<T, D, H>& a, generator<T, D, H>& b) {
  a.swap(b);
}

namespace detail {
template <typename T, typename Details, template <typename...> typename H>
generator<T, Details, H>
generator_promise<T, Details, H>::get_return_object() noexcept {
  using coroutine_handle = H<generator_promise<T, Details, H>>;
  return generator<T, Details, H>{coroutine_handle::from_promise(*this)};
}
}  // namespace detail

template <typename FUNC, typename T, typename D,
          template <typename...> typename H>
generator<std::invoke_result_t<
              FUNC&, typename generator<T, D, H>::iterator::reference>,
          D, H>
fmap(FUNC func, generator<T, D, H> source) {
  for (auto&& value : source) {
    co_yield std::invoke(func, static_cast<decltype(value)>(value));
  }
}

// Get the first element of a generator and verify that it's the only one.
template <typename T, typename Details, template <typename...> typename H>
T getSingleElement(generator<T, Details, H> g) {
  auto it = g.begin();
  AD_CORRECTNESS_CHECK(it != g.end());
  T t = std::move(*it);
  AD_CORRECTNESS_CHECK(++it == g.end());
  return t;
}

// helper function to convert ad_utility::InputRangeTypeErased<T> to
// cppcoro::generator<T> with no details
template <typename T>
generator<T> fromInputRange(ad_utility::InputRangeTypeErased<T> range) {
  for (auto& value : range) {
    co_yield value;
  }
}
}  // namespace cppcoro

// ____________________________________________________________________________
// Infrastructure for coroutines in C++17
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
    // TODO<joka921> This has to take into account the alignment.
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

  void destroy() { ptr->destroyFunc(ptr->target); }
};

template <typename T>
inline constexpr bool alwaysFalse = false;

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
    static_assert(alwaysFalse<type>,
                  "await_suspend with symmetric transfer is not yet supported");
  }
}

#define CO_YIELD(index, value)                            \
  {                                                       \
    auto&& awaiter = promise().yield_value(value);        \
    this->curState = index;                               \
    if (!co_await_impl(awaiter, Hdl::from_promise(pt))) { \
      return;                                             \
    }                                                     \
  }                                                       \
  [[fallthrough]];                                        \
  case index:

namespace blubbi {
template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;
}

#define CO_YIELD_BUFFERED(index, value)                                        \
  {                                                                            \
    using BufT##index = blubbi::remove_cvref_t<decltype(value)>;               \
    using PtrT##index =                                                        \
        std::add_pointer_t<std::remove_reference_t<decltype(value)>>;          \
    new (state.yieldBuffer) std::remove_reference_t<decltype(value)>{value};   \
    CO_YIELD(index, static_cast<std::add_rvalue_reference_t<decltype(value)>>( \
                        *reinterpret_cast<PtrT##index>(state.yieldBuffer)))    \
    reinterpret_cast<PtrT##index>(state.yieldBuffer)->~BufT##index();          \
  }                                                                            \
  void()

template <typename Derived, typename PromiseType>
struct CoroImpl {
  HandleFrame frm;
  PromiseType pt;

  static void CHECK() {
    static_assert(offsetof(CoroImpl, pt) - offsetof(CoroImpl, frm) ==
                  sizeof(HandleFrame));
  }

  size_t curState = 0;
  using Hdl = Handle<PromiseType>;

  PromiseType& promise() { return pt; }

  static auto cast(void* blubb) {
    return static_cast<Derived*>(reinterpret_cast<CoroImpl*>(
        reinterpret_cast<char*>(blubb) - offsetof(CoroImpl, frm)));
  }

  static void resume(void* blubb) { cast(blubb)->doStep(); }

  // TODO<joka921> Allocator support.
  static void destroy(void* blubb) { delete (cast(blubb)); }

  static bool done([[maybe_unused]] void* blubb) {
    // TODO extend to more general things.
    return false;
  }

  CoroImpl() {
    CHECK();
    frm.target = this;
    frm.resumeFunc = &CoroImpl::resume;
    frm.destroyFunc = &CoroImpl::destroy;
    frm.doneFunc = &CoroImpl::done;
  }

  static auto make() {
    // TODO allocator support
    auto* frame = new Derived;
    return frame->pt.get_return_object();
  }
};

#define COROUTINE_HEADER(returnType, StateType)        \
  using PromiseType = returnType::promise_type;        \
  struct GeneratorStateMachine                         \
      : CoroImpl<GeneratorStateMachine, PromiseType> { \
    StateType state;                                   \
    void doStep() {                                    \
      switch (this->curState) {                        \
        case 0:
#define COROUTINE_FOOTER(...)                                 \
  }                                                           \
  }                                                           \
  }                                                           \
  ;                                                           \
  auto* frame = new GeneratorStateMachine{{}, {__VA_ARGS__}}; \
  return frame->pt.get_return_object();

#define FOR_LOOP_HEADER(N)

template <typename Ref, bool isOwningStorage>
struct _coro_storage {
  static constexpr bool isOwning = isOwningStorage;
  using Storage = std::conditional_t<isOwning, std::decay_t<Ref>,
                                     std::add_pointer_t<std::decay_t<Ref>>>;
  alignas(Storage) char buffer[sizeof(Storage)];
  bool constructed = false;

  struct Val {
    Ref ref_;
  };

  template <typename... Args>
  void construct(Args&&... args) {
    if constexpr (!isOwning) {
      new (buffer) Storage(&args...);
    } else {
      new (buffer) Storage(std::forward<Args>(args)...);
    }
    constructed = true;
  }

  void destroy() {
    if (constructed) {
      reinterpret_cast<Storage*>(buffer)->~Storage();
      constructed = false;
    }
  }

  Val get() {
    Storage& storage = *reinterpret_cast<Storage*>(buffer);
    if constexpr (isOwning) {
      return Val{static_cast<Ref>(storage)};
    } else {
      return Val{static_cast<Ref>(*storage)};
    }
  }

  ~_coro_storage() { destroy(); }
};

#define CO_BRACED_INIT(mem, ...)                                   \
  new (this->state.mem.buffer) decltype(this->state.mem)::Storage{ \
      &__VA_ARGS__};                                               \
  this->state.mem.constructed = true
#define CO_BRACED_INIT_OWNING(mem, ...)                            \
  new (this->state.mem.buffer) decltype(this->state.mem)::Storage{ \
      __VA_ARGS__};                                                \
  this->state.mem.constructed = true
#define CO_PAREN_INIT(mem, ...)                                    \
  new (this->state.mem.buffer) decltype(this->state.mem)::Storage{ \
      &__VA_ARGS__};                                               \
  this->state.mem.constructed = true
#define CO_PAREN_INIT_OWNING(mem, ...)                             \
  new (this->state.mem.buffer) decltype(this->state.mem)::Storage( \
      __VA_ARGS__);                                                \
  this->state.mem.constructed = true

template <typename Ref, bool isOwning>
struct coro_for_loop_storage {
  _coro_storage<Ref, isOwning> range_;
  // TODO<joka921> This doesn't work for nonmember begin and end, but that
  // should work for most cases now.
  using Begin = decltype(std::declval<Ref>().begin());
  using End = decltype(std::declval<Ref>().end());
  _coro_storage<Begin&, true> begin_;
  _coro_storage<End&, true> end_;
};

#define CO_GET(arg) this->state.arg.get().ref_

#endif
