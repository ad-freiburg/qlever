//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//   Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_GENERATORS_H
#define QLEVER_SRC_UTIL_GENERATORS_H

#include <condition_variable>
#include <mutex>
#include <optional>

#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

namespace ad_utility {

// Wrap the given `generator` inside another generator that aggregates a cache
// by calling `aggregator` on every iteration of the inner `generator` until it
// returns false. If the `aggregator` returns false, the cached value is
// discarded. If the cached value is still present once the generator is fully
// consumed, `onFullyCached` is called with the cached value.
CPP_template(typename InputRange, typename AggregatorT,
             typename T = ql::ranges::range_value_t<InputRange>,
             typename FullyCachedFuncT = int)(
    requires InvocableWithExactReturnType<AggregatorT, bool, std::optional<T>&,
                                          const T&>
        CPP_and InvocableWithExactReturnType<
            FullyCachedFuncT, void,
            T>) auto wrapGeneratorWithCache(InputRange generator,
                                            AggregatorT aggregator,
                                            FullyCachedFuncT onFullyCached) {
  struct CachingWrapper : public InputRangeFromGet<T> {
    InputRange generator_;
    AggregatorT aggregator_;
    FullyCachedFuncT onFullyCached_;
    ql::ranges::iterator_t<InputRange> it_;
    std::optional<T> aggregatedData_;
    bool shouldBeAggregated_ = true;
    bool initialized_ = false;

    CachingWrapper(InputRange generator, AggregatorT aggregator,
                   FullyCachedFuncT onFullyCached)
        : generator_(std::move(generator)),
          aggregator_(std::move(aggregator)),
          onFullyCached_(std::move(onFullyCached)) {}

    std::optional<T> get() override {
      if (!std::exchange(initialized_, true)) {
        it_ = generator_.begin();
      } else {
        ++it_;
      }

      if (it_ == generator_.end()) {
        if (aggregatedData_.has_value()) {
          AD_CORRECTNESS_CHECK(shouldBeAggregated_);
          onFullyCached_(std::move(aggregatedData_.value()));
        }
        return std::nullopt;
      }

      T element = std::move(*it_);

      if (shouldBeAggregated_) {
        shouldBeAggregated_ = aggregator_(aggregatedData_, element);
        if (!shouldBeAggregated_) {
          aggregatedData_.reset();
        }
      }

      return element;
    }
  };

  return InputRangeTypeErased<T>{std::make_unique<CachingWrapper>(
      std::move(generator), std::move(aggregator), std::move(onFullyCached))};
}

// Convert a callback-based `functionWithCallback` into a generator by spawning
// a thread that runs the functionWithCallback. The `functionWithCallback` is a
// callable that takes a `callback` with signature `void(T)`. The arguments with
// which this callback is called when running the `functionWithCallback` become
// the elements that are yielded by the created `generator`.
template <typename T, typename F>
InputRangeTypeErased<T> generatorFromActionWithCallback(F functionWithCallback)
    requires ql::concepts::invocable<F, std::function<void(T)>> {
  class CallbackToRangeAdapter : public InputRangeFromGet<T> {
    F functionWithCallback_;
    std::mutex mutex_;
    std::condition_variable cv_;
    // Only one of the threads can run at the same time. The semantics are
    // `Inner` -> The inner thread that runs the callback has to produce the
    // next value. `Outer` -> The outer thread that is the generator has to
    // yield the next value. `OuterIsFinished` -> The generator is finished and
    // will read no further values from the inner thread, the inner thread has
    // to stop.
    enum struct State { Inner, Outer, OuterIsFinished };
    State state_ = State::Inner;
    // Used to pass the produced values from the inner thread to the generator.
    // `monostate` means that the inner thread is finished and there will be no
    // further values. `exception_ptr` means that the inner thread has
    // encountered an exception, which is then rethrown by the outer generator.
    std::variant<std::monostate, T, std::exception_ptr> storage_;

    // IMPORTANT NOTE: It is crucial that this `thread_` is the last data member
    // of this class, as it references several of the above members, and thus
    // has to be joined/destroyed before these other members are destroyed!!!
    ad_utility::JThread thread_;

   public:
    explicit CallbackToRangeAdapter(F functionWithCallback)
        : functionWithCallback_(std::move(functionWithCallback)) {}

    std::optional<T> get() override {
      std::unique_lock lock(mutex_);

      if (!thread_.joinable()) {
        lock.unlock();
        startThread();
      } else {
        // Signal the inner thread to produce the next value.
        state_ = State::Inner;
        lock.unlock();
        cv_.notify_one();
      }

      // Wait for the inner thread to produce a value.
      lock.lock();
      cv_.wait(lock, [this] { return state_ != State::Inner; });

      if (state_ == State::OuterIsFinished) return std::nullopt;

      return std::visit(
          [](auto& value) -> std::optional<T> {
            using ValueType = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<ValueType, std::monostate>) {
              return std::nullopt;
            } else if constexpr (std::is_same_v<ValueType,
                                                std::exception_ptr>) {
              std::rethrow_exception(value);
            } else {
              return std::move(value);
            }
          },
          storage_);
    }

    ~CallbackToRangeAdapter() {
      {
        std::unique_lock lock(mutex_);
        state_ = State::OuterIsFinished;
      }
      cv_.notify_one();
    }

   private:
    void startThread() {
      // Set up the inner thread.
      thread_ = ad_utility::JThread{[this]() {
        struct FinishedBecauseOuterIsFinishedException {};

        std::unique_lock lock(mutex_);
        AD_CORRECTNESS_CHECK(state_ == State::Inner);

        // Wait for the outer thread to consume the last value and return
        // control to the inner thread.
        auto wait = [this, &lock]() {
          cv_.wait(lock, [this] { return state_ != State::Outer; });
          if (state_ == State::OuterIsFinished) {
            throw FinishedBecauseOuterIsFinishedException{};
          }
        };

        // Write the `value` to the `storage` and notify the outer thread that
        // it has to consume it.
        auto writeValue = [this, &lock](auto value) noexcept {
          storage_ = std::move(value);
          state_ = State::Outer;
          lock.unlock();
          cv_.notify_one();
          lock.lock();
        };

        // Write the `value`, pass control to the outer thread and wait till it
        // is our turn again.
        auto writeValueAndWait = [&](T value) {
          AD_CORRECTNESS_CHECK(state_ == State::Inner);
          writeValue(std::move(value));
          wait();
        };

        try {
          // `writeValueAndWait` is the callback that is repeatedly called by
          // the `functionWithCallback`.
          functionWithCallback_(writeValueAndWait);

          // The function has completed, notify the outer thread and finish.
          writeValue(std::monostate{});
        } catch (const FinishedBecauseOuterIsFinishedException&) {
          // This means that the outer thread is completely finished and only
          // waits for the inner thread to join.
          return;
        } catch (...) {
          // The function has created an exception, pass it to the outer thread.
          writeValue(std::current_exception());
        }
      }};
    }
  };

  return InputRangeTypeErased{std::make_unique<CallbackToRangeAdapter>(
      std::move(functionWithCallback))};
}
};  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GENERATORS_H
