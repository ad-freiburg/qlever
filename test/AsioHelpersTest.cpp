//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>

#include "util/AsioHelpers.h"
#include "util/jthread.h"

namespace net = boost::asio;
using namespace net::experimental::awaitable_operators;

using ad_utility::resumeOnOriginalExecutor;
using namespace boost::asio::experimental::awaitable_operators;

TEST(AsioHelpers, cancellationOnOtherStrand) {
  struct Context {
    net::io_context ctx_;
    using Strand = decltype(net::make_strand(ctx_));
    Strand strand1_ = net::make_strand(ctx_);
    Strand strand2_ = net::make_strand(ctx_);
    net::deadline_timer infiniteTimer1_{
        strand1_, static_cast<net::deadline_timer::time_type>(
                      boost::posix_time::pos_infin)};
    net::deadline_timer infiniteTimer2_{
        strand2_, static_cast<net::deadline_timer::time_type>(
                      boost::posix_time::pos_infin)};

    int x_ = 0;
    std::atomic_flag done_{false};
  };

  Context ctx;

  static constexpr size_t numValues = 1'000;
  auto increment = [](Context& ctx) -> net::awaitable<void> {
    co_await ad_utility::runOnStrand(ctx.strand1_, net::deferred);
    auto result = ++ctx.x_;
    if (result == numValues) {
      ctx.done_.test_and_set();
      ctx.done_.notify_all();
      co_await ctx.infiniteTimer1_.async_wait(net::use_awaitable);
    } else {
      co_await ad_utility::runOnStrand(ctx.strand2_, net::deferred);
      co_await ctx.infiniteTimer2_.async_wait(net::deferred);
    }
  };

  using Op = decltype(net::co_spawn(ctx.ctx_, increment(ctx), net::deferred));
  std::vector<Op> ops;
  for (size_t i = 0; i < numValues; ++i) {
    ops.push_back(net::co_spawn(ctx.ctx_, increment(ctx), net::deferred));
  }
  auto group = net::experimental::make_parallel_group(std::move(ops));
  auto awaitAll = [](auto& group) -> net::awaitable<void> {
    try {
      [[maybe_unused]] auto x = group.async_wait(
          net::experimental::wait_for_one_error(), net::use_future);
    } catch (...) {
      LOG(INFO) << "Caught something" << std::endl;
      throw;
    }
    co_return;
  };

  auto future = net::co_spawn(ctx.strand1_, awaitAll(group), net::use_future);
  std::vector<ad_utility::JThread> threads;
  for (size_t i = 0; i < 20; ++i) {
    threads.emplace_back([&] { ctx.ctx_.run(); });
  }
  ctx.done_.wait(false);
    net::dispatch(ctx.strand1_, std::packaged_task<void()>{
            [&]() { ctx.infiniteTimer1_.cancel(); }}).wait();
    /*
    net::dispatch(ctx.strand2_, std::packaged_task<void()>{
            [&]() { ctx.infiniteTimer2_.cancel(); }}).wait();
            */
  future.get();
  threads.clear();
  ASSERT_EQ(ctx.x_, numValues);
}


TEST(AsioHelpers, raceConditionCancellation) {
    static constexpr size_t numValues = 10'000;
    struct Context {
        net::io_context ctx_;
        using Strand = decltype(net::make_strand(ctx_));
        std::vector<Strand> strands_;
        Strand strand1_ = net::make_strand(ctx_);
        Strand strand2_ = net::make_strand(ctx_);
        net::deadline_timer infiniteTimer1_{
                strand1_, static_cast<net::deadline_timer::time_type>(
                        boost::posix_time::pos_infin)};
        net::deadline_timer infiniteTimer2_{
                strand2_, static_cast<net::deadline_timer::time_type>(
                        boost::posix_time::pos_infin)};

        int x_ = 0;
        std::atomic_flag done_{false};
        Context() {
          for (size_t i = 0; i < numValues; ++i) {
            strands_.push_back(net::make_strand(ctx_));
          }
        }
    };

    Context ctx;

    static constexpr auto dummy = []() -> net::awaitable<void> {
      auto exec = co_await net::this_coro::executor;
      net::steady_timer t{exec, std::chrono::microseconds(1)};
      co_await t.async_wait(net::deferred);
      co_return;
    };

    auto increment = [](Context& ctx, size_t num) -> net::awaitable<void> {
      EXPECT_TRUE(ctx.strands_.at(num).running_in_this_thread());
      auto other = num == 0 ? 1 : num - 1;
      EXPECT_FALSE(ctx.strands_.at(other).running_in_this_thread());
      EXPECT_FALSE(ctx.strand2_.running_in_this_thread());
      EXPECT_FALSE(ctx.strand1_.running_in_this_thread());
      if (num == 0) {
        try {
          co_await ctx.infiniteTimer1_.async_wait(net::deferred);
          throw std::runtime_error{"cancelling this"};
        } catch (...) {
          LOG(INFO) << "Caught cancellation signal" << std::endl;
          throw;
        }
      }
      while (true) {
        co_await dummy();
        EXPECT_TRUE(ctx.strands_.at(num).running_in_this_thread());
        auto other = num == 0 ? 1 : num - 1;
        EXPECT_FALSE(ctx.strands_.at(other).running_in_this_thread());
        EXPECT_FALSE(ctx.strand2_.running_in_this_thread());
        EXPECT_FALSE(ctx.strand1_.running_in_this_thread());
      }
    };

    using Op = decltype(ad_utility::runAwaitableOnStrand(ctx.strands_.at(0), increment(ctx, 0), net::deferred));
    //using Op = net::awaitable<void>;
    std::vector<Op> ops;
    for (size_t i = 0; i < numValues; ++i) {
        ops.push_back(ad_utility::runAwaitableOnStrand(ctx.strands_.at(i), increment(ctx, i), net::deferred));
        //ops.push_back(ad_utility::runAwaitableOnStrandAwaitable(ctx.strands_.at(i), increment(ctx, i)));
    }
    auto group = net::experimental::make_parallel_group(std::move(ops));
    auto awaitAll = [](auto& group) -> net::awaitable<void> {
        try {
            [[maybe_unused]] auto x = group.async_wait(
                    net::experimental::wait_for_one_error(), net::use_future);
        } catch (...) {
            LOG(INFO) << "Caught something" << std::endl;
            throw;
        }
        co_return;
    };

    auto future = net::co_spawn(ctx.strand2_, awaitAll(group), net::use_future);
    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < 30; ++i) {
        threads.emplace_back([&] { ctx.ctx_.run(); });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    net::dispatch(ctx.strand1_, std::packaged_task<void()>{
            [&]() { ctx.infiniteTimer1_.cancel(); }}).wait();
    LOG(INFO) << "Cancelled the main thread" << std::endl;
    future.get();
    //EXPECT_GT(ctx.x_, 0);
    threads.clear();
}

TEST(AsioHelpers, resumeOnOriginalExecutor) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand1,
                         strand2]() -> net::awaitable<int> {
    co_await net::post(net::bind_executor(strand2, net::use_awaitable));
    // sanity check
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    co_return 1337;
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1,
                         strand2]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    auto value = co_await resumeOnOriginalExecutor(innerAwaitable());
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_EQ(value, 1337);
    sanityCounter++;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

TEST(AsioHelpers, resumeOnOriginalExecutorVoidOverload) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  bool sanityFlag = false;

  auto outerAwaitable = [&sanityFlag, strand1,
                         strand2]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    co_await resumeOnOriginalExecutor(
        net::post(net::bind_executor(strand2, net::use_awaitable)));
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    sanityFlag = true;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_TRUE(sanityFlag);
}

// _____________________________________________________________________________

TEST(AsioHelpers, resumeOnOriginalExecutorWhenException) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand1,
                         strand2]() -> net::awaitable<int> {
    co_await net::post(net::bind_executor(strand2, net::use_awaitable));
    // sanity check
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    throw std::runtime_error{"Expected"};
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1,
                         strand2]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_THROW(co_await resumeOnOriginalExecutor(innerAwaitable()),
                 std::runtime_error);
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

TEST(AsioHelpers, resumeOnOriginalExecutorVoidOverloadWhenException) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand1,
                         strand2]() -> net::awaitable<void> {
    co_await net::post(net::bind_executor(strand2, net::use_awaitable));
    // sanity check
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    throw std::runtime_error{"Expected"};
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1,
                         strand2]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_THROW(co_await resumeOnOriginalExecutor(innerAwaitable()),
                 std::runtime_error);
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

template <typename T, typename Rep, typename Period>
net::awaitable<T> cancelAfter(net::awaitable<T> coroutine,
                              std::chrono::duration<Rep, Period> duration) {
  net::steady_timer timer{co_await net::this_coro::executor, duration};
  co_await (std::move(coroutine) || timer.async_wait(net::use_awaitable));
}
// _____________________________________________________________________________

// Checks that behavior is consistent for cancellation case
TEST(AsioHelpers, resumeOnOriginalExecutorWhenCancelled) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);
  auto strand3 = net::make_strand(ioContext);
  net::deadline_timer infiniteTimer{ioContext,
                                    static_cast<net::deadline_timer::time_type>(
                                        boost::posix_time::pos_infin)};

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand1, strand2,
                         &infiniteTimer]() -> net::awaitable<void> {
    co_await net::post(net::bind_executor(strand2, net::use_awaitable));
    // sanity check
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    co_await infiniteTimer.async_wait(net::use_awaitable);
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1, strand2,
                         strand3]() -> net::awaitable<void> {
    co_await net::post(net::bind_executor(strand1, net::use_awaitable));
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_FALSE(strand3.running_in_this_thread());
    EXPECT_THROW(co_await resumeOnOriginalExecutor(innerAwaitable()),
                 boost::system::system_error);
    // Verify we're on the strand the cancellation happened
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_TRUE(strand3.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand3,
                cancelAfter(outerAwaitable(), std::chrono::milliseconds(10)),
                net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

// Checks that behavior is consistent for cancellation case
TEST(AsioHelpers, resumeOnOriginalExecutorVoidOverloadWhenCancelled) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);
  auto strand3 = net::make_strand(ioContext);
  net::deadline_timer infiniteTimer{ioContext,
                                    static_cast<net::deadline_timer::time_type>(
                                        boost::posix_time::pos_infin)};

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand1, strand2, strand3,
                         &infiniteTimer]() -> net::awaitable<void> {
    co_await net::post(net::bind_executor(strand2, net::use_awaitable));
    // sanity check
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_TRUE(strand2.running_in_this_thread());
    EXPECT_FALSE(strand3.running_in_this_thread());
    sanityCounter++;
    co_await infiniteTimer.async_wait(net::use_awaitable);
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1, strand2,
                         strand3]() -> net::awaitable<void> {
    co_await net::post(net::bind_executor(strand1, net::use_awaitable));
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_FALSE(strand3.running_in_this_thread());
    EXPECT_THROW(co_await resumeOnOriginalExecutor(innerAwaitable()),
                 boost::system::system_error);
    // Verify we're on the strand the cancellation happened
    EXPECT_FALSE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    EXPECT_TRUE(strand3.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand3,
                cancelAfter(outerAwaitable(), std::chrono::milliseconds(10)),
                net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}
