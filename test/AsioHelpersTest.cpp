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

  static constexpr size_t numValues = 5'000;
  /*
  auto increment = [](Context& ctx) -> net::awaitable<void> {
    co_await net::post(ad_utility::ChangeStrandToken{ctx.strand1_, net::use_awaitable});
    //auto token = ad_utility::ChangeStrandToken{ctx.strand1_, net::use_awaitable});
    co_await ad_utility::changeStrand(ctx.strand1_, net::use_awaitable);
    auto result = ++ctx.x_;
    if (result == numValues) {
      ctx.done_.test_and_set();
      ctx.done_.notify_all();
      throw std::runtime_error("I am done here");
      //   co_await ctx.infiniteTimer1_.async_wait(net::use_awaitable);
    } else {
      // co_await ad_utility::changeStrand(ctx.strand2_, net::use_awaitable);
      co_await ctx.infiniteTimer1_.async_wait(net::use_awaitable);
    }
  };
   */

    auto increment = [](Context& ctx) -> net::awaitable<void> {
        ctx.done_.test_and_set();
        ctx.done_.notify_all();
        co_await net::post(ad_utility::ChangeStrandToken{ctx.strand1_, net::use_awaitable});
        throw std::runtime_error("I am done here");
    };

  using Op = decltype(net::co_spawn(ctx.ctx_, increment(ctx),
                                   net::deferred));
  std::vector<Op> ops;
  for (size_t i = 0; i < numValues; ++i) {
    ops.push_back(net::co_spawn(ctx.ctx_, increment(ctx),
                  net::deferred));
  }
  auto group = net::experimental::make_parallel_group(std::move(ops));
  auto future = group.async_wait(net::experimental::wait_for_one_error(), net::use_future);
  std::vector<ad_utility::JThread> threads;
  for (size_t i = 0; i < 20; ++i) {
    threads.emplace_back([&] { ctx.ctx_.run(); });
  }
  ctx.done_.wait(false);
  future.get();
  threads.clear();
  ASSERT_EQ(ctx.x_, numValues);
}

TEST(AsioHelpers, simpleException) {
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

    static constexpr size_t numValues = 2000;
    auto increment = [](Context& ctx) -> net::awaitable<void> {
        //co_await net::post(ad_utility::ChangeStrandToken{ctx.strand2_, net::use_awaitable});
        throw std::runtime_error("I am done here");
    };

    using Op = decltype(net::co_spawn(ctx.strand1_, increment(ctx),
                                      net::deferred));
    std::vector<Op> ops;
    for (size_t i = 0; i < numValues; ++i) {
        ops.push_back(net::co_spawn(ctx.ctx_, increment(ctx),
                                    net::deferred));
    }
    auto group = net::experimental::make_parallel_group(std::move(ops));
    auto future = group.async_wait(net::experimental::wait_for_one_error(), net::use_future);
    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < 20; ++i) {
        threads.emplace_back([&] { ctx.ctx_.run(); });
    }
    future.get();
    threads.clear();
    ASSERT_EQ(ctx.x_, numValues);
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
