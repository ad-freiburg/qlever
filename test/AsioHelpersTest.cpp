//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>
#include <latch>

#include "util/http/beast.h"
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

namespace {

static constexpr auto stallUntilCancelled = []() -> net::awaitable<void> {
  auto exec = co_await net::this_coro::executor;
  net::steady_timer t{exec, std::chrono::microseconds(1)};
  co_await t.async_wait(net::deferred);
  co_return;
};

void checkStrandNotRunning(auto strand) {
  auto future = net::post(net::bind_executor(strand, std::packaged_task<void()>{[](){return;}}));
  auto status = future.wait_for(std::chrono::milliseconds(5));
  EXPECT_EQ(status, std::future_status::ready);
  //future.get();
}
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
}

TEST(AsioHelpers, deadlockingWithStrands) {
  Context ctx;
  [[maybe_unused]] static constexpr auto dummy = [](auto strand, Context& ctx) -> net::awaitable<void> {
    ctx.x_++;
    EXPECT_TRUE(strand.running_in_this_thread());
    co_return;
  };

  [[maybe_unused]] static constexpr auto a = [](auto strand, auto& ctx) -> net::awaitable<void> {
    for (size_t i = 0; i < 10'000; ++i) {
      co_await ad_utility::runAwaitableOnStrandAwaitable(strand, dummy(strand, ctx));
    }
    EXPECT_FALSE(strand.running_in_this_thread());
  };

  auto b = [](auto strand, auto& ctx) -> net::awaitable<void> {
    co_await (a(strand, ctx) && a(strand, ctx));
  };

  net::co_spawn(ctx.ctx_, b(ctx.strand1_, ctx), net::detached);

  ad_utility::JThread j{[&]{ctx.ctx_.run();}};
  ctx.ctx_.run();
  EXPECT_EQ(ctx.x_, 20'000);
}


TEST(AsioHelpers, correctStrandScheduling) {
   Context ctx;
   static constexpr auto dummy = [](auto strand, [[maybe_unused]] auto wrongStrand)->net::awaitable<void> {
     EXPECT_TRUE(strand.running_in_this_thread());
     EXPECT_FALSE(wrongStrand.running_in_this_thread());
     co_return;
   };

   auto scheduler = [](Context& ctx) -> net::awaitable<void> {
     EXPECT_TRUE(ctx.strand1_.running_in_this_thread());
     EXPECT_FALSE(ctx.strand2_.running_in_this_thread());
     co_await ad_utility::runAwaitableOnStrandAwaitable(ctx.strand2_, dummy(ctx.strand2_, ctx.strand1_));
     EXPECT_TRUE(ctx.strand1_.running_in_this_thread());
     EXPECT_FALSE(ctx.strand2_.running_in_this_thread());
   };
   net::co_spawn(ctx.strand1_, scheduler(ctx), net::detached);
   ctx.ctx_.run();
}

TEST(AsioHelpers, CancellationSegfault) {
   auto run = [](auto strand) -> net::awaitable<void> {
     /*
     co_await [](auto strand) -> net::awaitable<void> {
       co_await ad_utility::runAwaitableOnStrandAwaitable(
           strand, []() -> net::awaitable<void> { co_return; }());
     }(strand);
      */
     co_await net::co_spawn(
         strand,
         []() -> net::awaitable<void> {
           co_await stallUntilCancelled();
           co_return;
         }(),
         net::use_awaitable);
     /*
       co_await ad_utility::runAwaitableOnStrandAwaitable(
               strand, []() -> net::awaitable<void> { co_return; }());
               */
       co_await stallUntilCancelled();
   };

   Context ctx;
   for (size_t i = 0; i < 200; ++i) {
     net::cancellation_signal sig;

     net::co_spawn(ctx.strand1_, run(ctx.strand2_),
                   net::bind_cancellation_slot(sig.slot(), net::detached));
     {
       ad_utility::JThread t{[&] { ctx.ctx_.run(); }};
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
       net::dispatch(ctx.strand1_,
                     [&]() { sig.emit(net::cancellation_type::terminal); });
     }
   }
}
/*
TEST(AsioHelpers, cancellationOnOtherStrand) {

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
  future.get();
  threads.clear();
  ASSERT_EQ(ctx.x_, numValues);
}
 */

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
        co_await ctx.infiniteTimer1_.async_wait(net::deferred);
        throw std::runtime_error{"cancelling this"};
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

    using Op = decltype(net::co_spawn(ctx.strand1_, ad_utility::runAwaitableOnStrandAwaitable(ctx.strands_.at(0), increment(ctx, 0)), net::deferred));
    std::vector<Op> ops;
    //for (size_t i = 0; i < numValues; ++i) {
        for (size_t i = 0; i < 10; ++i) {
        ops.push_back(net::co_spawn(ctx.strand1_, ad_utility::runAwaitableOnStrandAwaitable(ctx.strands_.at(i), increment(ctx, i)), net::deferred));
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

    try {
        auto future = net::co_spawn(ctx.ctx_, awaitAll(group), net::use_future);
        std::vector<ad_utility::JThread> threads;
        for (size_t i = 0; i < 30; ++i) {
          threads.emplace_back([&] { ctx.ctx_.run(); });
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        net::dispatch(ctx.strand1_, std::packaged_task<void()>{[&]() {
                        ctx.infiniteTimer1_.cancel();
                      }})
            .wait();
        LOG(INFO) << "Cancelled the main thread" << std::endl;
        future.get();
        // EXPECT_GT(ctx.x_, 0);
        threads.clear();
    } catch (...) {
        FAIL();
    }
}
