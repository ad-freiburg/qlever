// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FAIRSHAREEXECUTOR_H
#define QLEVER_SRC_UTIL_FAIRSHAREEXECUTOR_H

#include <absl/functional/any_invocable.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/Executor.h"
#include "util/Synchronized.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/execution.hpp>
#include <boost/asio/prefer.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/require.hpp>
#include <concepts>
#include <functional>
#include <type_traits>

#include "util/Forward.h"
#endif

namespace ad_utility {

// The classes of work that share the threads of a `FairScheduler`. Under
// contention, the threads are split between the classes according to their
// shares (see `FairScheduler::setShare`), and within a class equally between
// its clients. A class (or client) that has no work leaves its threads to the
// others, so a single client gets all the threads if it is the only one with
// work (the scheduler is work-conserving).
enum class FairShareClass { Queries = 0, Background = 1 };

// A scheduler that runs tasks of several clients on the threads of an
// underlying executor (typically a `boost::asio::thread_pool`) such that
//
// 1. at most `numSlots` tasks run at the same time (one per thread of the
//    underlying pool), so the pool's own FIFO queue never holds more than the
//    tasks that are about to run, and
// 2. whenever a slot becomes free, the next task is taken from the class and
//    the client that so far received the least CPU time relative to its share
//    (stride scheduling on the measured run time of the tasks).
//
// This replaces the plain FIFO order of the pool, in which a client that posts
// many tasks (for example the read-ahead of many concurrent index scans) delays
// the tasks of all other clients (for example a running index rebuild).
//
// NOTE: Tasks must not block on other tasks of the same scheduler, just like
// they must not block on other tasks of a plain thread pool.
class FairScheduler : public std::enable_shared_from_this<FairScheduler> {
 public:
  using Task = absl::AnyInvocable<void() &&>;

  struct Client;

 private:
  // The scheduling state of one `FairShareClass`.
  struct ClassState {
    // The share of this class (any positive number, relative to the shares of
    // the other classes).
    double share_ = 1.0;
    // The virtual time of the class: its received CPU time divided by its
    // share.
    double virtualTime_ = 0;
    // The clients of the class that currently have queued tasks.
    using Clients = std::vector<std::shared_ptr<Client>>;
    Clients activeClients_;
    // The virtual time of the client that was dispatched last, used to place
    // clients that become active (so that a new client neither starves the
    // others nor is starved by them).
    double lastDispatchedClientTime_ = 0;

    // Return true if any active client of this class may start another task.
    bool hasRunnableClient() const;

    // Return the active client with the least virtual time among those that
    // may start another task. Requires `hasRunnableClient()`.
    Clients::iterator nextClient();

    // Remove the client at `it` (whose queue is empty) from the active ones.
    void deactivate(Clients::iterator it);
  };

 public:
  // A source of tasks (e.g. a single query), see `makeClient`.
  struct Client {
    FairShareClass class_;
    // The maximal number of tasks that may run at the same time.
    size_t maxConcurrency_ = std::numeric_limits<size_t>::max();
    // The members below are only accessed while the `state_` of the scheduler
    // is locked.
    // The tasks that wait to be run.
    std::deque<Task> queue_;
    // The number of tasks that currently run.
    size_t numRunning_ = 0;
    // The CPU time (in seconds) that the tasks of this client received.
    double virtualTime_ = 0;
    // True if the client is in the `activeClients_` of its class.
    bool isActive_ = false;

    // Return true if the client may start another task.
    bool isBelowMaxConcurrency() const { return numRunning_ < maxConcurrency_; }
  };

 private:
  using ClientAndTask = std::pair<std::shared_ptr<Client>, Task>;

  // The mutable state of the scheduler, which is protected by a single mutex.
  struct State {
    // The number of workers that are posted to or running on the underlying
    // executor. Each of them occupies one of the `numSlots_` slots.
    size_t numWorkers_ = 0;
    // TODO<#3918> Size it via `qlever::enumSize<FairShareClass>`.
    std::array<ClassState, 2> classes_;
    double lastDispatchedClassTime_ = 0;

    ClassState& classOf(FairShareClass fairShareClass) {
      return classes_[static_cast<size_t>(fairShareClass)];
    }

    // Return true if any active client may start another task.
    bool hasRunnableTask() const;

    // Pop the next task to run (from the class and the client with the least
    // virtual time), or return `std::nullopt` if no task may run.
    std::optional<ClientAndTask> popNextTask();

    // Account the `seconds` that a task of the `client` ran (and that has
    // finished) to the `client` and its class.
    void recordRunTime(Client& client, double seconds);
  };

  ql::any_io_executor executor_;
  size_t numSlots_;
  ad_utility::Synchronized<State, std::mutex> state_;

 public:
  FairScheduler(ql::any_io_executor executor, size_t numSlots);

  const ql::any_io_executor& underlyingExecutor() const { return executor_; }

  // Create a new client of the given class, which may run at most
  // `maxConcurrency` tasks at the same time (by default without a limit).
  std::shared_ptr<Client> makeClient(
      FairShareClass fairShareClass,
      size_t maxConcurrency = std::numeric_limits<size_t>::max());

  // Set the share of a class (must be positive).
  void setShare(FairShareClass fairShareClass, double share);

  // Queue `task` for `client` and make sure that a worker will run it.
  void submit(const std::shared_ptr<Client>& client, Task task);

 private:
  // The loop of a worker on a thread of the underlying executor: run tasks as
  // long as there are any that may run, then give the slot of the worker back.
  void runWorker();

  // Start a new worker if there is a free slot and a runnable task. The
  // `state` must be the locked `state_`.
  void startWorker(State& state);
};

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace detail::fairShare {
template <typename Property>
concept QueryableProperty =
    boost::asio::can_query_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept RequirableProperty =
    boost::asio::can_require_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept PreferableProperty =
    boost::asio::can_prefer_v<const ql::any_io_executor&, Property>;
template <typename Function>
concept ExecutableFunction =
    std::invocable<std::decay_t<Function>&> &&
    std::constructible_from<std::decay_t<Function>, Function> &&
    std::move_constructible<std::decay_t<Function>>;
}  // namespace detail::fairShare

// An executor that runs its function objects as tasks of a single client of a
// `FairScheduler`. Copies belong to the same client. Function objects are
// never run inline.
class FairShareExecutor {
  std::shared_ptr<FairScheduler> scheduler_;
  std::shared_ptr<FairScheduler::Client> client_;
  // The executor of the underlying pool, to which all properties are
  // forwarded.
  ql::any_io_executor executor_;

  FairShareExecutor(std::shared_ptr<FairScheduler> scheduler,
                    std::shared_ptr<FairScheduler::Client> client,
                    ql::any_io_executor executor)
      : scheduler_{std::move(scheduler)},
        client_{std::move(client)},
        executor_{std::move(executor)} {}

 public:
  FairShareExecutor(std::shared_ptr<FairScheduler> scheduler,
                    std::shared_ptr<FairScheduler::Client> client)
      : scheduler_{std::move(scheduler)}, client_{std::move(client)} {
    AD_CONTRACT_CHECK(scheduler_ != nullptr && client_ != nullptr);
    executor_ = scheduler_->underlyingExecutor();
  }

  bool operator==(const FairShareExecutor&) const noexcept = default;

  const std::shared_ptr<FairScheduler>& scheduler() const { return scheduler_; }

  template <detail::fairShare::QueryableProperty Property>
  decltype(auto) query(const Property& property) const {
    return boost::asio::query(executor_, property);
  }

  template <detail::fairShare::RequirableProperty Property>
  FairShareExecutor require(const Property& property) const {
    return {scheduler_, client_, boost::asio::require(executor_, property)};
  }

  template <detail::fairShare::PreferableProperty Property>
  FairShareExecutor prefer(const Property& property) const {
    return {scheduler_, client_, boost::asio::prefer(executor_, property)};
  }

  template <detail::fairShare::ExecutableFunction Function>
  void execute(Function&& function) const {
    scheduler_->submit(client_, [function = AD_FWD(function)]() mutable {
      std::invoke(function);
    });
  }
};
#endif

// Return an executor that runs its tasks on `executor` via a new
// `FairScheduler` with `numSlots` slots (which should be the number of threads
// of `executor`), as the default client of the `Background` class. Clients of
// other classes are derived from it via `makeFairShareClientExecutor`. In the
// reduced C++17 build, `executor` is returned unchanged.
qlever::Executor makeFairShareExecutor(qlever::Executor executor,
                                       size_t numSlots);

// If `executor` is a `FairShareExecutor`, return an executor for a new client
// of the given class on the same scheduler. Otherwise (for example in tests
// that use a plain thread pool, or in the reduced C++17 build), fall back to
// limiting the concurrency of `executor` (see
// `makeConcurrencyLimitedExecutor`).
qlever::Executor makeFairShareClientExecutor(
    const qlever::Executor& executor, FairShareClass fairShareClass,
    std::optional<size_t> maxConcurrency);

// Set the share of a class on the scheduler of `executor`, if it has one.
void setFairShare(const qlever::Executor& executor,
                  FairShareClass fairShareClass, double share);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_FAIRSHAREEXECUTOR_H
