// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FairShareExecutor.h"

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>

#include "backports/algorithm.h"
#include "util/ConcurrencyLimitedExecutor.h"
#include "util/Timer.h"

namespace ad_utility {

namespace {
// The outcome of `runAndDestroy`.
struct TaskOutcome {
  double seconds_;
  std::exception_ptr exception_;
};

// Run the `task` and destroy it. Return how long this took and the exception
// that the task threw (if any).
//
// NOTE: This must not be called while the state of the scheduler is locked,
// because the task (and also its destructor) may submit further tasks.
TaskOutcome runAndDestroy(FairScheduler::Task task) {
  Timer timer{Timer::Started};
  std::exception_ptr exception;
  try {
    std::invoke(std::move(task));
  } catch (...) {
    exception = std::current_exception();
  }
  // Destroy the task explicitly, so that its destruction is also measured.
  task = FairScheduler::Task{};
  return {Timer::toSeconds(timer.value()), std::move(exception)};
}
}  // namespace

// _____________________________________________________________________________
FairScheduler::FairScheduler(ql::any_io_executor executor, size_t numSlots)
    // NOTE: `State` has to be passed explicitly, because for a nested class
    // with default member initializers, `Synchronized` sees no default
    // constructor.
    : executor_{std::move(executor)}, numSlots_{numSlots}, state_{State{}} {
  AD_CONTRACT_CHECK(static_cast<bool>(executor_));
  AD_CONTRACT_CHECK(numSlots_ > 0);
}

// _____________________________________________________________________________
std::shared_ptr<FairScheduler::Client> FairScheduler::makeClient(
    FairShareClass fairShareClass, size_t maxConcurrency) {
  AD_CONTRACT_CHECK(maxConcurrency > 0);
  auto client = std::make_shared<Client>();
  client->class_ = fairShareClass;
  client->maxConcurrency_ = maxConcurrency;
  return client;
}

// _____________________________________________________________________________
void FairScheduler::setShare(FairShareClass fairShareClass, double share) {
  AD_CONTRACT_CHECK(share > 0);
  state_.wlock()->classOf(fairShareClass).share_ = share;
}

// _____________________________________________________________________________
void FairScheduler::submit(const std::shared_ptr<Client>& client, Task task) {
  state_.withWriteLock([this, &client, &task](State& state) {
    client->queue_.push_back(std::move(task));
    if (!client->isActive_) {
      auto& cls = state.classOf(client->class_);
      if (cls.activeClients_.empty()) {
        // A class that becomes active neither gets credit for the time in
        // which it had no work, nor is it penalized for work done before.
        cls.virtualTime_ =
            std::max(cls.virtualTime_, state.lastDispatchedClassTime_);
      }
      client->virtualTime_ =
          std::max(client->virtualTime_, cls.lastDispatchedClientTime_);
      client->isActive_ = true;
      cls.activeClients_.push_back(client);
    }
    startWorker(state);
  });
}

// _____________________________________________________________________________
bool FairScheduler::ClassState::hasRunnableClient() const {
  return ql::ranges::any_of(activeClients_, [](const auto& client) {
    return client->isBelowMaxConcurrency();
  });
}

// _____________________________________________________________________________
auto FairScheduler::ClassState::nextClient() -> Clients::iterator {
  // Clients that may not start another task are never the minimum.
  auto it = ql::ranges::min_element(
      activeClients_, std::less<>{}, [](const auto& client) {
        return client->isBelowMaxConcurrency()
                   ? client->virtualTime_
                   : std::numeric_limits<double>::infinity();
      });
  AD_CORRECTNESS_CHECK(it != activeClients_.end() &&
                       (*it)->isBelowMaxConcurrency());
  return it;
}

// _____________________________________________________________________________
void FairScheduler::ClassState::deactivate(Clients::iterator it) {
  (*it)->isActive_ = false;
  std::iter_swap(it, std::prev(activeClients_.end()));
  activeClients_.pop_back();
}

// _____________________________________________________________________________
bool FairScheduler::State::hasRunnableTask() const {
  return ql::ranges::any_of(classes_, &ClassState::hasRunnableClient);
}

// _____________________________________________________________________________
void FairScheduler::startWorker(State& state) {
  // Start (at most) one worker per call. Every `submit` calls this, and a
  // worker keeps running tasks as long as there are runnable ones, so this
  // suffices to keep up to `numSlots_` workers busy.
  if (state.numWorkers_ < numSlots_ && state.hasRunnableTask()) {
    ++state.numWorkers_;
    boost::asio::post(executor_,
                      [self = shared_from_this()]() { self->runWorker(); });
  }
}

// _____________________________________________________________________________
auto FairScheduler::State::popNextTask() -> std::optional<ClientAndTask> {
  // The class with the least virtual time among those that may start a task.
  auto& cls = *ql::ranges::min_element(
      classes_, std::less<>{}, [](const ClassState& candidate) {
        return candidate.hasRunnableClient()
                   ? candidate.virtualTime_
                   : std::numeric_limits<double>::infinity();
      });
  if (!cls.hasRunnableClient()) {
    return std::nullopt;
  }
  auto clientIt = cls.nextClient();
  auto client = *clientIt;
  Task task = std::move(client->queue_.front());
  client->queue_.pop_front();
  ++client->numRunning_;
  cls.lastDispatchedClientTime_ = client->virtualTime_;
  lastDispatchedClassTime_ = cls.virtualTime_;
  if (client->queue_.empty()) {
    cls.deactivate(clientIt);
  }
  return ClientAndTask{std::move(client), std::move(task)};
}

// _____________________________________________________________________________
void FairScheduler::State::recordRunTime(Client& client, double seconds) {
  --client.numRunning_;
  client.virtualTime_ += seconds;
  auto& cls = classOf(client.class_);
  cls.virtualTime_ += seconds / cls.share_;
}

// _____________________________________________________________________________
void FairScheduler::runWorker() {
  // When the worker returns (also via an exception), give its slot back, and
  // let another worker take over if there is work (which might have been
  // submitted after the worker found none, but before the slot was released).
  absl::Cleanup releaseSlot{[this]() {
    state_.withWriteLock([this](State& state) {
      --state.numWorkers_;
      startWorker(state);
    });
  }};
  auto next = state_.wlock()->popNextTask();
  while (next.has_value()) {
    auto& client = next->first;
    auto [seconds, exception] = runAndDestroy(std::move(next->second));
    if (exception) {
      // Like a plain thread pool, let the exception propagate out of the
      // thread of the underlying executor.
      state_.wlock()->recordRunTime(*client, seconds);
      std::rethrow_exception(exception);
    }
    next = state_.withWriteLock([&client, seconds = seconds](State& state) {
      state.recordRunTime(*client, seconds);
      return state.popNextTask();
    });
  }
}

// _____________________________________________________________________________
qlever::Executor makeFairShareExecutor(qlever::Executor executor,
                                       size_t numSlots) {
  AD_CONTRACT_CHECK(static_cast<bool>(executor));
  AD_CONTRACT_CHECK(numSlots > 0);
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto scheduler =
      std::make_shared<FairScheduler>(std::move(executor), numSlots);
  auto client = scheduler->makeClient(FairShareClass::Background);
  return FairShareExecutor{std::move(scheduler), std::move(client)};
#else
  return executor;
#endif
}

// _____________________________________________________________________________
qlever::Executor makeFairShareClientExecutor(
    const qlever::Executor& executor, FairShareClass fairShareClass,
    std::optional<size_t> maxConcurrency) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  if (const auto* fair = executor.target<FairShareExecutor>()) {
    const auto& scheduler = fair->scheduler();
    return FairShareExecutor{
        scheduler,
        scheduler->makeClient(
            fairShareClass,
            maxConcurrency.value_or(std::numeric_limits<size_t>::max()))};
  }
#endif
  return makeConcurrencyLimitedExecutor(executor, maxConcurrency);
}

// _____________________________________________________________________________
void setFairShare(const qlever::Executor& executor,
                  FairShareClass fairShareClass, double share) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  if (const auto* fair = executor.target<FairShareExecutor>()) {
    fair->scheduler()->setShare(fairShareClass, share);
  }
#endif
}

}  // namespace ad_utility

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
static_assert(
    boost::asio::execution::is_executor_v<ad_utility::FairShareExecutor>);
#endif
