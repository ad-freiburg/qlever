// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FairShareExecutor.h"

#include <algorithm>
#include <boost/asio/post.hpp>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>

#include "backports/algorithm.h"
#include "util/Algorithm.h"
#include "util/ConcurrencyLimitedExecutor.h"
#include "util/Timer.h"

namespace ad_utility {

namespace {
// The outcome of `runAndDestroy`: the run time of the task, and the exception
// that it threw (or `nullptr`).
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
      // The client (and maybe its class) becomes active, initially or after
      // it ran out of queued tasks. Its virtual time is moved forward to that
      // of the client (class) that was dispatched last, which is about the
      // least one among the active ones, as the least one is always dispatched
      // next. This works for any number of clients (classes). This way, it
      // neither gets credit for the time in which it had no work (with which it
      // would starve the others), nor is it penalized for work done before.
      if (cls.activeClients_.empty()) {
        cls.virtualTime_ =
            std::max(cls.virtualTime_, state.lastDispatchedClassTime_);
      }
      client->virtualTime_ =
          std::max(client->virtualTime_, cls.lastDispatchedClientTime_);
      client->isActive_ = true;
      cls.activeClients_.push_back(client);
    }
    // A new worker is only needed if the task may start right away. Otherwise
    // the client already runs its maximal number of tasks, and the worker that
    // finishes one of them takes the next one.
    if (client->isBelowMaxConcurrency() && state.numWorkers_ < numSlots_) {
      ++state.numWorkers_;
      postWorker();
    }
  });
}

// _____________________________________________________________________________
auto FairScheduler::ClassState::nextClient()
    -> std::optional<Clients::iterator> {
  // Clients that may start a task come first, ordered by their virtual time.
  auto it = ql::ranges::min_element(activeClients_, {}, [](const auto& client) {
    return std::pair{!client->isBelowMaxConcurrency(), client->virtualTime_};
  });
  if (it == activeClients_.end() || !(*it)->isBelowMaxConcurrency()) {
    return std::nullopt;
  }
  return it;
}

// _____________________________________________________________________________
void FairScheduler::ClassState::deactivate(Clients::iterator it) {
  (*it)->isActive_ = false;
  std::iter_swap(it, std::prev(activeClients_.end()));
  activeClients_.pop_back();
}

// _____________________________________________________________________________
auto FairScheduler::State::nextClassAndClient()
    -> std::optional<ClassAndClient> {
  // Each class with its `nextClient` (computed only once per class).
  struct Candidate {
    ClassState& class_;
    std::optional<ClassState::Clients::iterator> client_;
  };
  auto candidates = ad_utility::transformArray(
      classes_, [](auto&& cls) { return Candidate{cls, cls.nextClient()}; });
  // Classes that have a `nextClient` come first, ordered by their virtual time.
  auto& best =
      *ql::ranges::min_element(candidates, {}, [](const Candidate& candidate) {
        return std::pair{!candidate.client_.has_value(),
                         candidate.class_.virtualTime_};
      });
  if (!best.client_.has_value()) {
    return std::nullopt;
  }
  return ClassAndClient{best.class_, best.client_.value()};
}

// _____________________________________________________________________________
void FairScheduler::postWorker() {
  boost::asio::post(executor_,
                    [self = shared_from_this()]() { self->runWorker(); });
}

// _____________________________________________________________________________
auto FairScheduler::State::popNextTaskOrRetireWorker()
    -> std::optional<ClientAndTask> {
  auto next = nextClassAndClient();
  if (!next.has_value()) {
    // Retiring in the same critical section in which no task was found
    // guarantees that a task that is submitted later starts a new worker.
    --numWorkers_;
    return std::nullopt;
  }
  auto& [cls, clientIt] = next.value();
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
  auto next = state_.wlock()->popNextTaskOrRetireWorker();
  while (next.has_value()) {
    auto& [client, task] = next.value();
    auto [seconds, exception] = runAndDestroy(std::move(task));
    if (exception) {
      // Like a plain thread pool, let the exception propagate out of the
      // thread of the underlying executor. The slot of this worker is handed
      // over to a new one (which retires right away if there is no task).
      state_.wlock()->recordRunTime(*client, seconds);
      postWorker();
      std::rethrow_exception(exception);
    }
    next = state_.withWriteLock(
        [&client = client, seconds = seconds](State& state) {
          state.recordRunTime(*client, seconds);
          return state.popNextTaskOrRetireWorker();
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
  auto client = scheduler->makeClient(FairShareClass::Foreground);
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
