//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
#define QLEVER_SRC_UTIL_PARALLELEXECUTOR_H

#include <future>
#include <vector>

#include "util/jthread.h"

namespace ad_utility {
// Run the given tasks in parallel and wait for their completion. This function
// will spawn a new thread for each task. If one of the tasks throws an
// exception, this exception will be rethrown in the main thread. If multiple
// tasks throw exceptions, only the first one will be rethrown.
inline void runTasksInParallel(
    std::vector<std::packaged_task<void()>>&& tasks) {
  std::vector<std::future<void>> futures;
  futures.reserve(tasks.size());
  std::vector<JThread> threads;
  futures.reserve(tasks.size());
  for (auto& task : tasks) {
    futures.push_back(task.get_future());
    threads.push_back(JThread{std::move(task)});
  }
  // Wait for completion.
  for (auto& future : futures) {
    future.get();
  }
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
