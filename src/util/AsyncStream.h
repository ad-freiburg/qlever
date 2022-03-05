// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <absl/cleanup/cleanup.h>

#include <exception>
#include <thread>
#include <type_traits>

#include "./Generator.h"
#include "./ThreadSafeQueue.h"

namespace ad_utility::streams {

using ad_utility::data_structures::ThreadSafeQueue;

/**
 * Runs the passed generator on an asynchronous layer. This can improve
 * performance if the generated values are expensive to compute so that the time
 * between resumes can be used additionally.
 */
template <typename GeneratorType>
cppcoro::generator<std::string> runStreamAsync(
    std::remove_reference_t<GeneratorType> generator, size_t bufferLimit) {
  ThreadSafeQueue<std::string> queue{bufferLimit};
  std::exception_ptr exception = nullptr;
  std::thread thread{[&] {
    try {
      for (auto& value : generator) {
        if (queue.push(std::move(value))) {
          return;
        }
      }
    } catch (...) {
      exception = std::current_exception();
    }
    queue.finish();
  }};

  absl::Cleanup cleanup{[&] {
    queue.abort();
    thread.join();
    if (exception) {
      // This exception will only be thrown once all the already existing values
      // have already been processed.
      std::rethrow_exception(exception);
    }
  }};

  while (std::optional<std::string> value = queue.pop()) {
    co_yield value.value();
  }
}
}  // namespace ad_utility::streams
