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

constexpr size_t BUFFER_LIMIT = 100;

using ad_utility::data_structures::ThreadSafeQueue;

template <typename GeneratorType>
cppcoro::generator<std::string> runStreamAsync(
    std::remove_reference_t<GeneratorType> generator) {
  ThreadSafeQueue<std::string> queue{BUFFER_LIMIT};
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
      std::rethrow_exception(exception);
    }
  }};

  while (std::optional<std::string> value = queue.pop()) {
    co_yield value.value();
  }
}
}  // namespace ad_utility::streams
