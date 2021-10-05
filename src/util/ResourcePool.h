//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 24.09.21.
//

#ifndef QLEVER_RESOURCEPOOL_H
#define QLEVER_RESOURCEPOOL_H

#include <condition_variable>
#include <memory>
#include <queue>

namespace ad_utility {

template <typename Resource>
class ResourcePool {
 private:
  std::queue<std::unique_ptr<Resource>> m_queue;
  std::condition_variable m_resourceWasReturned;
  std::mutex m_mutex;

  static auto getReturningDeleter(ResourcePool* poolPointer) {
    return [poolPointer](Resource* pointer) {
      std::unique_lock lock{poolPointer->m_mutex};
      poolPointer->m_queue.emplace(pointer);
      lock.unlock();
      poolPointer->m_resourceWasReturned.notify_one();
    };
  }

  using ReturningDeleter = decltype(getReturningDeleter(nullptr));
  using ReturningPtr = std::unique_ptr<Resource, ReturningDeleter>;

 public:
  ResourcePool() = default;

  template <typename... ConstructorArgs>
  void addResource(ConstructorArgs&&... t_constructorArgs) {
    std::unique_lock lock{m_mutex};
    m_queue.push(std::make_unique<Resource>(
        std::forward<ConstructorArgs>(t_constructorArgs)...));
    ;
  }

  ReturningPtr acquire() {
    std::unique_lock lock{m_mutex};
    m_resourceWasReturned.wait(lock, [&] { return !m_queue.empty(); });
    ReturningPtr handle{m_queue.front().release(), getReturningDeleter(this)};
    m_queue.pop();
    return handle;
  }

  static void release(ReturningPtr) {
    // The move of the argument alone triggers the deleter
  }
};

}  // namespace ad_utility

#endif  // QLEVER_RESOURCEPOOL_H
