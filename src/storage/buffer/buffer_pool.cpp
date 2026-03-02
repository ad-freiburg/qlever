// Copyright 2024, DuckDB contributors. Adapted for QLever.

#include "storage/buffer/buffer_pool.h"

#include <deque>
#include <thread>

#include "storage/compat/duckdb_allocator.h"
#include "storage/compat/duckdb_exceptions.h"
#include "storage/compat/duckdb_types.h"

namespace duckdb {

BufferEvictionNode::BufferEvictionNode(weak_ptr<BlockHandle> handle_p,
                                       idx_t eviction_seq_num)
    : handle(std::move(handle_p)), handle_sequence_number(eviction_seq_num) {
  D_ASSERT(!handle.expired());
}

bool BufferEvictionNode::CanUnload(BlockHandle& handle_p) {
  if (handle_sequence_number != handle_p.EvictionSequenceNumber()) {
    return false;
  }
  return handle_p.CanUnload();
}

shared_ptr<BlockHandle> BufferEvictionNode::TryGetBlockHandle() {
  auto handle_p = handle.lock();
  if (!handle_p) {
    return nullptr;
  }
  if (!CanUnload(*handle_p)) {
    return nullptr;
  }
  return handle_p;
}

// A simple concurrent queue implementation using a mutex and deque.
// DuckDB uses moodycamel::ConcurrentQueue; we simplify here.
struct SimpleQueue {
  std::deque<BufferEvictionNode> items;
  mutex queue_lock;

  void enqueue(BufferEvictionNode&& node) {
    lock_guard<mutex> guard(queue_lock);
    items.push_back(std::move(node));
  }

  bool try_dequeue(BufferEvictionNode& node) {
    lock_guard<mutex> guard(queue_lock);
    if (items.empty()) return false;
    node = std::move(items.front());
    items.pop_front();
    return true;
  }

  idx_t try_dequeue_bulk(
      typename std::deque<BufferEvictionNode>::iterator /*begin*/,
      idx_t count) {
    lock_guard<mutex> guard(queue_lock);
    idx_t dequeued = 0;
    // We cannot write to the iterator from our deque, so handle differently.
    (void)count;
    return dequeued;
  }

  idx_t size_approx() {
    lock_guard<mutex> guard(queue_lock);
    return items.size();
  }
};

struct EvictionQueue {
 public:
  explicit EvictionQueue(const FileBufferType file_buffer_type_p)
      : file_buffer_type(file_buffer_type_p),
        evict_queue_insertions(0),
        total_dead_nodes(0) {}

 public:
  // Add a buffer handle to the eviction queue.
  bool AddToEvictionQueue(BufferEvictionNode&& node) {
    q.enqueue(std::move(node));
    return ++evict_queue_insertions % INSERT_INTERVAL == 0;
  }

  bool TryDequeueWithLock(BufferEvictionNode& node) {
    lock_guard<mutex> lock(purge_lock);
    return q.try_dequeue(node);
  }

  void Purge() {
    if (!purge_lock.try_lock()) {
      return;
    }
    lock_guard<mutex> lock{purge_lock, std::adopt_lock};

    idx_t purge_size = INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER;
    idx_t approx_q_size = q.size_approx();
    if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
      return;
    }

    idx_t max_purges = approx_q_size / purge_size;
    while (max_purges != 0) {
      PurgeIteration(purge_size);
      approx_q_size = q.size_approx();
      if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
        break;
      }
      idx_t approx_dead_nodes = total_dead_nodes;
      approx_dead_nodes =
          approx_dead_nodes > approx_q_size ? approx_q_size : approx_dead_nodes;
      idx_t approx_alive_nodes = approx_q_size - approx_dead_nodes;
      if (approx_alive_nodes * (ALIVE_NODE_MULTIPLIER - 1) >
          approx_dead_nodes) {
        break;
      }
      max_purges--;
    }
  }

  template <typename FN>
  void IterateUnloadableBlocks(FN fn) {
    for (;;) {
      BufferEvictionNode node;
      if (!q.try_dequeue(node)) {
        if (!TryDequeueWithLock(node)) {
          return;
        }
      }
      auto handle = node.TryGetBlockHandle();
      if (!handle) {
        DecrementDeadNodes();
        continue;
      }
      auto lock = handle->GetLock();
      if (!node.CanUnload(*handle)) {
        DecrementDeadNodes();
        continue;
      }
      if (!fn(node, handle, lock)) {
        break;
      }
    }
  }

  inline void IncrementDeadNodes() { total_dead_nodes++; }
  inline void DecrementDeadNodes() { total_dead_nodes--; }

 private:
  void PurgeIteration(const idx_t purge_size) {
    // Simple purge: dequeue and re-enqueue alive nodes.
    vector<BufferEvictionNode> purge_nodes;
    for (idx_t i = 0; i < purge_size; i++) {
      BufferEvictionNode node;
      if (!q.try_dequeue(node)) break;
      auto handle = node.TryGetBlockHandle();
      if (handle) {
        q.enqueue(std::move(node));
      } else {
        total_dead_nodes--;
      }
    }
  }

 public:
  const FileBufferType file_buffer_type;
  SimpleQueue q;

 private:
  static constexpr idx_t INSERT_INTERVAL = 4096;
  static constexpr idx_t PURGE_SIZE_MULTIPLIER = 2;
  static constexpr idx_t EARLY_OUT_MULTIPLIER = 4;
  static constexpr idx_t ALIVE_NODE_MULTIPLIER = 4;

  atomic<idx_t> evict_queue_insertions;
  atomic<idx_t> total_dead_nodes;
  mutex purge_lock;
};

BufferPool::BufferPool(idx_t maximum_memory, bool track_eviction_timestamps)
    : eviction_queue_sizes({BLOCK_QUEUE_SIZE, MANAGED_BUFFER_QUEUE_SIZE,
                            TINY_BUFFER_QUEUE_SIZE}),
      maximum_memory(maximum_memory),
      track_eviction_timestamps(track_eviction_timestamps) {
  for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
    const auto type = static_cast<FileBufferType>(type_idx + 1);
    const auto& type_queue_size = eviction_queue_sizes[type_idx];
    for (idx_t queue_idx = 0; queue_idx < type_queue_size; queue_idx++) {
      queues.push_back(make_uniq<EvictionQueue>(type));
    }
  }
}

BufferPool::~BufferPool() {}

bool BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle>& handle) {
  auto& queue = GetEvictionQueueForBlockHandle(*handle);
  D_ASSERT(handle->Readers() == 0);
  auto ts = handle->NextEvictionSequenceNumber();
  if (track_eviction_timestamps) {
    handle->SetLRUTimestamp(
        std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now())
            .time_since_epoch()
            .count());
  }
  if (ts != 1) {
    queue.IncrementDeadNodes();
  }
  return queue.AddToEvictionQueue(
      BufferEvictionNode(weak_ptr<BlockHandle>(handle), ts));
}

EvictionQueue& BufferPool::GetEvictionQueueForBlockHandle(
    const BlockHandle& handle) {
  const auto& handle_buffer_type = handle.GetBufferType();
  idx_t queue_index = 0;
  for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
    const auto queue_buffer_type = static_cast<FileBufferType>(type_idx + 1);
    if (handle_buffer_type == queue_buffer_type) {
      break;
    }
    const auto& type_queue_size = eviction_queue_sizes[type_idx];
    queue_index += type_queue_size;
  }
  const auto& queue_size =
      eviction_queue_sizes[static_cast<uint8_t>(handle_buffer_type) - 1];
  auto eviction_queue_idx = handle.GetEvictionQueueIndex();
  if (eviction_queue_idx < queue_size) {
    queue_index += queue_size - eviction_queue_idx - 1;
  }
  D_ASSERT(queues[queue_index]->file_buffer_type == handle_buffer_type);
  return *queues[queue_index];
}

void BufferPool::IncrementDeadNodes(const BlockHandle& handle) {
  GetEvictionQueueForBlockHandle(handle).IncrementDeadNodes();
}

void BufferPool::UpdateUsedMemory(MemoryTag tag, int64_t size) {
  memory_usage.UpdateUsedMemory(tag, size);
}

idx_t BufferPool::GetUsedMemory() const {
  return memory_usage.GetUsedMemory(MemoryUsageCaches::FLUSH);
}

idx_t BufferPool::GetMaxMemory() const { return maximum_memory; }

idx_t BufferPool::GetQueryMaxMemory() const { return GetMaxMemory(); }

BufferPool::EvictionResult BufferPool::EvictBlocks(
    MemoryTag tag, idx_t extra_memory, idx_t memory_limit,
    unique_ptr<FileBuffer>* buffer) {
  for (auto& queue : queues) {
    auto block_result =
        EvictBlocksInternal(*queue, tag, extra_memory, memory_limit, buffer);
    if (block_result.success || RefersToSameObject(*queue, *queues.back())) {
      return block_result;
    }
  }
  throw InternalException(
      "Exited BufferPool::EvictBlocksInternal without obtaining "
      "BufferPool::EvictionResult");
}

BufferPool::EvictionResult BufferPool::EvictBlocksInternal(
    EvictionQueue& queue, MemoryTag tag, idx_t extra_memory, idx_t memory_limit,
    unique_ptr<FileBuffer>* buffer) {
  TempBufferPoolReservation r(tag, *this, extra_memory);
  bool found = false;

  if (memory_usage.GetUsedMemory(MemoryUsageCaches::NO_FLUSH) <= memory_limit) {
    return {true, std::move(r)};
  }

  queue.IterateUnloadableBlocks([&](BufferEvictionNode&,
                                    const shared_ptr<BlockHandle>& handle,
                                    BlockLock& lock) {
    if (buffer && handle->GetBuffer(lock)->AllocSize() == extra_memory) {
      *buffer = handle->UnloadAndTakeBlock(lock);
      found = true;
      return false;
    }
    handle->Unload(lock);
    if (memory_usage.GetUsedMemory(MemoryUsageCaches::NO_FLUSH) <=
        memory_limit) {
      found = true;
      return false;
    }
    return true;
  });

  if (!found) {
    r.Resize(0);
  }
  return {found, std::move(r)};
}

idx_t BufferPool::PurgeAgedBlocks(uint32_t max_age_sec) {
  int64_t now = std::chrono::time_point_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now())
                    .time_since_epoch()
                    .count();
  int64_t limit = now - (static_cast<int64_t>(max_age_sec) * 1000);
  idx_t purged_bytes = 0;
  for (auto& queue : queues) {
    purged_bytes += PurgeAgedBlocksInternal(*queue, max_age_sec, now, limit);
  }
  return purged_bytes;
}

idx_t BufferPool::PurgeAgedBlocksInternal(EvictionQueue& queue,
                                          uint32_t /*max_age_sec*/, int64_t now,
                                          int64_t limit) {
  idx_t purged_bytes = 0;
  queue.IterateUnloadableBlocks([&](BufferEvictionNode&,
                                    const shared_ptr<BlockHandle>& handle,
                                    BlockLock& lock) {
    auto lru_timestamp_msec = handle->GetLRUTimestamp();
    bool is_fresh = lru_timestamp_msec >= limit && lru_timestamp_msec <= now;
    purged_bytes += handle->GetMemoryUsage();
    handle->Unload(lock);
    return !is_fresh;
  });
  return purged_bytes;
}

void BufferPool::PurgeQueue(const BlockHandle& block) {
  GetEvictionQueueForBlockHandle(block).Purge();
}

void BufferPool::SetLimit(idx_t limit, const char* exception_postscript) {
  lock_guard<mutex> l_lock(limit_lock);
  if (!EvictBlocks(MemoryTag::EXTENSION, 0, limit).success) {
    throw OutOfMemoryException(
        "Failed to change memory limit to %llu: could not free up enough "
        "memory for the new limit%s",
        limit, exception_postscript);
  }
  idx_t old_limit = maximum_memory;
  maximum_memory = limit;
  if (!EvictBlocks(MemoryTag::EXTENSION, 0, limit).success) {
    maximum_memory = old_limit;
    throw OutOfMemoryException(
        "Failed to change memory limit to %llu: could not free up enough "
        "memory for the new limit%s",
        limit, exception_postscript);
  }
}

BufferPool::MemoryUsage::MemoryUsage() {
  for (auto& v : memory_usage) {
    v = 0;
  }
  for (auto& cache : memory_usage_caches) {
    for (auto& v : cache) {
      v = 0;
    }
  }
}

void BufferPool::MemoryUsage::UpdateUsedMemory(MemoryTag tag, int64_t size) {
  auto tag_idx = (idx_t)tag;
  if ((idx_t)AbsValue(size) < MEMORY_USAGE_CACHE_THRESHOLD) {
    // Use a simple hash of the thread id for cache slot selection.
    auto cache_idx = std::hash<std::thread::id>{}(std::this_thread::get_id()) %
                     MEMORY_USAGE_CACHE_COUNT;
    auto& cache = memory_usage_caches[cache_idx];
    auto new_tag_size =
        cache[tag_idx].fetch_add(size, std::memory_order_relaxed) + size;
    if ((idx_t)AbsValue(new_tag_size) >= MEMORY_USAGE_CACHE_THRESHOLD) {
      auto tag_size = cache[tag_idx].exchange(0, std::memory_order_relaxed);
      memory_usage[tag_idx].fetch_add(tag_size, std::memory_order_relaxed);
    }
    auto new_total_size = cache[TOTAL_MEMORY_USAGE_INDEX].fetch_add(
                              size, std::memory_order_relaxed) +
                          size;
    if ((idx_t)AbsValue(new_total_size) >= MEMORY_USAGE_CACHE_THRESHOLD) {
      auto total_size = cache[TOTAL_MEMORY_USAGE_INDEX].exchange(
          0, std::memory_order_relaxed);
      memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(
          total_size, std::memory_order_relaxed);
    }
  } else {
    memory_usage[tag_idx].fetch_add(size, std::memory_order_relaxed);
    memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size,
                                                     std::memory_order_relaxed);
  }
}

}  // namespace duckdb
