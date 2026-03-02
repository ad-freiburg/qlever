// Copyright 2024, DuckDB contributors. Adapted for QLever.

#include "storage/standard_buffer_manager.h"

#include <map>

#include "storage/buffer/buffer_pool.h"
#include "storage/compat/duckdb_exceptions.h"
#include "storage/in_memory_block_manager.h"

namespace duckdb {

unique_ptr<FileBuffer> StandardBufferManager::ConstructManagedBuffer(
    idx_t size, unique_ptr<FileBuffer>&& source, FileBufferType type) {
  unique_ptr<FileBuffer> result;
  if (type == FileBufferType::BLOCK) {
    throw InternalException(
        "ConstructManagedBuffer cannot be used to construct blocks");
  }
  if (source) {
    auto tmp = std::move(source);
    D_ASSERT(tmp->AllocSize() == BufferManager::GetAllocSize(size));
    result = make_uniq<FileBuffer>(*tmp, type);
  } else {
    result = make_uniq<FileBuffer>(buffer_allocator, type, size);
  }
  result->Initialize(config_.debug_initialize);
  return result;
}

void StandardBufferManager::SetTemporaryDirectory(const string& new_dir) {
  temporary_directory_ = new_dir;
}

StandardBufferManager::StandardBufferManager(BufferPool& pool,
                                             StorageConfig config)
    : BufferManager(),
      buffer_pool(pool),
      config_(std::move(config)),
      temporary_id(MAXIMUM_BLOCK) {
  temp_block_manager =
      make_uniq<InMemoryBlockManager>(*this, DEFAULT_BLOCK_ALLOC_SIZE);
  temporary_directory_ = config_.temporary_directory;
  for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
    evicted_data_per_tag[i] = 0;
  }
}

StandardBufferManager::~StandardBufferManager() {}

BufferPool& StandardBufferManager::GetBufferPool() const { return buffer_pool; }

idx_t StandardBufferManager::GetUsedMemory() const {
  return buffer_pool.GetUsedMemory();
}

idx_t StandardBufferManager::GetMaxMemory() const {
  return buffer_pool.GetMaxMemory();
}

idx_t StandardBufferManager::GetUsedSwap() {
  // Simplified: no swap tracking in standalone version.
  return 0;
}

optional_idx StandardBufferManager::GetMaxSwap() const {
  return optional_idx();
}

idx_t StandardBufferManager::GetBlockAllocSize() const {
  return temp_block_manager->GetBlockAllocSize();
}

idx_t StandardBufferManager::GetBlockSize() const {
  return temp_block_manager->GetBlockSize();
}

template <typename... ARGS>
TempBufferPoolReservation StandardBufferManager::EvictBlocksOrThrow(
    MemoryTag tag, idx_t memory_delta, unique_ptr<FileBuffer>* buffer,
    ARGS... args) {
  auto r = buffer_pool.EvictBlocks(tag, memory_delta,
                                   buffer_pool.maximum_memory, buffer);
  if (!r.success) {
    throw OutOfMemoryException(args...);
  }
  return std::move(r.reservation);
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterSmallMemory(
    MemoryTag tag, const idx_t size) {
  D_ASSERT(size < GetBlockSize());
  auto reservation = EvictBlocksOrThrow(
      tag, size, nullptr, "could not allocate block of size %llu", size);

  auto buffer =
      ConstructManagedBuffer(size, nullptr, FileBufferType::TINY_BUFFER);

  auto result = make_shared_ptr<BlockHandle>(
      *temp_block_manager, ++temporary_id, tag, std::move(buffer),
      DestroyBufferUpon::BLOCK, size, std::move(reservation));
  return result;
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterMemory(
    MemoryTag tag, idx_t block_size, bool can_destroy) {
  auto alloc_size = GetAllocSize(block_size);

  unique_ptr<FileBuffer> reusable_buffer;
  auto res =
      EvictBlocksOrThrow(tag, alloc_size, &reusable_buffer,
                         "could not allocate block of size %llu", alloc_size);

  auto buffer = ConstructManagedBuffer(block_size, std::move(reusable_buffer));
  DestroyBufferUpon destroy_buffer_upon =
      can_destroy ? DestroyBufferUpon::EVICTION : DestroyBufferUpon::BLOCK;
  return make_shared_ptr<BlockHandle>(*temp_block_manager, ++temporary_id, tag,
                                      std::move(buffer), destroy_buffer_upon,
                                      alloc_size, std::move(res));
}

BufferHandle StandardBufferManager::Allocate(MemoryTag tag, idx_t block_size,
                                             bool can_destroy) {
  auto block = RegisterMemory(tag, block_size, can_destroy);
  return Pin(block);
}

void StandardBufferManager::ReAllocate(shared_ptr<BlockHandle>& handle,
                                       idx_t block_size) {
  D_ASSERT(block_size >= GetBlockSize());
  auto lock = handle->GetLock();

  auto handle_memory_usage = handle->GetMemoryUsage();
  D_ASSERT(handle->GetState() == BlockState::BLOCK_LOADED);
  D_ASSERT(handle_memory_usage == handle->GetBuffer(lock)->AllocSize());
  D_ASSERT(handle_memory_usage == handle->GetMemoryCharge(lock).size);

  auto req = handle->GetBuffer(lock)->CalculateMemory(block_size);
  int64_t memory_delta = NumericCast<int64_t>(req.alloc_size) -
                         NumericCast<int64_t>(handle_memory_usage);

  if (memory_delta == 0) {
    return;
  } else if (memory_delta > 0) {
    lock.unlock();
    auto reservation = EvictBlocksOrThrow(
        handle->GetMemoryTag(), NumericCast<idx_t>(memory_delta), nullptr,
        "failed to resize block from %llu to %llu", handle_memory_usage,
        req.alloc_size);
    lock.lock();
    handle->MergeMemoryReservation(lock, std::move(reservation));
  } else {
    handle->ResizeMemory(lock, req.alloc_size);
  }
  handle->ResizeBuffer(lock, block_size, memory_delta);
}

void StandardBufferManager::Prefetch(vector<shared_ptr<BlockHandle>>& handles) {
  // Simplified synchronous prefetch.
  for (auto& handle : handles) {
    if (handle->GetState() != BlockState::BLOCK_LOADED) {
      auto buf = Pin(handle);
      // Immediately unpin — the data stays loaded until evicted.
    }
  }
}

BufferHandle StandardBufferManager::Pin(shared_ptr<BlockHandle>& handle) {
  BufferHandle buf;
  idx_t required_memory;
  {
    auto lock = handle->GetLock();
    if (handle->GetState() == BlockState::BLOCK_LOADED) {
      buf = handle->Load();
    }
    required_memory = handle->GetMemoryUsage();
  }

  if (buf.IsValid()) {
    return buf;
  } else {
    unique_ptr<FileBuffer> reusable_buffer;
    auto reservation = EvictBlocksOrThrow(
        handle->GetMemoryTag(), required_memory, &reusable_buffer,
        "failed to pin block of size %llu", required_memory);

    auto lock = handle->GetLock();
    if (handle->GetState() == BlockState::BLOCK_LOADED) {
      reservation.Resize(0);
      buf = handle->Load();
    } else {
      D_ASSERT(handle->Readers() == 0);
      buf = handle->Load(std::move(reusable_buffer));
      auto& memory_charge = handle->GetMemoryCharge(lock);
      memory_charge = std::move(reservation);
      int64_t delta =
          NumericCast<int64_t>(handle->GetBuffer(lock)->AllocSize()) -
          NumericCast<int64_t>(handle->GetMemoryUsage());
      if (delta) {
        handle->ChangeMemoryUsage(lock, delta);
      }
      D_ASSERT(handle->GetMemoryUsage() ==
               handle->GetBuffer(lock)->AllocSize());
    }
  }

  D_ASSERT(buf.IsValid());
  return buf;
}

void StandardBufferManager::PurgeQueue(const BlockHandle& handle) {
  buffer_pool.PurgeQueue(handle);
}

void StandardBufferManager::AddToEvictionQueue(
    shared_ptr<BlockHandle>& handle) {
  buffer_pool.AddToEvictionQueue(handle);
}

void StandardBufferManager::Unpin(shared_ptr<BlockHandle>& handle) {
  bool purge = false;
  {
    auto lock = handle->GetLock();
    if (!handle->GetBuffer(lock) ||
        handle->GetBufferType() == FileBufferType::TINY_BUFFER) {
      return;
    }
    D_ASSERT(handle->Readers() > 0);
    auto new_readers = handle->DecrementReaders();
    if (new_readers == 0) {
      if (handle->MustAddToEvictionQueue()) {
        purge = buffer_pool.AddToEvictionQueue(handle);
      } else {
        handle->Unload(lock);
      }
    }
  }
  if (purge) {
    PurgeQueue(*handle);
  }
}

void StandardBufferManager::SetMemoryLimit(idx_t limit) {
  buffer_pool.SetLimit(limit, InMemoryWarning());
}

void StandardBufferManager::SetSwapLimit(optional_idx /*limit*/) {
  // Simplified: no swap limit tracking.
}

vector<MemoryInformation> StandardBufferManager::GetMemoryUsageInfo() const {
  vector<MemoryInformation> result;
  for (idx_t k = 0; k < MEMORY_TAG_COUNT; k++) {
    MemoryInformation info;
    info.tag = MemoryTag(k);
    info.size = buffer_pool.memory_usage.GetUsedMemory(
        MemoryTag(k), BufferPool::MemoryUsageCaches::FLUSH);
    info.evicted_data = evicted_data_per_tag[k].load();
    result.push_back(info);
  }
  return result;
}

void StandardBufferManager::WriteTemporaryBuffer(MemoryTag tag,
                                                 block_id_t block_id,
                                                 FileBuffer& buffer) {
  if (temporary_directory_.empty()) {
    throw InvalidInputException(
        "Out-of-memory: cannot write buffer because no temporary directory is "
        "specified!");
  }
  // Simplified temporary file writing using direct POSIX I/O.
  auto path = temporary_directory_ + "/qlever_temp_block-" +
              to_string(block_id) + ".block";
  evicted_data_per_tag[uint8_t(tag)] += buffer.AllocSize();
  auto handle = OpenFile(path, "w");
  idx_t buf_size = buffer.size;
  handle->Write(reinterpret_cast<data_ptr_t>(&buf_size), sizeof(idx_t), 0);
  buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> StandardBufferManager::ReadTemporaryBuffer(
    MemoryTag /*tag*/, BlockHandle& block,
    unique_ptr<FileBuffer> reusable_buffer) {
  D_ASSERT(!temporary_directory_.empty());
  auto id = block.BlockId();
  auto path =
      temporary_directory_ + "/qlever_temp_block-" + to_string(id) + ".block";
  auto handle = OpenFile(path, "r");

  idx_t block_size;
  handle->Read(reinterpret_cast<data_ptr_t>(&block_size), sizeof(idx_t), 0);

  auto buffer = ConstructManagedBuffer(block_size, std::move(reusable_buffer));
  buffer->Read(*handle, sizeof(idx_t));
  handle.reset();

  // Delete the temporary file.
  DeleteTemporaryFile(block);
  return buffer;
}

void StandardBufferManager::DeleteTemporaryFile(BlockHandle& block) {
  auto id = block.BlockId();
  if (temporary_directory_.empty()) {
    return;
  }
  auto path =
      temporary_directory_ + "/qlever_temp_block-" + to_string(id) + ".block";
  evicted_data_per_tag[uint8_t(block.GetMemoryTag())] -= block.GetMemoryUsage();
  std::remove(path.c_str());
}

bool StandardBufferManager::HasTemporaryDirectory() const {
  return !temporary_directory_.empty();
}

vector<TemporaryFileInformation> StandardBufferManager::GetTemporaryFiles() {
  return {};
}

const char* StandardBufferManager::InMemoryWarning() {
  if (!temporary_directory_.empty()) {
    return "";
  }
  return "\nNo temporary directory is specified. Unused blocks cannot be "
         "offloaded to disk.";
}

void StandardBufferManager::ReserveMemory(idx_t size) {
  if (size == 0) return;
  auto reservation =
      EvictBlocksOrThrow(MemoryTag::EXTENSION, size, nullptr,
                         "failed to reserve memory data of size %llu", size);
  reservation.size = 0;
}

void StandardBufferManager::FreeReservedMemory(idx_t size) {
  if (size == 0) return;
  buffer_pool.memory_usage.UpdateUsedMemory(MemoryTag::EXTENSION,
                                            -(int64_t)size);
}

Allocator& StandardBufferManager::GetBufferAllocator() {
  return buffer_allocator;
}

}  // namespace duckdb
