# Storage / Buffer Management Layer

This directory contains a buffer management and block I/O layer adapted from
[DuckDB](https://github.com/duckdb/duckdb) (v1.2.1, commit `19864453b`).

## Origin

The code was copied from DuckDB's `src/storage/buffer/` and `src/storage/`
directories, then adapted to compile standalone within QLever. The `duckdb::`
namespace is kept to minimize diff from upstream.

## Architecture

```
BufferPool          – Manages memory limits and eviction queues.
  ↕
BufferManager       – Abstract interface for allocating/pinning/unpinning blocks.
  ↕
StandardBufferManager – Concrete implementation with temp-file spill support.
  ↕
BlockManager        – Abstract interface for reading/writing blocks to disk.
  ↕
SingleFileBlockManager – Persists blocks in a single file with dual-header
                         crash recovery.
  ↕
FileBuffer          – Aligned memory buffer with sector-aligned I/O.
```

### Key classes

- **`BufferPool`**: Tracks memory usage with per-thread caches, maintains
  multiple eviction queues (one per `FileBufferType`), and evicts blocks under
  memory pressure.
- **`BlockHandle`**: Reference-counted handle to a single block. Supports
  pin/unpin semantics via `BufferHandle` (RAII).
- **`BufferHandle`**: RAII wrapper returned by `Pin()`. Automatically unpins
  on destruction.
- **`StandardBufferManager`**: Allocates blocks, pins/unpins them, and spills
  to temporary files when memory is exhausted.
- **`SingleFileBlockManager`**: Manages block layout in a single file. Uses
  dual headers (h1/h2) for crash recovery with alternating writes.

## Compatibility shims (`compat/`)

DuckDB-specific dependencies are replaced by lightweight shims in `compat/`:

| Shim | Replaces |
|------|----------|
| `duckdb_types.h` | `duckdb/common/typedefs.hpp` — `idx_t`, `data_ptr_t`, smart pointer aliases |
| `duckdb_exceptions.h` | `duckdb/common/exception.hpp` — Exception classes as `std::runtime_error` subclasses |
| `duckdb_allocator.h` | `duckdb/common/allocator.hpp` — `std::aligned_alloc(4096, ...)` wrapper |
| `duckdb_file_system.h` | `duckdb/common/file_system.hpp` — POSIX `pread`/`pwrite`/`fsync` |
| `duckdb_mutex.h` | Mutex/lock aliases from `<mutex>` |
| `duckdb_atomic.h` | `std::atomic` aliases |
| `duckdb_assert.h` | `D_ASSERT` → `AD_CORRECTNESS_CHECK` |
| `duckdb_config.h` | `StorageConfig` struct |
| `duckdb_optional_idx.h` | `optional_idx` wrapper |
| `duckdb_optional_ptr.h` | Non-owning `optional_ptr<T>` |
| `duckdb_api.h` | `DUCKDB_API` macro (empty) |

## Simplifications from DuckDB

- Removed `DatabaseInstance`, `AttachedDatabase`, `ClientContext` dependencies.
- Removed `TemporaryMemoryManager`, `MetadataManager`, `ObjectCache`.
- Removed `TaskScheduler` usage (eviction queue selection uses thread ID hash).
- Replaced `moodycamel::ConcurrentQueue` with a simple mutex + deque queue.
- Removed checksums and serialization streams from `SingleFileBlockManager`.
- Simplified temporary file handling (direct POSIX I/O).

## Usage example

```cpp
#include "storage/buffer/buffer_pool.h"
#include "storage/standard_buffer_manager.h"
#include "storage/single_file_block_manager.h"

using namespace duckdb;

// Create a buffer pool with 1 GB memory limit.
BufferPool pool(1ULL << 30, false);

// Create a buffer manager.
StorageConfig config;
config.block_alloc_size = Storage::DEFAULT_BLOCK_ALLOC_SIZE;
StandardBufferManager buffer_manager(pool, config);

// Allocate a buffer block.
auto handle = buffer_manager.Allocate(MemoryTag::BASE_TABLE,
                                       pool.GetMaxMemory());
auto& buf = handle.GetFileBuffer();
memset(buf.buffer, 42, buf.FileBufferSize());
```
