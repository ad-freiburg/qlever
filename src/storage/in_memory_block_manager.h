// Copyright 2024, DuckDB contributors. Adapted for QLever.
// InMemoryBlockManager throws on all I/O operations.

#pragma once

#include "storage/block_manager.h"
#include "storage/compat/duckdb_exceptions.h"
#include "storage/compat/duckdb_types.h"

namespace duckdb {

class InMemoryBlockManager : public BlockManager {
 public:
  using BlockManager::BlockManager;

  unique_ptr<Block> ConvertBlock(block_id_t, FileBuffer&) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - ConvertBlock!");
  }
  unique_ptr<Block> CreateBlock(block_id_t, FileBuffer*) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - CreateBlock!");
  }
  block_id_t GetFreeBlockId() override {
    throw InternalException(
        "Cannot perform IO in in-memory database - GetFreeBlockId!");
  }
  block_id_t PeekFreeBlockId() override {
    throw InternalException(
        "Cannot perform IO in in-memory database - PeekFreeBlockId!");
  }
  bool IsRootBlock(MetaBlockPointer) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - IsRootBlock!");
  }
  void MarkBlockAsFree(block_id_t) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - MarkBlockAsFree!");
  }
  void MarkBlockAsUsed(block_id_t) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - MarkBlockAsUsed!");
  }
  void MarkBlockAsModified(block_id_t) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - MarkBlockAsModified!");
  }
  void IncreaseBlockReferenceCount(block_id_t) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - "
        "IncreaseBlockReferenceCount!");
  }
  idx_t GetMetaBlock() override {
    throw InternalException(
        "Cannot perform IO in in-memory database - GetMetaBlock!");
  }
  void Read(Block&) override {
    throw InternalException("Cannot perform IO in in-memory database - Read!");
  }
  void ReadBlocks(FileBuffer&, block_id_t, idx_t) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - ReadBlocks!");
  }
  void Write(FileBuffer&, block_id_t) override {
    throw InternalException("Cannot perform IO in in-memory database - Write!");
  }
  void WriteHeader(DatabaseHeader) override {
    throw InternalException(
        "Cannot perform IO in in-memory database - WriteHeader!");
  }
  bool InMemory() override { return true; }
  void FileSync() override {
    throw InternalException(
        "Cannot perform IO in in-memory database - FileSync!");
  }
  idx_t TotalBlocks() override {
    throw InternalException(
        "Cannot perform IO in in-memory database - TotalBlocks!");
  }
  idx_t FreeBlocks() override {
    throw InternalException(
        "Cannot perform IO in in-memory database - FreeBlocks!");
  }
};

}  // namespace duckdb
