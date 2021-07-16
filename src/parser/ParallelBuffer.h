// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once
#include <future>
#include <optional>
#include <string>
#include <vector>

#include "../util/File.h"

/**
 * @brief Abstract base class for certain input buffers.
 *
 * Is able to return a whole block of bytes from some kind of
 * input file/stream via the call to getNextBlock()
 * If It is computationally expensive to get the next bytes (e.g.
 * because they have to be decompressed) this can be done asynchronously
 * between two calls to getNextBlock.
 */
class ParallelBuffer {
 public:
  ParallelBuffer() = default;
  /**
   * @brief Specify the size of the blocks that are to be retrieved
   * @param blocksize  the blocksize (in bytes)
   */
  ParallelBuffer(size_t blocksize) : _blocksize(blocksize) {}
  virtual ~ParallelBuffer() = default;
  /**
   * @brief Open a file (in the sense of unix files, can also be a
   * (network/pipe) stream.
   * @param filename Name of the file which is to be opened.
   */
  virtual void open(const string& filename) = 0;
  /**
   * @brief Get (approximately) the next _blocksize bytes from the inut stream.
   *
   * Only valid after a call to open().
   *
   * @return The next bytes or std::nullopt to signal EOF.
   */
  virtual std::optional<std::vector<char>> getNextBlock() = 0;

 protected:
  size_t _blocksize = 100 * (2 << 20);
};

/**
 * @brief Read the bytes from a file/stream and just pass them directly without
 * any modification.
 *
 * The next block is also read in parallel in case we have a
 * really slow filesystem etc.
 */
class ParallelFileBuffer : public ParallelBuffer {
 public:
  ParallelFileBuffer() : ParallelBuffer(){};
  ParallelFileBuffer(size_t blocksize) : ParallelBuffer(blocksize) {}

  virtual void open(const string& filename) override {
    _file.open(filename, "r");
    _eof = false;
    _buf.resize(_blocksize);
    auto task = [&file = this->_file, bs = this->_blocksize,
                 &buf = this->_buf]() { return file.read(buf.data(), bs); };
    _fut = std::async(task);
  }

  // _____________________________________________________
  virtual std::optional<std::vector<char>> getNextBlock() override {
    if (_eof) {
      return std::nullopt;
    }

    AD_CHECK(_file.isOpen() && _fut.valid());
    auto numBytesRead = _fut.get();
    _buf.resize(numBytesRead);
    std::optional<std::vector<char>> ret = std::move(_buf);

    _buf.resize(_blocksize);
    auto task = [&file = this->_file, bs = this->_blocksize,
                 &buf = this->_buf]() { return file.read(buf.data(), bs); };
    _fut = std::async(task);

    return ret;
  }

 private:
  ad_utility::File _file;
  bool _eof = true;
  std::vector<char> _buf;
  std::future<size_t> _fut;
};
