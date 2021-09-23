//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 22.09.21.
//
#ifndef QLEVER_COMPRESSIONSERIALIZER_H
#define QLEVER_COMPRESSIONSERIALIZER_H

#include <vector>
#include <future>
#include "../CompressionUsingZstd/ZstdWrapper.h"
#include "./FileSerializer.h"
#include "./SerializeVector.h"
#include "../TaskQueue.h"


namespace ad_utility {
namespace serialization {

struct CompressedBlockAndOriginalSize {
  size_t m_originalNumBytes;
  std::vector<char> m_compressedBlock;
};

template <typename Serializer>
void serialize(Serializer& serializer, CompressedBlockAndOriginalSize& c) {
  serializer | c.m_originalNumBytes;
  serializer | c.m_compressedBlock;
}

template <class UnderlyingSerializer>
class CompressedWriteSerializer {
 private:
  const size_t m_blocksize;
  UnderlyingSerializer m_underlyingSerializer;
  ByteBufferWriteSerializer m_buffer;
  bool m_isFinished = false;

 public:
  constexpr static bool IsWriteSerializer = true;

  template <typename... Args>
  CompressedWriteSerializer(size_t blocksize, Args&&... args)
      : m_blocksize{blocksize},
        m_underlyingSerializer{std::forward<Args>(args)...} {}

  void serializeBytes(const char* bytePtr, size_t numBytes) {
    if (m_buffer.numBytes() + numBytes > 1.5 * m_blocksize) {
      compressAndWriteBuffer();
    }
    m_buffer.serializeBytes(bytePtr, numBytes);

    if (m_buffer.numBytes() > 0.8 * m_blocksize) {
      compressAndWriteBuffer();
    }
  }
  void finish() {
    if (!m_isFinished) {
      compressAndWriteBuffer();
      m_underlyingSerializer.close();
      m_isFinished = true;
    }
  }

  ~CompressedWriteSerializer() { finish(); }

 private:
  void compressAndWriteBuffer() {
    auto compressedBlock = ZstdWrapper::compress(
        const_cast<char*>(m_buffer.data().data()), m_buffer.numBytes());
    CompressedBlockAndOriginalSize c{m_buffer.numBytes(),
                                     std::move(compressedBlock)};
    m_underlyingSerializer << c;
    m_buffer.clear();
  }
};

template <class UnderlyingSerializer>
class CompressedReadSerializer {
 private:
  UnderlyingSerializer m_underlyingSerializer;
  ByteBufferReadSerializer m_buffer{{}};
  std::future<std::optional<std::vector<char>>> m_nextBlockFuture;
  mutable bool m_isExhausted = false;

 public:
  constexpr static bool IsWriteSerializer = false;

  /*
  CompressedReadSerializer(const CompressedReadSerializer&) = delete;
  CompressedReadSerializer(CompressedReadSerializer&&) = delete;
  CompressedReadSerializer& operator=(const CompressedReadSerializer&) = delete;
  CompressedReadSerializer& operator=(CompressedReadSerializer&&) = delete;
   */

  template <typename... Args>
  CompressedReadSerializer(Args&&... args)
      : m_underlyingSerializer{std::forward<Args>(args)...} {}

  bool isExhausted() {
    return false;
    if (m_isExhausted) {
      return true;
    }
    if (!m_buffer.isExhausted()) {
      return false;
    }
    tryToFillBuffer();
    if (m_buffer.isExhausted()) {
      m_isExhausted = true;
      return true;
    }
    return false;
  }

  void serializeBytes(char* bytePtr, size_t numBytes) {
    if (m_buffer.isExhausted()) {
      tryToFillBuffer();
      if (m_buffer.isExhausted()) {
        throw SerializationException("Read past the end of a compressedSerializer");
      }
      AD_CHECK(!m_buffer.isExhausted())
    }
    m_buffer.serializeBytes(bytePtr, numBytes);
  }

 private:
  std::optional<std::vector<char>> getAndDecompressBlock() {
    CompressedBlockAndOriginalSize c;
    try {
      m_underlyingSerializer >> c;
    } catch (const SerializationException&) {
      return std::nullopt;
      m_isExhausted = true;
    }
    auto result = ZstdWrapper::decompress<char>(c.m_compressedBlock.data(),
                                                c.m_compressedBlock.size(),
                                                c.m_originalNumBytes);
    return result;
  }

  void tryToFillBuffer() {
    m_buffer.clear();
    while (!m_nextBlockFuture.valid()) {
      m_nextBlockFuture =
          std::async(std::launch::async,
                     &CompressedReadSerializer::getAndDecompressBlock, this);
    }
    const auto& nextBlockOptional = m_nextBlockFuture.get();
    if (nextBlockOptional) {
      m_buffer.reset(std::move(*nextBlockOptional));
    }
    m_nextBlockFuture =
        std::async(std::launch::async,
                   &CompressedReadSerializer::getAndDecompressBlock, this);
    // LOG(INFO) << "Done filling buffer" << std::endl;
  }
};


}
}

#endif  // QLEVER_COMPRESSIONSERIALIZER_H
