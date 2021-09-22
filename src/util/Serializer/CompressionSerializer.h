//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 22.09.21.
//
#include <vector>
#include <future>
#include "../CompressionUsingZstd/ZstdWrapper.h"
#include "FileSerializer.h"


#ifndef QLEVER_COMPRESSIONSERIALIZER_H
#define QLEVER_COMPRESSIONSERIALIZER_H
namespace ad_utility::serialization {

struct CompressedBlockAndOriginalSize {
  size_t m_originalNumBytes;
  std::vector<char> m_compressedBlock;
};

template<typename Serializer>
void serialize(Serializer& serializer, CompressedBlockAndOriginalSize& c) {
  serializer | c.m_originalNumBytes;
  serializer | c.m_compressedBlock;
}

template<class UnderlyingSerializer>
class CompressedWriteSerializer {
 private:
  const size_t m_blocksize;
  UnderlyingSerializer m_underlyingSerializer;
  ByteBufferWriteSerializer m_buffer;
 public:
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
    compressAndWriteBuffer();
  }

  ~CompressedWriteSerializer() {
    close();
  }

 private:
  void compressAndWriteBuffer() {
    auto compressedBlock = ZstdWrapper::compress(m_buffer.data(), m_buffer.size());
    CompressedBlockAndOriginalSize c {m_buffer.size(), std::move(compressedBlock)};
    m_underlyingSerializer << c;
    m_buffer.clear();
  }
};

template<class UnderlyingSerializer>
class CompressedReadSerializer {
 private:
  UnderlyingSerializer m_underlyingSerializer;
  ByteBufferReadSerializer m_buffer;
  std::future<std::optional<std::vector<char>>> m_nextBlockFuture;
  bool m_isExhausted = false;

 public:
  void serializeBytes(const char* bytePtr, size_t numBytes) {
    if (m_buffer.numBytes() + numBytes > 1.5 * m_blocksize) {
      compressAndWriteBuffer();
    }
    m_buffer.serializeBytes(bytePtr, numBytes);

    if (m_buffer.numBytes() > 0.8 * m_blocksize) {
      compressAndWriteBuffer();
    }
  }

 private:
  std::optional<std::vector<char>> getAndDecompressBlock() {
    if (m_underlyingSerializer.isExhausted()) {
      return std::nullopt;
    }
    CompressedBlockAndOriginalSize c;
    m_underlyingSerializer >> c;
    return ZstdWrapper::decompress<char>(c.m_compressedBlock.data(), c.m_compressedBlock.size(), c.m_originalNumBytes);
  }

};

}
#endif  // QLEVER_COMPRESSIONSERIALIZER_H
