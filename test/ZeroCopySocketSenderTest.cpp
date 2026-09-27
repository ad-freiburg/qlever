// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstring>
#include <thread>
#include <utility>
#include <vector>

#include "util/ZeroCopySocketSender.h"
#include "util/jthread.h"

using namespace ad_utility;

// _____________________________________________________________________________
TEST(ZeroCopyBufferPoolTest, BasicAcquireAndRelease) {
  constexpr size_t numBuffers = 8;
  constexpr size_t bufferSize = 4096;  // 4KB

  ZeroCopyBufferPool pool(numBuffers, bufferSize);
  EXPECT_EQ(pool.numBuffers(), numBuffers);
  EXPECT_EQ(pool.bufferSizeBytes(), bufferSize);
  EXPECT_EQ(pool.availableSlots(), numBuffers);

  std::vector<uint32_t> acquired;
  for (size_t i = 0; i < numBuffers; ++i) {
    auto slot = pool.acquireSlot();
    ASSERT_TRUE(slot.has_value());
    EXPECT_TRUE(pool.isSlotInUse(slot.value()));
    acquired.push_back(slot.value());
  }

  EXPECT_EQ(pool.availableSlots(), 0u);
  EXPECT_FALSE(pool.acquireSlot().has_value());

  // Test span access
  for (uint32_t slot : acquired) {
    auto span = pool.getSlotSpan(slot);
    EXPECT_EQ(span.size(), bufferSize);
    std::memset(span.data(), 0xAB, span.size());
    EXPECT_EQ(static_cast<unsigned char>(span[0]), 0xAB);
  }

  // Release all slots
  for (uint32_t slot : acquired) {
    pool.releaseSlot(slot);
    EXPECT_FALSE(pool.isSlotInUse(slot));
  }

  EXPECT_EQ(pool.availableSlots(), numBuffers);
}

// _____________________________________________________________________________
TEST(ZeroCopySocketSenderTest, TransmissionOverTcpLoopback) {
  // IORING_OP_SEND_ZC requires TCP loopback: AF_UNIX socketpairs reject
  // zero-copy sends, so connect a TCP loopback pair.
  struct FdGuard {
    int fd = -1;
    explicit FdGuard(int fd_) : fd{fd_} {}
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    FdGuard(FdGuard&& other) noexcept : fd{std::exchange(other.fd, -1)} {}
    FdGuard& operator=(FdGuard&& other) noexcept {
      if (fd >= 0) {
        ::close(fd);
      }
      fd = std::exchange(other.fd, -1);
      return *this;
    }
    ~FdGuard() { reset(); }
    // Close the owned descriptor (if any) and release ownership, so the
    // destructor becomes a no-op for it.
    void reset() {
      if (fd >= 0) {
        ::close(fd);
        fd = -1;
      }
    }
  };
  FdGuard listenGuard{::socket(AF_INET, SOCK_STREAM, 0)};
  ASSERT_GE(listenGuard.fd, 0);
  int listenFd = listenGuard.fd;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(::bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
            0);
  socklen_t addrLen = sizeof(addr);
  ASSERT_EQ(
      ::getsockname(listenFd, reinterpret_cast<sockaddr*>(&addr), &addrLen), 0);
  ASSERT_EQ(::listen(listenFd, 1), 0);
  FdGuard sendGuard{::socket(AF_INET, SOCK_STREAM, 0)};
  ASSERT_GE(sendGuard.fd, 0);
  int sendFd = sendGuard.fd;
  ASSERT_EQ(::connect(sendFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
            0);
  FdGuard recvGuard{::accept(listenFd, nullptr, nullptr)};
  ASSERT_GE(recvGuard.fd, 0);
  int recvFd = recvGuard.fd;
  // The listening socket is no longer needed: hand its lifetime back to the
  // guard instead of closing the raw descriptor behind its back.
  listenGuard.reset();

  ZeroCopySenderConfig config;
  config.ringEntries = 16;
  config.numBuffers = 8;
  config.bufferSizeBytes = 4096;
  config.useRegisteredBuffers = true;
  config.useZeroCopy = true;

  ZeroCopySocketSender sender(config);
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);

  constexpr size_t numChunks = 20;
  constexpr size_t chunkSize = 1024;
  std::vector<char> expectedData(numChunks * chunkSize);
  for (size_t i = 0; i < expectedData.size(); ++i) {
    expectedData[i] = static_cast<char>((i * 37 + 13) % 256);
  }

  std::vector<char> receivedData(numChunks * chunkSize, 0);

  // Bound the receiver's blocking `::recv`, so an early test failure cannot
  // hang the joining thread below forever.
  struct timeval recvTimeout {};
  recvTimeout.tv_sec = 30;
  ASSERT_EQ(::setsockopt(recvFd, SOL_SOCKET, SO_RCVTIMEO, &recvTimeout,
                         sizeof(recvTimeout)),
            0);

  // Background thread to receive data. `JThread` joins on destruction, so an
  // early `ASSERT_*` return or a sender exception cannot leak a joinable
  // thread (which would call `std::terminate`).
  ad_utility::JThread receiverThread([&]() {
    size_t totalReceived = 0;
    while (totalReceived < expectedData.size()) {
      ssize_t bytes = ::recv(recvFd, receivedData.data() + totalReceived,
                             expectedData.size() - totalReceived, 0);
      if (bytes <= 0) {
        break;
      }
      totalReceived += static_cast<size_t>(bytes);
    }
  });

  // Sender thread loop
  for (size_t i = 0; i < numChunks; ++i) {
    uint32_t slot = sender.acquireBuffer();
    auto span = sender.getSlotSpan(slot);
    std::memcpy(span.data(), expectedData.data() + (i * chunkSize), chunkSize);
    sender.sendChunk(sendFd, slot, chunkSize);
  }

  sender.flushAndDrainAll();
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);
  EXPECT_EQ(sender.bufferPool().availableSlots(), config.numBuffers);

  receiverThread.join();

  EXPECT_EQ(receivedData, expectedData);
}
