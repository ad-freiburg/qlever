// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_HTTP_ZEROCOPYHTTPSENDER_H
#define QLEVER_SRC_UTIL_HTTP_ZEROCOPYHTTPSENDER_H

#include <absl/strings/str_cat.h>
#include <poll.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/ZeroCopySocketSender.h"
#include "util/http/beast.h"
#include "util/http/streamable_body.h"

namespace ad_utility::httpUtils {

// Per-session configuration for the zero-copy export response path. Kept
// small on purpose: one sender lives per HTTP session, so the pool (8 slots
// of 64 KB = 512 KB) bounds the extra memory per connection.
struct ZeroCopyHttpSenderConfig {
  size_t ringEntries = 32;
  size_t numBuffers = 8;
  size_t bufferSizeBytes = 64 * 1024;
  bool useRegisteredBuffers = true;
  bool useZeroCopy = true;

  [[nodiscard]] ZeroCopySenderConfig toSenderConfig() const {
    return ZeroCopySenderConfig{ringEntries,     numBuffers,
                                bufferSizeBytes, useRegisteredBuffers,
                                useZeroCopy,     0};
  }
};

// Detect `http::response<streamable_body>` (any fields type) for the
// send-path dispatch in `HttpServer`.
template <typename T>
struct IsStreamableBodyResponse : std::false_type {};
template <typename Fields>
struct IsStreamableBodyResponse<
    boost::beast::http::response<httpStreams::streamable_body, Fields>>
    : std::true_type {};

// Copy `size` bytes at `data` into the sender's registered pool slots and
// enqueue them as zero-copy (or synchronous fallback) sends on `sockfd`,
// splitting across slots as needed. Empty input is a no-op. Submissions are
// batched by the caller via `submit()`; call `flushAndDrainAll()` once the
// full response was enqueued.
inline void sendBytesViaZeroCopySocket(int sockfd, ZeroCopySocketSender& sender,
                                       const char* data, size_t size) {
  AD_CONTRACT_CHECK(sockfd >= 0);
  while (size > 0) {
    uint32_t slot = sender.acquireBuffer();
    auto span = sender.getSlotSpan(slot);
    const size_t n = std::min(size, span.size());
    std::memcpy(span.data(), data, n);
    sender.sendChunk(sockfd, slot, n);
    data += n;
    size -= n;
  }
}

// Overload for contiguous string-like views.
inline void sendBytesViaZeroCopySocket(int sockfd, ZeroCopySocketSender& sender,
                                       std::string_view data) {
  if (!data.empty()) {
    sendBytesViaZeroCopySocket(sockfd, sender, data.data(), data.size());
  }
}

// Block until `sockfd` is writable (for the synchronous fallback path, which
// runs on sockets that Boost.Asio manages in non-blocking mode). Throws on
// timeout or on poll errors.
inline void waitForSocketWritable(int sockfd) {
  AD_CONTRACT_CHECK(sockfd >= 0);
  pollfd pfd{sockfd, POLLOUT, 0};
  // Bounded wait (matching the session's read timeout): a stalled peer
  // must surface as an error instead of stalling the session coroutine
  // forever. The session owns this socket exclusively while a response is
  // being written.
  while (true) {
    int ret = ::poll(&pfd, 1, kZeroCopyPeerStallTimeoutSeconds * 1000);
    if (ret == 0) {
      AD_THROW("timed out waiting for socket writability (peer stalled)");
    }
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      AD_THROW("poll for socket writability failed");
    }
    if (pfd.revents == 0) {
      // Spurious wakeup, keep waiting within the timeout above.
      continue;
    }
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
      AD_THROW("socket not writable (peer closed)");
    }
    if (pfd.revents & POLLOUT) {
      return;
    }
  }
}

// Send a chunked `streamable_body` response over `stream`, transmitting the
// body bytes via `sender` (Linux `IORING_OP_SEND_ZC` when available,
// synchronous `send()` otherwise, byte-identical in both cases). The response
// head is written with Boost.Beast first; the body generator is then driven
// chunk by chunk, emitting explicit HTTP/1.1 chunk framing around each
// non-empty chunk. Only valid when `response.chunked()` is true; callers must
// use the regular `http::async_write` path otherwise (e.g. HTTP/1.0 without
// chunked encoding). Generator exceptions propagate like on the Beast path,
// where they surface as truncated responses; the session is then closed.
// Before propagating, enqueued data is drained on a best-effort basis, and
// the session discards the sender, so a reused keep-alive connection never
// inherits pinned buffers.
template <typename Stream>
boost::asio::awaitable<void> asyncWriteStreamableBodyZeroCopy(
    Stream& stream,
    boost::beast::http::response<httpStreams::streamable_body>& response,
    ZeroCopySocketSender& sender) {
  namespace http = boost::beast::http;
  AD_CONTRACT_CHECK(response.chunked());
  // Serialize the head (status line + headers, no body access yet).
  http::response_serializer<httpStreams::streamable_body> serializer{response};
  co_await http::async_write_header(stream, serializer,
                                    boost::asio::use_awaitable);
  // From here on the body generator is owned by this loop; the serializer is
  // no longer used.
  auto generator = std::move(response.body());
  const int sockfd = stream.socket().native_handle();
  // Without an io_uring ring the sender uses synchronous `send()` on a
  // non-blocking socket, so wait for writability before each batch.
  const bool useRing = sender.isRingInitialized();
  size_t chunksSinceSubmit = 0;
  constexpr size_t kSubmitInterval = 8;
  auto submitIfDue = [&]() {
    if (++chunksSinceSubmit >= kSubmitInterval) {
      sender.submit();
      chunksSinceSubmit = 0;
    }
  };
  // On any failure below (generator exception, poll/send error, drain error)
  // make a best-effort attempt to drain what was enqueued, so the sender is
  // left with no pinned buffers before the exception propagates to the
  // session (which discards the sender, see `HttpServer::session`).
  try {
    for (auto&& chunk : generator) {
      if (chunk.empty()) {
        // An empty chunk would serialize as the terminating `0` chunk, so it
        // must never be emitted mid-body.
        continue;
      }
      if (!useRing) {
        waitForSocketWritable(sockfd);
      }
      // Chunk-size line in hexadecimal, without leading `0x`.
      std::string header = absl::StrCat(absl::Hex(chunk.size()), "\r\n");
      sendBytesViaZeroCopySocket(sockfd, sender, header);
      sendBytesViaZeroCopySocket(sockfd, sender, chunk);
      static constexpr std::string_view kCrlf = "\r\n";
      sendBytesViaZeroCopySocket(sockfd, sender, kCrlf);
      submitIfDue();
    }
    static constexpr std::string_view kTerminator = "0\r\n\r\n";
    if (!useRing) {
      waitForSocketWritable(sockfd);
    }
    sendBytesViaZeroCopySocket(sockfd, sender, kTerminator);
    sender.submit();
    sender.flushAndDrainAll();
  } catch (...) {
    try {
      sender.submit();
      sender.flushAndDrainAll();
    } catch (...) {
      // Best-effort only: the original exception propagates.
    }
    throw;
  }
  co_return;
}

}  // namespace ad_utility::httpUtils

#endif  // QLEVER_SRC_UTIL_HTTP_ZEROCOPYHTTPSENDER_H
