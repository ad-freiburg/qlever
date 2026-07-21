// Copyright 2021-2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_HTTPSERVER_H
#define QLEVER_HTTPSERVER_H

#include <absl/cleanup/cleanup.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <thread>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/http/HttpUtils.h"
#include "util/http/beast.h"
#include "util/http/websocket/WebSocketSession.h"
#include "util/jthread.h"
namespace beast = boost::beast;    // from <boost/beast.hpp>
namespace http = beast::http;      // from <boost/beast/http.hpp>
namespace net = boost::asio;       // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;  // from <boost/asio/ip/tcp.hpp>

// Including the `RuntimeParameters` header is expensive. Move functions that
// require it into an implementation file.
ad_utility::MemorySize getRequestBodyLimit();

// Selects whether an HttpServer reads the full request body into memory before
// calling the handler (Eager) or streams it to the handler chunk by chunk
// while sending the response (Lazy).
enum class BodyReadMode { Eager, Lazy };

// A simple `HttpServer`, based on Boost::Beast. It can be configured via
// the mandatory `HttpHandler` parameter.
//
// `HttpHandler` is a callable returning `net::awaitable<void>`. Its signature
// depends on `bodyReadMode`:
//
// - `BodyReadMode::Eager`: `handler(http::request<http::string_body>, send)`.
//   The full request body is read into memory before the handler is called.
//
// - `BodyReadMode::Lazy`: `handler(http::request<http::empty_body>, send,
//   bodyGetter)`. Only headers are read before the handler is called.
//   `bodyGetter` is a callable with signature `() ->
//   net::awaitable<std::optional<std::string_view>>`. Each `co_await
//   bodyGetter()` reads the next body chunk; the returned view is valid until
//   the next call. `co_await bodyGetter()` returns `std::nullopt` when the
//   body is fully consumed and throws on network errors.
//
// In both modes `send` is a callable that takes a `http::message` and returns
// `net::awaitable<void>`; the handler is responsible for sending the response
// via `co_await send(response)`.
//
// A very basic `HttpHandler`, which simply serves files from a directory, can
// be obtained via `ad_utility::httpUtils::makeFileServer()`.
//
// `WebSocketHandler` is a callable type that receives a
// `http::request<...>` and the underlying socket that was used to receive the
// request and returns a `net::awaitable<void>`. It is only called if the
// request is a valid websocket upgrade request and the URL represents a valid
// path.
CPP_template(BodyReadMode bodyReadMode, typename HttpHandler,
             typename WebSocketHandler)(
    requires ad_utility::InvocableWithExactReturnType<
        WebSocketHandler, net::awaitable<void>,
        const http::request<http::string_body>&,
        tcp::socket>) class HttpServer {
 private:
  // Returned by `handleEagerRequest` and `handleLazyRequest` to indicate
  // whether the session loop should close the connection after this request.
  enum class SessionControl { Continue, Close };

  HttpHandler httpHandler_;
  int numServerThreads_;
  net::io_context ioContext_;
  WebSocketHandler webSocketHandler_;
  ad_utility::MemorySize lazyBodyChunkSize_;
  // All code that uses the `acceptor_` must run within this strand.
  // Note that the `acceptor_` might be concurrently accessed by the `listener`
  // and the `shutdown` function, the latter of which is currently only used in
  // unit tests.
  net::strand<net::io_context::executor_type> acceptorStrand_ =
      net::make_strand(ioContext_);
  tcp::acceptor acceptor_{acceptorStrand_};
  std::atomic<bool> serverIsReady_ = false;
  // Number of currently active `session` coroutines. `shutDown` waits (for a
  // bounded time) until this reaches zero before stopping the `io_context`, so
  // that in-flight sessions can finish (in particular, flush their final log
  // messages). See `shutDown` for details.
  std::atomic<int> numActiveSessions_ = 0;

 public:
  /// Construct from the `queryRegistry`, port and ip address, on which this
  /// server will listen, as well as the HttpHandler. This constructor only
  /// initializes several member functions
  ///
  // `lazyBodyChunkSize` is only used in `BodyReadMode::Lazy` mode: the body is
  // read in chunks of `lazyBodyChunkSize` bytes; each chunk is filled
  // completely before being yielded, trading latency for throughput.
  // Note: The following constraint can not be written with a single declaration
  // in the `std::enable_if_t` world, because of the following bug in GCC 11:
  // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105268
  template <typename HandlerSupplier>
  static constexpr bool isSupplier =
      ad_utility::InvocableWithConvertibleReturnType<
          HandlerSupplier, WebSocketHandler, net::io_context&>;
  CPP_template_2(typename HandlerSupplier)(
      requires isSupplier<
          HandlerSupplier>) explicit HttpServer(unsigned short port,
                                                std::string_view ipAddress =
                                                    "0.0.0.0",
                                                int numServerThreads = 1,
                                                HttpHandler handler =
                                                    HttpHandler{},
                                                HandlerSupplier
                                                    webSocketHandlerSupplier =
                                                        {},
                                                ad_utility::MemorySize
                                                    lazyBodyChunkSize =
                                                        ad_utility::MemorySize::
                                                            megabytes(1))
      : httpHandler_{std::move(handler)},
        // We need at least two threads to avoid blocking.
        // TODO<joka921> why is that?
        numServerThreads_{std::max(2, numServerThreads)},
        ioContext_{numServerThreads_},
        webSocketHandler_{
            std::invoke(std::move(webSocketHandlerSupplier), ioContext_)},
        lazyBodyChunkSize_{lazyBodyChunkSize} {
    try {
      tcp::endpoint endpoint{net::ip::make_address(ipAddress), port};
      // Open the acceptor.
      acceptor_.open(endpoint.protocol());
      boost::asio::socket_base::reuse_address option{true};
      acceptor_.set_option(option);
      // Bind to the server address.
      acceptor_.bind(endpoint);
    } catch (const boost::system::system_error& b) {
      logBeastError(b.code(), "Opening or binding the socket failed");
      throw;
    }
  }

  /// Run the server using the specified number of threads. Note that this
  /// function never returns, unless the Server crashes. The second argument
  /// limits the number of connections/sessions that may be handled concurrently
  /// (this is not equal to the number of threads due to the asynchronous
  /// implementation of this Server).
  void run() {
    // Create the listener coroutine.
    auto listenerCoro = listener();

    // Schedule the listener onto the `acceptorStrand_`.
    net::co_spawn(acceptorStrand_, std::move(listenerCoro), net::detached);

    // Add some threads to the io context, s.t. the work can actually be
    // scheduled.
    std::vector<ad_utility::JThread> threads;
    threads.reserve(numServerThreads_);
    for (auto i = 0; i < numServerThreads_; ++i) {
      threads.emplace_back([&ioContext = ioContext_] { ioContext.run(); });
    }

    // This will run forever, because the destructor of the JThreads blocks
    // until ioContext.run() completes, which should never happen, because the
    // listener contains an infinite loop.
  }

  // Get the server port.
  auto getPort() const { return acceptor_.local_endpoint().port(); }

  // Is the server ready yet? We need this in `test/HttpTest.cpp` so that our
  // test can wait for the server to be ready and continue with its test
  // queries.
  [[nodiscard]] bool serverIsReady() const { return serverIsReady_; }

  // Shut down the server. Http sessions that are still active are still allowed
  // to finish, even after this call, but all outstanding new connections will
  // fail. This interface is currently only used for testing. In the typical use
  // case, the server runs forever.
  void shutDown() {
    // The actual code for the shutdown is being executed on the same `strand`
    // as the `listener`. By the way strands work, there will then be no
    // concurrent access to `acceptor_`.
    auto future =
        net::post(acceptorStrand_,
                  std::packaged_task<void()>([this]() { acceptor_.close(); }));

    // Wait until the posted task has successfully executed
    future.wait();

    // Give still-active sessions a bounded grace period to finish before we
    // tear down the `io_context`. This matters because a session may be
    // suspended at an `async_*` operation between sending its response and
    // running its exception-handling/logging code. `ioContext_.stop()` discards
    // all queued completion handlers, so without this wait the handler that
    // would resume such a session (and let it log) may never run, losing the
    // log output. The client has already received the response at that point,
    // so a test that shuts down right after reading the response would race
    // with the server's logging (see `HttpTest`'s `ErrorHandlingInSession`).
    // The bound is a safety net so that a genuinely stuck session (e.g. an idle
    // keep-alive or WebSocket connection) cannot make `shutDown` hang forever.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (numActiveSessions_.load() > 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Make sure the tests don't run forever because without this `run()` may
    // not terminate.
    ioContext_.stop();
  }

 private:
  // Format a boost/beast error and log it to console
  void logBeastError(beast::error_code ec, std::string_view message) {
    AD_LOG_ERROR << message << ": " << ec.message() << std::endl;
  }

  // Handle a WebSocket upgrade request: send an error response if the path is
  // invalid, otherwise cancel `releaseConnection` and delegate to
  // `webSocketHandler_`.
  template <typename SendMessage, typename ReleaseConnection>
  net::awaitable<void> handleWebsocketUpgrade(
      beast::tcp_stream& stream, const http::request<http::string_body>& req,
      SendMessage& sendMessage, ReleaseConnection& releaseConnection) {
    auto websocketErrorResponse = ad_utility::websocket::WebSocketSession::
        getErrorResponseIfPathIsInvalid(req);
    if (websocketErrorResponse.has_value()) {
      co_await sendMessage(std::move(websocketErrorResponse.value()));
    } else {
      // Prevent cleanup after socket has been moved from.
      releaseConnection.cancel();
      co_await std::invoke(webSocketHandler_, req, std::move(stream.socket()));
    }
  }

  // Handle one eager-mode request: read the full body, then dispatch to
  // `httpHandler_` (or `handleWebsocketUpgrade` for WebSocket upgrades).
  // Returns `SessionControl::Close` when the session should exit (WebSocket was
  // handled), `SessionControl::Continue` otherwise.
  template <typename SendMessage, typename ReleaseConnection>
  net::awaitable<SessionControl> handleEagerRequest(
      beast::tcp_stream& stream, beast::flat_buffer& buffer,
      SendMessage& sendMessage, ReleaseConnection& releaseConnection)
      requires(bodyReadMode == BodyReadMode::Eager) {
    http::request_parser<http::string_body> requestParser;
    auto bodyLimit = getRequestBodyLimit().getBytes();
    requestParser.body_limit(
        bodyLimit == 0 ? boost::none : boost::optional<uint64_t>(bodyLimit));
    co_await http::async_read(stream, buffer, requestParser,
                              boost::asio::use_awaitable);
    http::request<http::string_body> req = requestParser.release();

    if (beast::websocket::is_upgrade(req)) {
      co_await handleWebsocketUpgrade(stream, req, sendMessage,
                                      releaseConnection);
      co_return SessionControl::Close;
    }
    // Currently there is no timeout on the server side, this is handled by
    // QLever's timeout mechanism.
    stream.expires_never();
    co_await httpHandler_(std::move(req), sendMessage);
    co_return SessionControl::Continue;
  }

 public:
  // Reads bytes from `requestParser` into `outputBuffer` until the buffer is
  // full or the body is exhausted. Returns the number of bytes written.
  // `need_buffer` (buffer segment full, more data remains) is treated as a
  // non-error stop condition; all other errors are thrown.
  //
  // The outer function is a non-coroutine factory that returns an
  // immediately-invoked coroutine lambda. This pattern works around a GCC 11
  // internal compiler error (ICE) that occurs when a `static` member function
  // is itself a coroutine and calls another coroutine `static` member function.
  static net::awaitable<size_t> readIntoBuffer(
      beast::tcp_stream& stream, beast::flat_buffer& buffer,
      http::request_parser<http::buffer_body>& requestParser,
      ql::span<char> outputBuffer) {
    return [](auto& stream, auto& buffer, auto& requestParser,
              auto outputBuffer) -> net::awaitable<size_t> {
      size_t totalRead = 0;
      while (!requestParser.is_done() && totalRead < outputBuffer.size()) {
        requestParser.get().body().data = outputBuffer.data() + totalRead;
        requestParser.get().body().size = outputBuffer.size() - totalRead;
        boost::system::error_code ec;
        co_await http::async_read_some(
            stream, buffer, requestParser,
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (ec && ec != http::error::need_buffer) {
          throw boost::system::system_error{ec};
        }
        totalRead +=
            (outputBuffer.size() - totalRead) - requestParser.get().body().size;
        // `need_buffer` means the buffer segment is full; stop here.
        if (ec == http::error::need_buffer) break;
      }
      co_return totalRead;
    }(stream, buffer, requestParser, outputBuffer);
  }

  // Reads at most `bodyLimit` bytes from `requestParser` into a string and
  // returns it.
  //
  // Uses the same IIFE factory pattern as `readIntoBuffer` to avoid a GCC 11
  // ICE (see comment there for details).
  static net::awaitable<std::string> materializeBody(
      beast::tcp_stream& stream, beast::flat_buffer& buffer,
      http::request_parser<http::buffer_body>& requestParser,
      size_t bodyLimit) {
    return [](auto& stream, auto& buffer, auto& requestParser,
              size_t bodyLimit) -> net::awaitable<std::string> {
      std::string result;
      std::vector<char> chunk(std::min(bodyLimit, size_t{4096}));
      while (!requestParser.is_done() && result.size() < bodyLimit) {
        const size_t toRead = std::min(bodyLimit - result.size(), chunk.size());
        const size_t bytesRead = co_await HttpServer::readIntoBuffer(
            stream, buffer, requestParser,
            ql::span<char>{chunk.data(), toRead});
        result.append(chunk.data(), bytesRead);
        if (bytesRead == 0) break;
      }
      co_return result;
    }(stream, buffer, requestParser, bodyLimit);
  }

 private:
  // Callable passed to the lazy-mode `httpHandler_` as `bodyGetter`. Holds
  // references to the stream state; `operator()` reads the next body chunk
  // (up to `chunkSize_` bytes) into `chunkBuffer_` and returns a `string_view`
  // into it, or `nullopt` when the body is exhausted.
  //
  // `operator()` cannot be implemented as a nested coroutine lambda because
  // that triggers the same GCC 11 ICE described in `readIntoBuffer`. The
  // IIFE factory pattern is used instead.
  template <typename Stream, typename Buffer, typename RequestParser,
            typename ChunkBuffer>
  struct BodyChunkReader {
    Stream& stream_;
    Buffer& buffer_;
    RequestParser& requestParser_;
    ChunkBuffer& chunkBuffer_;
    size_t chunkSize_;

    BodyChunkReader(Stream& stream, Buffer& buffer,
                    RequestParser& requestParser, ChunkBuffer& chunkBuffer,
                    size_t chunkSize)
        : stream_(stream),
          buffer_(buffer),
          requestParser_(requestParser),
          chunkBuffer_(chunkBuffer),
          chunkSize_(chunkSize) {}

    net::awaitable<std::optional<std::string_view>> operator()() {
      return [](auto& stream, auto& buffer, auto& requestParser,
                auto& chunkBuffer, size_t chunkSize)
                 -> net::awaitable<std::optional<std::string_view>> {
        const size_t bytesRead = co_await HttpServer::readIntoBuffer(
            stream, buffer, requestParser,
            ql::span<char>{chunkBuffer.data(), chunkSize});
        if (bytesRead == 0) co_return std::nullopt;
        co_return std::string_view{chunkBuffer.data(), bytesRead};
      }(stream_, buffer_, requestParser_, chunkBuffer_, chunkSize_);
    }
  };

  // Factory for `BodyChunkReader` using function template argument deduction,
  // avoiding CTAD for a nested class template (broken in clang < 18).
  template <typename Stream, typename Buffer, typename RequestParser,
            typename ChunkBuffer>
  static auto makeBodyChunkReader(Stream& stream, Buffer& buffer,
                                  RequestParser& requestParser,
                                  ChunkBuffer& chunkBuffer, size_t chunkSize) {
    return BodyChunkReader<Stream, Buffer, RequestParser, ChunkBuffer>{
        stream, buffer, requestParser, chunkBuffer, chunkSize};
  }

  // Handle one lazy-mode request: read only the headers, then check for a
  // WebSocket upgrade or pass a `bodyGetter` callable to `httpHandler_`.
  // For WebSocket upgrades the remaining body is materialized and
  // `handleWebsocketUpgrade` is called; returns `SessionControl::Close` so the
  // caller exits the session loop. For normal requests each
  // `co_await bodyGetter()` reads the next chunk (up to `lazyBodyChunkSize_`
  // bytes) into a buffer that lives in this function's stack frame and returns
  // a `string_view` into it; the view is valid only until the next call;
  // `nullopt` signals end-of-body; throws on network errors. Returns
  // `SessionControl::Continue` for normal requests.
  template <typename SendMessage, typename ReleaseConnection>
  net::awaitable<SessionControl> handleLazyRequest(
      beast::tcp_stream& stream, beast::flat_buffer& buffer,
      SendMessage& sendMessage, ReleaseConnection& releaseConnection)
      requires(bodyReadMode == BodyReadMode::Lazy) {
    http::request_parser<http::buffer_body> requestParser;
    // Apply the configured body limit (same as eager mode) to guard against
    // excessively large requests.
    const auto bodyLimit = getRequestBodyLimit().getBytes();
    requestParser.body_limit(
        bodyLimit == 0 ? boost::none : boost::optional<uint64_t>(bodyLimit));
    co_await http::async_read_header(stream, buffer, requestParser,
                                     boost::asio::use_awaitable);

    http::request<http::empty_body> headersReq =
        ad_utility::httpUtils::getHeaderOnlyRequest(requestParser.get());

    if (beast::websocket::is_upgrade(headersReq)) {
      static constexpr size_t maxWebSocketBodyBytes = 100'000;
      const size_t configBodyLimit = getRequestBodyLimit().getBytes();
      const size_t wsBodyLimit = std::min(
          configBodyLimit != 0 ? configBodyLimit : maxWebSocketBodyBytes,
          maxWebSocketBodyBytes);
      std::string body =
          co_await materializeBody(stream, buffer, requestParser, wsBodyLimit);
      auto stringReq = ad_utility::httpUtils::getStringBodyRequest(
          headersReq, std::move(body));
      co_await handleWebsocketUpgrade(stream, stringReq, sendMessage,
                                      releaseConnection);
      co_return SessionControl::Close;
    }

    // Buffer for body chunks; its lifetime covers the entire handler
    // invocation.
    const size_t chunkSize = lazyBodyChunkSize_.getBytes();
    std::vector<char> chunkBuffer(chunkSize);

    // `bodyGetter` yields body chunks; see `BodyChunkReader`.
    auto bodyGetter = makeBodyChunkReader(stream, buffer, requestParser,
                                          chunkBuffer, chunkSize);

    stream.expires_never();
    co_await httpHandler_(std::move(headersReq), sendMessage,
                          std::move(bodyGetter));
    co_return SessionControl::Continue;
  }

  // The loop which accepts TCP connections and delegates their handling
  // to the session() coroutine below.
  boost::asio::awaitable<void> listener() {
    try {
      acceptor_.listen(boost::asio::socket_base::max_listen_connections);
    } catch (const boost::system::system_error& b) {
      logBeastError(b.code(), "Listening on the socket failed");
      co_return;
    }
    serverIsReady_ = true;

    // While the `acceptor_` is open, accept connections and handle each
    // connection asynchronously (so that we are ready to accept the next
    // connection right away). The `acceptor_` will be closed via the
    // `shutdown` method.
    while (acceptor_.is_open()) {
      try {
        // Wait for a request (the code only continues after we have received
        // and accepted a request).

        // Although a coroutine is conceptually single-threaded we still
        // schedule onto an explicit strand because the Websocket implementation
        // expects a strand.
        auto strand = net::make_strand(ioContext_);
        auto socket =
            co_await acceptor_.async_accept(strand, boost::asio::use_awaitable);
        // Schedule the session such that it may run in parallel to this loop.
        net::co_spawn(strand, session(std::move(socket)), net::detached);
      } catch (const boost::system::system_error& b) {
        // If the server is shut down this will cause operations to abort.
        // This will most likely only happen in tests, but could also occur
        // in a future version of Qlever that manually handles SIGTERM signals.
        if (b.code() != boost::asio::error::operation_aborted) {
          logBeastError(b.code(), "Error in the accept loop");
        }
      }
    }
  }

  // This coroutine handles a single http session which is represented by a
  // socket.
  boost::asio::awaitable<void> session(tcp::socket socket) {
    // Track this session as active so that `shutDown` can wait for it to finish
    // before stopping the `io_context`. The counter is decremented when this
    // coroutine's frame is destroyed, i.e. after all of its code (including any
    // exception logging) has run.
    numActiveSessions_.fetch_add(1);
    absl::Cleanup decrementActiveSessions{
        [this]() { numActiveSessions_.fetch_sub(1); }};

    beast::flat_buffer buffer;
    beast::tcp_stream stream{std::move(socket)};

    auto releaseConnection =
        ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([&stream]() {
          [[maybe_unused]] beast::error_code ec;
          stream.socket().shutdown(tcp::socket::shutdown_send, ec);
          stream.socket().close(ec);
        });

    // Keep track of whether we have to close the session after a
    // request/response pair.
    std::atomic<bool> streamNeedsClosing = false;

    // This lambda sends an http message to the `stream` and sets
    // `streamNeedsClosing` if the closing of the session is requested by the
    // session.
    auto sendMessage = [&stream, &streamNeedsClosing](
                           auto message) -> boost::asio::awaitable<void> {
      // Write the response
      co_await http::async_write(stream, message, boost::asio::use_awaitable);

      // Inform the session if the message requires the closing of the
      // connection.
      if (message.need_eof()) {
        streamNeedsClosing = true;
      }
    };

    // Sessions might be reused for multiple request/response pairs.
    while (true) {
      // Optional to temporarily store an error response. We can not `co_await`
      // in a `catch` block and thus can not send the error response directly in
      // the `catch`.
      std::optional<http::response<http::string_body>> errorResponse;

      try {
        // Set the timeout for reading the next request.
        stream.expires_after(std::chrono::seconds(30));

        if constexpr (bodyReadMode == BodyReadMode::Eager) {
          auto control = co_await handleEagerRequest(
              stream, buffer, sendMessage, releaseConnection);
          if (control == SessionControl::Close) co_return;
        } else {
          auto control = co_await handleLazyRequest(stream, buffer, sendMessage,
                                                    releaseConnection);
          if (control == SessionControl::Close) co_return;
          // Reusing the stream for keep-alive is only safe if the full request
          // body has been consumed. In lazy mode the handler controls body
          // consumption, so we cannot guarantee it. This could be improved in
          // the future by draining any remaining body bytes after the handler
          // returns.
          streamNeedsClosing = true;
        }

        // The closing of the stream is done in the exception handler.
        if (streamNeedsClosing) {
          throw beast::system_error{http::error::end_of_stream};
        }
      } catch (const boost::system::system_error& error) {
        if (error.code() == http::error::end_of_stream) {
          // The stream has ended, gracefully close the connection.
          beast::error_code ec;
          stream.socket().shutdown(tcp::socket::shutdown_send, ec);
        } else if (error.code() == http::error::body_limit) {
          if constexpr (bodyReadMode == BodyReadMode::Eager) {
            errorResponse = ad_utility::httpUtils::createHttpResponseFromString(
                absl::StrCat(
                    "Request body size exceeds the allowed size (",
                    getRequestBodyLimit().asString(),
                    "), send a smaller request or set the allowed size "
                    "via the runtime parameter `request-body-limit`"),
                http::status::payload_too_large,
                ad_utility::MediaType::textPlain, std::nullopt, 11);
          }
        } else {
          // This is the error "The socket was closed due to a timeout" or if
          // the client stream ended unexpectedly.
          if (error.code() == beast::error::timeout ||
              error.code() == boost::asio::error::eof) {
            AD_LOG_TRACE << error.what() << " (code " << error.code() << ")"
                         << std::endl;
          } else {
            logBeastError(error.code(), error.what());
          }
        }
        // If we have an error response send it outside the `catch` block. (We
        // can not `co_await` in the `catch` block.) Otherwise close the
        // session by returning.
        if (!errorResponse) {
          co_return;
        }
      } catch (const std::exception& error) {
        AD_LOG_ERROR << error.what() << std::endl;
        co_return;
      } catch (...) {
        AD_LOG_ERROR << "Weird exception not inheriting from std::exception, "
                        "this shouldn't happen"
                     << std::endl;
        co_return;
      }

      // If we have an error response, send it and then close the session by
      // returning.
      if (errorResponse.has_value()) {
        co_return co_await sendMessage(std::move(errorResponse).value());
      }
    }
  }
};

/// Deduction guide for eager mode (the default). Specify BodyReadMode::Lazy
/// explicitly when creating a lazy-mode server.
template <typename HttpHandler, typename WebSocketHandlerSupplier>
HttpServer(unsigned short, const std::string&, int, HttpHandler,
           WebSocketHandlerSupplier)
    -> HttpServer<
        BodyReadMode::Eager, HttpHandler,
        std::invoke_result_t<WebSocketHandlerSupplier, net::io_context&>>;

#endif  // QLEVER_HTTPSERVER_H
