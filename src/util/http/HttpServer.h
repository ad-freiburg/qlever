//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_HTTPSERVER_H
#define QLEVER_HTTPSERVER_H

#include <cstdlib>
#include <future>

#include "absl/cleanup/cleanup.h"
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

/*
 * \brief A Simple HttpServer, based on Boost::Beast. It can be configured via
 * the mandatory HttpHandler parameter.
 *
 * \tparam HttpHandler A callable type that takes two parameters, a
 * `http::request<...>` , and a `sendAction` and returns an awaitable<void>
 * type. sendAction always is a callable that takes a http::message, and returns
 * an awaitable<void>.
 *
 * The behavior is then as follows: as soon as the server receives a HTTP
 * request, co_await httpHandler_(move(request), sendAction) is called.
 * (httpHandler_ is a member of type HttpHandler). The expected behavior of this
 * call is that httpHandler_ takes the request, computes the corresponding
 * `response`, and calls co_await sendAction(response). The `sendAction` is
 * needed because the `response` can have different types (in beast, a
 * http::message is templated on the body type). For this reason, this approach
 * is more flexible, than having httpHandler_ simply return the response.
 *
 * A very basic HttpHandler, which simply serves files from a directory, can be
 * obtained via `ad_utility::httpUtils::makeFileServer()`.
 *
 * \tparam WebSocketHandler A callable type that receives a `http::request<...>`
 * and the underlying socket that was used to receive the request and returns
 * a `net::awaitable<void>`. It is only called if the request is a valid
 * websocket upgrade request and the URL represents a valid path.
 */
template <typename HttpHandler,
          ad_utility::InvocableWithExactReturnType<
              net::awaitable<void>, const http::request<http::string_body>&,
              tcp::socket>
              WebSocketHandler>
class HttpServer {
 private:
  HttpHandler httpHandler_;
  int numServerThreads_;
  net::io_context ioContext_;
  WebSocketHandler webSocketHandler_;
  // All code that uses the `acceptor_` must run within this strand.
  // Note that the `acceptor_` might be concurrently accessed by the `listener`
  // and the `shutdown` function, the latter of which is currently only used in
  // unit tests.
  net::strand<net::io_context::executor_type> acceptorStrand_ =
      net::make_strand(ioContext_);
  tcp::acceptor acceptor_{acceptorStrand_};
  std::atomic<bool> serverIsReady_ = false;

 public:
  /// Construct from the `queryRegistry`, port and ip address, on which this
  /// server will listen, as well as the HttpHandler. This constructor only
  /// initializes several member functions
  explicit HttpServer(
      unsigned short port, std::string_view ipAddress = "0.0.0.0",
      int numServerThreads = 1, HttpHandler handler = HttpHandler{},
      ad_utility::InvocableWithConvertibleReturnType<WebSocketHandler,
                                                     net::io_context&> auto
          webSocketHandlerSupplier = {})
      : httpHandler_{std::move(handler)},
        // We need at least two threads to avoid blocking.
        // TODO<joka921> why is that?
        numServerThreads_{std::max(2, numServerThreads)},
        ioContext_{numServerThreads_},
        webSocketHandler_{std::invoke(webSocketHandlerSupplier, ioContext_)} {
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
  }

 private:
  // Format a boost/beast error and log it to console
  void logBeastError(beast::error_code ec, std::string_view message) {
    LOG(ERROR) << message << ": " << ec.message() << std::endl;
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
      try {
        // Set the timeout for reading the next request.
        stream.expires_after(std::chrono::seconds(30));
        http::request<http::string_body> req;

        // Read a request
        co_await http::async_read(stream, buffer, req,
                                  boost::asio::use_awaitable);

        // Let request be handled by `WebSocketSession` if the HTTP
        // request is a WebSocket handshake
        if (beast::websocket::is_upgrade(req)) {
          auto errorResponse = ad_utility::websocket::WebSocketSession::
              getErrorResponseIfPathIsInvalid(req);
          if (errorResponse.has_value()) {
            co_await sendMessage(errorResponse.value());
          } else {
            // prevent cleanup after socket has been moved from
            releaseConnection.cancel();
            co_await std::invoke(webSocketHandler_, req,
                                 std::move(stream.socket()));
            co_return;
          }
        } else {
          // Currently there is no timeout on the server side, this is handled
          // by QLever's timeout mechanism.
          stream.expires_never();

          // Handle the http request. Note that `httpHandler_` is also
          // responsible for sending the message via the `sendMessage` lambda.
          co_await httpHandler_(std::move(req), sendMessage);
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
        } else {
          // This is the error "The socket was closed due to a timeout" or if
          // the client stream ended unexpectedly.
          if (error.code() == beast::error::timeout ||
              error.code() == boost::asio::error::eof) {
            LOG(TRACE) << error.what() << " (code " << error.code() << ")"
                       << std::endl;
          } else {
            logBeastError(error.code(), error.what());
          }
        }
        // In case of an error, close the session by returning.
        co_return;
      } catch (const std::exception& error) {
        LOG(ERROR) << error.what() << std::endl;
        co_return;
      } catch (...) {
        LOG(ERROR) << "Weird exception not inheriting from std::exception, "
                      "this shouldn't happen"
                   << std::endl;
        co_return;
      }
    }
  }
};

/// Deduction guide, so you don't have to specify the types explicitly
/// when creating an instance of this class.
template <typename HttpHandler, typename WebSocketHandlerSupplier>
HttpServer(unsigned short, const std::string&, int, HttpHandler,
           WebSocketHandlerSupplier)
    -> HttpServer<HttpHandler, std::invoke_result_t<WebSocketHandlerSupplier,
                                                    net::io_context&>>;

#endif  // QLEVER_HTTPSERVER_H
