//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_HTTPSERVER_H
#define QLEVER_HTTPSERVER_H

#include <cstdlib>
#include <semaphore>

#include "../Log.h"
#include "../jthread.h"
#include "./HttpUtils.h"
#include "./beast.h"

namespace beast = boost::beast;    // from <boost/beast.hpp>
namespace http = beast::http;      // from <boost/beast/http.hpp>
namespace net = boost::asio;       // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;  // from <boost/asio/ip/tcp.hpp>

/*
 * \brief A Simple HttpServer, based on Boost::Beast. Its can be configured via
 * the mandatory HttpHandler parameter. \tparam HttpHandler A callable type that
 * takes two parameters, a `http::request<...>` , and a `sendAction` and returns
 * an awaitable<void> type. sendAction always is a callable that takes a
 * http::message, and returns an awaitable<void>; The behavior is then as
 * follows: as soon as the Server receives a http request, co_await
 * _httpHandler(move(request), sendAction) is called. (_httpHandler is a member
 * of type HttpHandler). The expected behavior of this call is that _httpHandler
 * takes the request, computes the corresponding `response`, and calls co_await
 * sendAction(response). The `sendAction` is needed, b.c. the `response` might
 * different types (in beast, a http::message is templated on the body type).
 *  For this reason, this approach is more flexible, than having _httpHandler
 * simply return the response. A very basic HttpHandler, that simply serves
 * files from a directory, can be obtained via
 * `ad_utility::httpUtils::makeFileServer()`
 */
template <typename HttpHandler>
class HttpServer {
 private:
  const net::ip::address _ipAdress;
  const short unsigned int _port;
  HttpHandler _httpHandler;

  // Limit the number of concurrent sessions. It has to be an optional, because
  // we only initialize it in the run() function.
  // TODO<joka921> The standard says, that there has to be some large default
  // template parameter for the limit of the semaphore, but neither libstdc++
  // nor libc++ currently implement  this, so we manually have to specify a
  // large value.
  std::optional<std::counting_semaphore<std::numeric_limits<int>::max()>>
      _numSessionLimiter;

 public:
  /// Construct from the port and ip address, on which this server will listen,
  /// as well as the HttpHandler. This constructor only initializes several
  /// member functions,
  explicit HttpServer(short unsigned int port,
                      const std::string& ipAdress = "0.0.0.0",
                      HttpHandler handler = HttpHandler{})
      : _ipAdress{net::ip::make_address(ipAdress)},
        _port{port},
        _httpHandler{std::move(handler)} {}

  /// Run the server using the specified number of threads. Note that this
  /// function never returns, unless the Server crashes. The second argument
  /// limits the number of connections/sessions that may be handled concurrently
  /// (this is not equal to the number of threads due to the asynchronous
  /// implementation of this Server).
  void run(int numServerThreads, size_t numSimultaneousConnections) {
    net::io_context ioc(numServerThreads);

    _numSessionLimiter.emplace(numSimultaneousConnections);

    // Create the listener coroutine.
    auto listenerCoro = listener(ioc, tcp::endpoint{_ipAdress, _port});

    // Schedule the listener onto the io context.
    net::co_spawn(ioc, std::move(listenerCoro), net::detached);

    // Add some threads to the io context, s.t. the work can actually be
    // scheduled.
    AD_CHECK(numServerThreads >= 1);
    std::vector<ad_utility::JThread> threads;
    threads.reserve(numServerThreads);
    for (auto i = 0; i < numServerThreads; ++i) {
      threads.emplace_back([&ioc] { ioc.run(); });
    }

    // This will run forever, because the destructor of the JThreads blocks
    // until ioc.run() completes, which should never happen, because the
    // listener contains an infinite loop.
  }

 private:
  // Format a boost/beast error and log it to console
  void logBeastError(beast::error_code ec, std::string_view message) {
    LOG(ERROR) << message << ": " << ec.message() << std::endl;
  }

  // The loop which accepts tcp connections and delegates their handling
  // to the session() coroutine below.
  boost::asio::awaitable<void> listener(boost::asio::io_context& ioc,
                                        tcp::endpoint endpoint) {
    tcp::acceptor acceptor{ioc};
    try {
      // Open the acceptor
      acceptor.open(endpoint.protocol());
      // Bind to the server address
      acceptor.bind(endpoint);
      // Start listening for connections
      acceptor.listen(boost::asio::socket_base::max_listen_connections);
    } catch (const boost::system::system_error& b) {
      logBeastError(b.code(),
                    "opening, binding or listening on the socket failed");
      co_return;
    }

    // `acceptor` is now listening on the port, start accepting connections in
    // an infinite, asynchronous, but conceptually single-threaded loop.
    for (;;) {
      try {
        _numSessionLimiter->acquire();
        auto socket =
            co_await acceptor.async_accept(boost::asio::use_awaitable);
        // Schedule the session, s.t. it may run in parallel to this loop.
        // the `strand` makes each session conceptually single-threaded.
        // TODO<joka921> is this even needed, to my understanding,
        // nothing may happen in parallel within an Http session, but
        // then again this does no harm.
        net::co_spawn(net::make_strand(ioc), session(std::move(socket)),
                      net::detached);

      } catch (const boost::system::system_error& b) {
        logBeastError(b.code(), b.what());
      }
    }
  }

  // This coroutine handles a single http session which is represented by a
  // socket.
  boost::asio::awaitable<void> session(tcp::socket socket) {
    beast::flat_buffer buffer;
    beast::tcp_stream stream{std::move(socket)};

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

      // If the message requires the closing of the connection,
      if (message.need_eof()) {
        streamNeedsClosing = true;
      }
    };

    // Sessions might be reused for multiple request/response pairs.
    for (;;) {
      try {
        // Set the timeout.
        stream.expires_after(std::chrono::seconds(30));
        http::request<http::string_body> req;

        // Read a request
        co_await http::async_read(stream, buffer, req,
                                  boost::asio::use_awaitable);

        // Handle the http request. Note that `_httpHandler` is also responsible
        // for sending the message via the `sendMessage` lambda.
        co_await _httpHandler(std::move(req), sendMessage);

        // The closing of the stream is done in the exception handler.
        if (streamNeedsClosing) {
          throw beast::system_error{http::error::end_of_stream};
        }
      } catch (const boost::system::system_error& b) {
        if (b.code() == http::error::end_of_stream) {
          // The stream has ended, gracefully close the connection.
          beast::error_code ec;
          stream.socket().shutdown(tcp::socket::shutdown_send, ec);
        } else {
          logBeastError(b.code(), b.what());
        }
        // In case of an error, close the session by returning.
        _numSessionLimiter->release();
        co_return;
      }
    }
  }
};

#endif  // QLEVER_HTTPSERVER_H
