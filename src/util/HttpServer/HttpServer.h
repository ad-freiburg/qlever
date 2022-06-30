//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_HTTPSERVER_H
#define QLEVER_HTTPSERVER_H

#include <cstdlib>
#include <semaphore>

#include "../Exception.h"
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
 * sendAction(response). The `sendAction` is needed because the `response` might
 * different types (in beast, a http::message is templated on the body type).
 *  For this reason, this approach is more flexible, than having _httpHandler
 * simply return the response. A very basic HttpHandler, which simply serves
 * files from a directory, can be obtained via
 * `ad_utility::httpUtils::makeFileServer()`.
 */
template <typename HttpHandler>
class HttpServer {
 private:
  const net::ip::address _ipAdress;
  const short unsigned int _port;
  HttpHandler _httpHandler;
  int _numServerThreads;
  net::io_context _ioContext;
  tcp::acceptor _acceptor;

 public:
  /// Construct from the port and ip address, on which this server will listen,
  /// as well as the HttpHandler. This constructor only initializes several
  /// member functions
  explicit HttpServer(short unsigned int port,
                      const std::string& ipAdress = "0.0.0.0",
                      int numServerThreads = 1,
                      HttpHandler handler = HttpHandler{})
      : _ipAdress{net::ip::make_address(ipAdress)},
        _port{port},
        _httpHandler{std::move(handler)},
        // We need at least two threads to avoid blocking.
        // TODO<joka921> why is that?
        _numServerThreads{std::max(2, numServerThreads)},
        _ioContext{_numServerThreads},
        _acceptor{_ioContext} {
    try {
      tcp::endpoint endpoint{_ipAdress, _port};
      // Open the acceptor.
      _acceptor.open(endpoint.protocol());
      boost::asio::socket_base::reuse_address option{true};
      _acceptor.set_option(option);
      // Bind to the server address.
      _acceptor.bind(endpoint);
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

    // Schedule the listener onto the io context.
    net::co_spawn(_ioContext, std::move(listenerCoro), net::detached);

    // Add some threads to the io context, s.t. the work can actually be
    // scheduled.
    std::vector<ad_utility::JThread> threads;
    threads.reserve(_numServerThreads);
    for (auto i = 0; i < _numServerThreads; ++i) {
      threads.emplace_back([&ioContext = _ioContext] { ioContext.run(); });
    }

    // This will run forever, because the destructor of the JThreads blocks
    // until ioContext.run() completes, which should never happen, because the
    // listener contains an infinite loop.
  }

  // _____________________________________________________________________
  net::io_context& getIoContext() { return _ioContext; };

 private:
  // Format a boost/beast error and log it to console
  void logBeastError(beast::error_code ec, std::string_view message) {
    LOG(ERROR) << message << ": " << ec.message() << std::endl;
  }

  // The loop which accepts TCP connections and delegates their handling
  // to the session() coroutine below.
  boost::asio::awaitable<void> listener() {
    try {
      _acceptor.listen(boost::asio::socket_base::max_listen_connections);
    } catch (const boost::system::system_error& b) {
      logBeastError(b.code(), "Listening on the socket failed");
      co_return;
    }

    // `acceptor` is now listening on the port, start accepting connections in
    // an infinite, asynchronous, but conceptually single-threaded loop.
    while (true) {
      try {
        auto socket =
            co_await _acceptor.async_accept(boost::asio::use_awaitable);
        // Schedule the session such that it may run in parallel to this loop.
        // the `strand` makes each session conceptually single-threaded.
        // TODO<joka921> is this even needed, to my understanding,
        // nothing may happen in parallel within an Http session, but
        // then again this does no harm.
        net::co_spawn(net::make_strand(_ioContext), session(std::move(socket)),
                      net::detached);

      } catch (const boost::system::system_error& b) {
        logBeastError(b.code(), "Error in the accept loop");
      }
    }
  }

  // This coroutine handles a single http session which is represented by a
  // socket.
  boost::asio::awaitable<void> session(tcp::socket socket) {
    beast::flat_buffer buffer;
    beast::tcp_stream stream{std::move(socket)};

    auto releaseConnection = ad_utility::OnDestruction{

        [&stream]() noexcept {
          beast::error_code ec;
          stream.socket().shutdown(tcp::socket::shutdown_send, ec);
          stream.socket().close();
        }};

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

        // Currently there is no timeout on the server side, this is handled
        // by QLever's timeout mechanism.
        stream.expires_never();

        // Handle the http request. Note that `_httpHandler` is also responsible
        // for sending the message via the `sendMessage` lambda.
        co_await _httpHandler(std::move(req), sendMessage);

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
          // This is the error "The socket was closed due to a timeout".
          if (error.code() == beast::error::timeout) {
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

#endif  // QLEVER_HTTPSERVER_H
