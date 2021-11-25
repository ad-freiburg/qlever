//
// Created by projector-user on 11/18/21.
//

#ifndef QLEVER_WEBSERVER_H
#define QLEVER_WEBSERVER_H

// TODO<joka921>:where to add/define these hacky macros.
#define BOOST_ASIO_HAS_CO_AWAIT
#define BOOST_ASIO_HAS_STD_COROUTINE


#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/config.hpp>
#include <boost/asio.hpp>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include "./Log.h"
#include "./WebServerHttpImplementations.h"

namespace beast = boost::beast;         // from <boost/beast.hpp>
namespace http = beast::http;           // from <boost/beast/http.hpp>
namespace net = boost::asio;            // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>

template<typename HttpHandler>
class HttpServer {


 private:
  void fail(beast::error_code ec, std::string_view message) {
      LOG(ERROR) << message << ": " << ec.message() << std::endl;
  }

  boost::asio::awaitable<void> session(
      tcp::socket socket) {
    beast::flat_buffer buffer;
    beast::tcp_stream stream{std::move(socket)};
    auto send_lambda = [&stream]<bool isRequest, typename Body, typename Fields>(http::message<isRequest, Body, Fields>&& message) ->boost::asio::awaitable<void> {
      // Write the response
      co_await http::async_write(
          stream,
          message, boost::asio::use_awaitable);
    };

    for (;;) {
      try {
        // Set the timeout.
        stream.expires_after(std::chrono::seconds(30));
        http::request<http::string_body> req;

        // Read a request
        co_await http::async_read(stream, buffer, req,
                                  boost::asio::use_awaitable);

        /*
        // TODO<joka921> :: reinstate the error codes.
        // This means they closed the connection
        if(ec == http::error::end_of_stream)
            return do_close();

        if(ec)
            return fail(ec, "read");
            */

        // Send the response
        co_await _httpHandler(std::move(req), send_lambda);
      } catch(...) {}

    }

  }

  //------------------------------------------------------------------------------
  boost::asio::awaitable<void> listener(
      boost::asio::io_context& ioc,
      tcp::endpoint endpoint) {

    tcp::acceptor acceptor{ioc};

    try {
      // Open the acceptor
      acceptor.open(endpoint.protocol());
      // Bind to the server address
      acceptor.bind(endpoint);
      // Start listening for connections
      acceptor.listen(boost::asio::socket_base::max_listen_connections );
    } catch (const boost::system::system_error& b) {
      fail(b.code(), "opening, binding or listening on the socket failed");
      co_return;
    }


    for(;;) {
      try {
        auto socket = co_await acceptor.async_accept(boost::asio::use_awaitable);
        net::co_spawn(net::make_strand(ioc), session(std::move(socket)), net::detached);

      } catch (const boost::system::system_error& b) {
        fail(b.code(), b.what());
      }

    }

  }


 public:
  void run(int numServerThreads) {
    const auto address = net::ip::make_address(_ipAdress);
    net::io_context ioc(numServerThreads);

    // startTheCoroutine that handles the actual logic.
    auto l = listener(ioc, tcp::endpoint{address, _port});
    net::co_spawn(ioc, std::move(l), net::detached);
    AD_CHECK(numServerThreads >= 1);
    std::vector<std::thread> threads;
    threads.reserve(numServerThreads - 1);
    for (auto i = 0; i < numServerThreads - 1; ++i) {
      threads.emplace_back([&ioc]{ioc.run();});
    }
    ioc.run();
  }

  explicit HttpServer(short unsigned int port, HttpHandler handler = HttpHandler{}) : _port{port}, _httpHandler{std::move(handler)} {}


 private:
  std::string _ipAdress = "0.0.0.0";
  short unsigned int _port = 80;
  HttpHandler _httpHandler;

};

#endif  // QLEVER_WEBSERVER_H
