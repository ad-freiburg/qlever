//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_BEASTTESTHELPERS_H
#define QLEVER_BEASTTESTHELPERS_H

#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Exception.h"

// Based on boost::beast::test in the boost/beast/_experimental/tcp.hpp header

namespace net = boost::asio;
using namespace boost::asio::experimental::awaitable_operators;

// Quickly connect 2 TCP/IP sockets together via the localhost loopback address.
template <class Executor>
net::awaitable<void> connect(
    net::basic_stream_socket<net::ip::tcp, Executor>& s1,
    net::basic_stream_socket<net::ip::tcp, Executor>& s2) {
  net::basic_socket_acceptor<net::ip::tcp, Executor> a(s1.get_executor());
  auto ep = net::ip::tcp::endpoint(net::ip::make_address_v4("127.0.0.1"), 0);
  a.open(ep.protocol());
  a.set_option(net::socket_base::reuse_address(true));
  a.bind(ep);
  a.listen(0);
  ep = a.local_endpoint();

  co_await (a.async_accept(s2, net::use_awaitable) &&
            s1.async_connect(ep, net::use_awaitable));

  AD_CORRECTNESS_CHECK(s1.remote_endpoint() == s2.local_endpoint());
  AD_CORRECTNESS_CHECK(s2.remote_endpoint() == s1.local_endpoint());
}

#endif  // QLEVER_BEASTTESTHELPERS_H
