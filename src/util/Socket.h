// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <string>
#include <sstream>
#include <iostream>

#include "./Log.h"

using std::string;

namespace ad_utility {
static const int MAX_NOF_CONNECTIONS = 500;
static const int RECIEVE_BUFFER_SIZE = 100000;

//! Basic Socket class used by the server code of the semantic search.
//! Wraps low-level socket calls should possibly be replaced by
//! different implementations and wrap them instead.
class Socket {
public:

  // Default ctor.
  Socket() :
      _fd(-1) {
    _buf = new char[RECIEVE_BUFFER_SIZE + 1];
  }

  // Destructor, close the socket if open.
  ~Socket() {
    ::close(_fd);
    delete[] _buf;
  }

  //! Create the socket.
  bool create(bool useTcpNoDelay = false) {
    _fd = socket(AF_INET, SOCK_STREAM, 0);
    if (useTcpNoDelay) {
      int flag = 1;
      int result = setsockopt(_fd,            /* socket affected */
                              IPPROTO_TCP,     /* set option at TCP level */
                              TCP_NODELAY,     /* name of option */
                              (char*) &flag,  /* the cast is historical
                                                         cruft */
                              sizeof(int));    /* length of option value */
      if (result < 0) return false;
    }
    if (!isOpen()) return false;
    // Make sockets reusable immediately after closing
    // Copied from CompletionServer code.
    int rc; /* return code */
    int on = 1;
    rc = setsockopt(_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    if (rc < 0) perror("! WARNING: setsockopt(SO_REUSEADDR) failed");
    return true;
  }

  //! Bind the socket to the given port.
  bool bind(const int port) {
    if (!isOpen()) return false;

    _address.sin_family = AF_INET;
    _address.sin_addr.s_addr = INADDR_ANY;
    _address.sin_port = htons(port);

    return ::bind(_fd, (struct sockaddr*) &_address,
                  sizeof(_address)) != -1;
  }

  //! Make it a listening socket.
  bool listen() const {
    if (!isOpen()) return false;
    return ::listen(_fd, MAX_NOF_CONNECTIONS) != -1;
  }

  //! Accept a connection.
  bool acceptClient(Socket* other) {
    int addressSize = sizeof(_address);
    other->_fd = ::accept(_fd, reinterpret_cast<sockaddr*>(&_address),
                          reinterpret_cast<socklen_t*>(&addressSize));

    return other->isOpen();
  }

  //! State if the socket's file descriptor is valid.
  bool isOpen() const {
    return _fd != -1;
  }

  //! Send some string.
  bool send(const std::string& data) const {
    LOG(TRACE) << "Called send() ... data: " << data << std::endl;
    LOG(TRACE) << "data.size(): " << data.size() << std::endl;
    return send(data, 5);
  }

  //! Send some string.
  bool send(const std::string& data, int timesRetry) const {
    int nb = ::send(_fd, data.c_str(), data.size(), MSG_NOSIGNAL);
    if (nb != static_cast<int>((data.size()))) {
      LOG(DEBUG) << "Could not send as much data as intended." << std::endl;
      if (nb == -1) {
        LOG(DEBUG) << "Errno: " << errno << std::endl;
        if (errno == 11 && timesRetry > 0) {
          LOG(DEBUG) << "Retrying " << timesRetry-- << " times " << std::endl;
          return send(data, timesRetry);
        }
      } else {
        LOG(DEBUG) << "Nof bytes sent: " << nb << std::endl;
      }
    }
    return nb;
  }

  //! Receive something.
  int receive(std::string* data) const {
    return receive(data, 5);
  }

  //! Receive something.
  int receive(std::string* data, int timesRetry) const {
    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
    setsockopt(_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    int rv = ::recv(_fd, _buf, RECIEVE_BUFFER_SIZE, 0);
    if (rv == -1) {
      LOG(DEBUG) << "Errno: " << errno << std::endl;
      if (errno == 11 && timesRetry > 0) {
        LOG(DEBUG) << "Retrying " << timesRetry-- << " times " << std::endl;
        return receive(data, timesRetry);
      }
    } else {
      LOG(DEBUG) << "Nof bytes received: " << rv << std::endl;
    }
    if (rv > 0) {
      *data = _buf;
    }
    return rv;
  }

  string getRequest() const {
    std::ostringstream os;
    char buf[512];
    for (;;) {
      auto recvsize = recv(_fd, buf, sizeof(buf) - 1, MSG_DONTWAIT);
//      std::cout << "recvsize: " << recvsize << std::endl;
      if (recvsize == -1) {
 //       std::cout << "errno: " << errno << std::endl;
        if (errno != EAGAIN && errno != EWOULDBLOCK) { break; }
        else { continue; }
      } else if (recvsize == 0) { break; } // properly closed connection.
      // Append
      buf[recvsize] = '\0';
      os << buf;
    }
    return os.str();
  }

  // Copied from online sources. Might be useful in the future.
//    void setNonBlocking(const bool val)
//    {
//      int opts = fcntl(_fd, F_GETFL);
//      if (opts < 0)
//      {
//        return;
//      }
//
//      if (val) opts = (opts | O_NONBLOCK);
//      else opts = (opts & ~O_NONBLOCK);
//
//      fcntl(_fd, F_SETFL, opts);
//    }

private:
  sockaddr_in _address;
  int _fd;
  char* _buf;;
};
}


