// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_HTTP_STREAMABLE_BODY_H
#define QLEVER_SRC_UTIL_HTTP_STREAMABLE_BODY_H

#include <exception>
#include <string>

#include "util/Generator.h"
#include "util/Log.h"
#include "util/http/beast.h"

namespace ad_utility::httpUtils::httpStreams {

/**
 * A message body represented by a cppcoro::generator<std::string>. This allows
 * to use a generator function to dynamically create a response.
 *
 * Example usage:
 *
 * http::response<streamable_body> response;
 * // generatorFunction returns a cppcoro::generator<std::string>
 * response.body() = generatorFunction();
 * response.prepare_payload();
 */
struct streamable_body {
  // Algorithm for retrieving buffers when serializing.
  class writer;

  // The type of the message::body member.
  // This determines which type response<streamable_body>::body() returns
  using value_type = cppcoro::generator<std::string>;
};

/**
 * Algorithm for retrieving buffers when serializing.
 *
 * Objects of this type are created during serialization
 * to extract the buffers representing the body.
 */
class streamable_body::writer {
  value_type& _generator;
  value_type::iterator _iterator;
  std::string _storage;
  bool _first = true;

 public:
  // The type of buffer sequence returned by `get`.
  using const_buffers_type = boost::asio::const_buffer;

  /**
   * `header` holds the headers of the message we are
   * serializing, while `generator` holds the body.
   *
   * The BodyWriter concept allows the writer to choose
   * whether to take the message by const reference or
   * non-const reference. Depending on the choice, a
   * serializer constructed using that body type will
   * require the same const or non-const reference to
   * construct.
   *
   * Readers which accept const messages usually allow
   * the same body to be serialized by multiple threads
   * concurrently, while readers accepting non-const
   * messages may only be serialized by one thread at
   * a time.
   *
   * We need the non-const case here, because the one-shot
   * cppcoro::generator<std::string> conceptually can't allow const access.
   */
  template <bool isRequest, class Fields>
  writer([[maybe_unused]] boost::beast::http::header<isRequest, Fields>& header,
         value_type& generator)
      : _generator{generator} {}

  /**
   * This is called before the body is serialized and
   * gives the writer a chance to do something that might
   * need to return an error code.
   */
  void init(boost::system::error_code& ec) noexcept {
    // Set the error code to "no error" (default value).
    ec = {};
  }

  /**
   * This function is called zero or more times to
   * retrieve buffers. A return value of `boost::none`
   * means there are no more buffers. Otherwise,
   * the contained pair will have the next buffer
   * to serialize, and a `bool` indicating whether
   * or not there may be additional buffers.
   *
   * Our strategy is to iterate over the generator to get the data step by step.
   */
  boost::optional<std::pair<const_buffers_type, bool>> get(
      boost::system::error_code& ec) {
    // Return the buffer to the caller.
    //
    // The second element of the pair indicates whether or
    // not there is more data. As long as there is some
    // unread bytes, there will be more data. Otherwise,
    // we set this bool to `false` so we will not be called
    // again.
    //
    ec = {};
    try {
      if (_first) {
        // This is not done in init() to avoid the duplicate exception handling.
        _iterator = _generator.begin();
        _first = false;
      } else {
        _iterator++;
      }
      if (_iterator == _generator.end()) {
        return boost::none;
      }
      _storage = std::move(*_iterator);
      return {{
          const_buffers_type{_storage.data(), _storage.size()},
          true  // `true` if there are more buffers.
      }};
    } catch (const std::exception& e) {
      ec = {EPIPE, boost::system::generic_category()};
      AD_LOG_ERROR << "Failed to generate response:\n" << e.what() << std::endl;
      return boost::none;
    }
  }
};

static_assert(boost::beast::http::is_body<streamable_body>::value,
              "Body type requirements not met");

}  // namespace ad_utility::httpUtils::httpStreams

#endif  // QLEVER_SRC_UTIL_HTTP_STREAMABLE_BODY_H
