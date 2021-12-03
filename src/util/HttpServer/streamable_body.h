// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include "../streamable_generator.h"
#include "./beast.h"

namespace ad_utility::httpUtils::httpStreams {

/**
 * A message body represented by a stream_generator. This allows to use a
 * generator function to dynamically create a response.
 * Example usage:
 * http::response<streamable_body> response;
 * response.body() = generatorFunction();
 * response.prepare_payload();
 */
struct streamable_body {
  // Algorithm for retrieving buffers when serializing.
  class writer;

  // The type of the message::body member.
  // This determines which type response<streamable_body>::body() returns
  using value_type = ad_utility::stream_generator::stream_generator;
};

/** Algorithm for retrieving buffers when serializing.

    Objects of this type are created during serialization
    to extract the buffers representing the body.
*/
class streamable_body::writer {
  value_type& _body;

 public:
  // The type of buffer sequence returned by `get`.
  //
  using const_buffers_type = boost::asio::const_buffer;

  // Constructor.
  //
  // `h` holds the headers of the message we are
  // serializing, while `b` holds the body.
  //
  // The BodyWriter concept allows the writer to choose
  // whether to take the message by const reference or
  // non-const reference. Depending on the choice, a
  // serializer constructed using that body type will
  // require the same const or non-const reference to
  // construct.
  //
  // Readers which accept const messages usually allow
  // the same body to be serialized by multiple threads
  // concurrently, while readers accepting non-const
  // messages may only be serialized by one thread at
  // a time.
  //
  template <bool isRequest, class Fields>
  writer(boost::beast::http::header<isRequest, Fields>& h, value_type& b)
      : _body{b} {
    boost::ignore_unused(h);
  }

  // Initializer
  //
  // This is called before the body is serialized and
  // gives the writer a chance to do something that might
  // need to return an error code.
  //
  void init(boost::system::error_code& ec) {
    // The error_code specification requires that we
    // either set the error to some value, or set it
    // to indicate no error.
    //
    // We don't do anything fancy so set "no error"
    ec = {};
  }

  // This function is called zero or more times to
  // retrieve buffers. A return value of `boost::none`
  // means there are no more buffers. Otherwise,
  // the contained pair will have the next buffer
  // to serialize, and a `bool` indicating whether
  // or not there may be additional buffers.
  boost::optional<std::pair<const_buffers_type, bool>> get(
      boost::system::error_code& ec);
};

// This function is called repeatedly by the serializer to
// retrieve the buffers representing the body. Our strategy
// is to iterate over the generator to get the data step by step
inline auto streamable_body::writer::get(boost::system::error_code& ec)
    -> boost::optional<std::pair<const_buffers_type, bool>> {
  // Return the buffer to the caller.
  //
  // The second element of the pair indicates whether or
  // not there is more data. As long as there is some
  // unread bytes, there will be more data. Otherwise,
  // we set this bool to `false` so we will not be called
  // again.
  //
  // TODO handle exceptions appropriately
  std::string_view view = _body.next();
  ec = {};
  // we can safely pass away the data() pointer because
  // it's just referencing the memory inside the generator's promise
  // it won't be modified until the next call to _body.next()
  return {{
      const_buffers_type{view.data(), view.size()},
      _body.hasNext()  // `true` if there are more buffers.
  }};
}

static_assert(boost::beast::http::is_body<streamable_body>::value,
              "Body type requirements not met");

}  // namespace ad_utility::httpUtils::httpStreams
