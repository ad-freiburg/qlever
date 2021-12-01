#pragma once

#include <algorithm>
#include <boost/assert.hpp>
#include <boost/beast/core/detail/config.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/core/file_base.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/optional.hpp>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

// coroutines are still experimental in clang, adapt the appropriate namespaces.
#ifdef __clang__
#include <experimental/coroutine>
#else
#include <coroutine>
#endif

namespace http_streams {

// coroutines are still experimental in clang, adapt the appropriate namespaces.
#ifdef __clang__
using std::suspend_always = std::experimental::suspend_always;
using std::suspend_never = std::experimental::suspend_never;
using std::coroutine_handle = std::experimental::coroutine_handle;
#endif

template <typename T>
concept Streamable = requires(T x, std::ostream& os) {
  os << x;
};

class stream_generator;

namespace detail {
class suspend_sometimes {
  const bool suspend;

 public:
  explicit suspend_sometimes(const bool suspend) : suspend(suspend) {}
  bool await_ready() const noexcept { return !suspend; }
  constexpr void await_suspend(std::coroutine_handle<>) const noexcept {}
  constexpr void await_resume() const noexcept {}
};

class stream_generator_promise {
 public:
  using value_type = std::string_view;
  using reference_type = value_type;
  using pointer_type = value_type*;

  stream_generator_promise() = default;

  stream_generator get_return_object() noexcept;

  constexpr std::suspend_always initial_suspend() const noexcept { return {}; }
  constexpr std::suspend_always final_suspend() const noexcept { return {}; }

  template <Streamable T>
  suspend_sometimes yield_value(T&& value) noexcept {
    // boost::iostreams::filtering_stream fs;
    // fs.push(gzip_compressor());
    // fs.push(m_value);

    if (isBufferLargeEnough()) {
      m_value.str("");
      m_value.clear();
    }
    m_value << value;
    if (isBufferLargeEnough()) {
      return suspend_sometimes(true);
    }
    return suspend_sometimes(false);
  }

  void unhandled_exception() { m_exception = std::current_exception(); }

  void return_void() {}

  reference_type value() const noexcept {
    // TODO fix reference ?
    return m_value.view();
  }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  std::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (m_exception) {
      std::rethrow_exception(m_exception);
    }
  }

 private:
  std::ostringstream m_value;
  std::exception_ptr m_exception;

  bool isBufferLargeEnough() {
    // TODO number is arbitrary, fine-tune in the future
    return m_value.view().length() >= 1000;
  }
};

struct generator_sentinel {};

class stream_generator_iterator {
  using coroutine_handle = std::coroutine_handle<stream_generator_promise>;

 public:
  using iterator_category = std::input_iterator_tag;
  // What type should we use for counting elements of a potentially infinite
  // sequence?
  using difference_type = std::ptrdiff_t;
  using value_type = typename stream_generator_promise::value_type;
  using reference = typename stream_generator_promise::reference_type;
  using pointer = typename stream_generator_promise::pointer_type;

  // Iterator needs to be default-constructible to satisfy the Range concept.
  stream_generator_iterator() noexcept : m_coroutine(nullptr) {}

  explicit stream_generator_iterator(coroutine_handle coroutine) noexcept
      : m_coroutine(coroutine) {}

  friend bool operator==(const stream_generator_iterator& it,
                         generator_sentinel) noexcept {
    return !it.m_coroutine || it.m_coroutine.done();
  }

  friend bool operator!=(const stream_generator_iterator& it,
                         generator_sentinel s) noexcept {
    return !(it == s);
  }

  friend bool operator==(generator_sentinel s,
                         const stream_generator_iterator& it) noexcept {
    return (it == s);
  }

  friend bool operator!=(generator_sentinel s,
                         const stream_generator_iterator& it) noexcept {
    return it != s;
  }

  stream_generator_iterator& operator++() {
    m_coroutine.resume();
    if (m_coroutine.done()) {
      m_coroutine.promise().rethrow_if_exception();
    }

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  void operator++(int) { (void)operator++(); }

  reference operator*() const noexcept { return m_coroutine.promise().value(); }

 private:
  coroutine_handle m_coroutine;
};
}  // namespace detail

class [[nodiscard]] stream_generator {
 public:
  using promise_type = detail::stream_generator_promise;
  using iterator = detail::stream_generator_iterator;
  using value_type = typename iterator::value_type;

  stream_generator() noexcept : m_coroutine(nullptr) {}

  stream_generator(stream_generator&& other) noexcept
      : m_coroutine(other.m_coroutine) {
    other.m_coroutine = nullptr;
  }

  stream_generator(const stream_generator& other) = delete;

  ~stream_generator() {
    if (m_coroutine) {
      m_coroutine.destroy();
    }
  }

  stream_generator& operator=(stream_generator other) noexcept {
    swap(other);
    return *this;
  }

  iterator begin() {
    if (m_coroutine) {
      m_coroutine.resume();
      if (m_coroutine.done()) {
        m_coroutine.promise().rethrow_if_exception();
      }
    }

    return iterator{m_coroutine};
  }

  detail::generator_sentinel end() noexcept {
    return detail::generator_sentinel{};
  }

  void swap(stream_generator& other) noexcept {
    std::swap(m_coroutine, other.m_coroutine);
  }

 private:
  friend class detail::stream_generator_promise;

  explicit stream_generator(
      std::coroutine_handle<promise_type> coroutine) noexcept
      : m_coroutine(coroutine) {}

  std::coroutine_handle<promise_type> m_coroutine;
};

namespace detail {
inline stream_generator stream_generator_promise::get_return_object() noexcept {
  using coroutine_handle = std::coroutine_handle<stream_generator_promise>;
  return stream_generator{coroutine_handle::from_promise(*this)};
}
}  // namespace detail

/**
 * A message body represented by a stream_generator
 */
struct streamable_body {
  // Algorithm for retrieving buffers when serializing.
  class writer;

  // The type of the @ref message::body member.
  using value_type = stream_generator;
};

/** Algorithm for retrieving buffers when serializing.

    Objects of this type are created during serialization
    to extract the buffers representing the body.
*/
class streamable_body::writer {
  value_type& body_;
  detail::stream_generator_iterator& iterator;
  std::string stringBuffer;
  // provide a default iterator object until initialisation
  inline static detail::stream_generator_iterator default_iter;

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
  writer(boost::beast::http::header<isRequest, Fields>& h, value_type& b);

  // Initializer
  //
  // This is called before the body is serialized and
  // gives the writer a chance to do something that might
  // need to return an error code.
  //
  void init(boost::system::error_code& ec);

  // This function is called zero or more times to
  // retrieve buffers. A return value of `boost::none`
  // means there are no more buffers. Otherwise,
  // the contained pair will have the next buffer
  // to serialize, and a `bool` indicating whether
  // or not there may be additional buffers.
  boost::optional<std::pair<const_buffers_type, bool>> get(
      boost::system::error_code& ec);
};

template <bool isRequest, class Fields>
inline streamable_body::writer::writer(
    boost::beast::http::header<isRequest, Fields>& h, value_type& b)
    : body_(b), iterator(default_iter) {
  boost::ignore_unused(h);
}

inline void streamable_body::writer::init(boost::system::error_code& ec) {
  iterator = body_.begin();
  // TODO handle error codes?
  // The error_code specification requires that we
  // either set the error to some value, or set it
  // to indicate no error.
  //
  // We don't do anything fancy so set "no error"
  ec = {};
}

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
  // TODO perhaps re-use buffers by swapping?
  stringBuffer = *iterator;
  if (iterator != body_.end()) {
    iterator++;
  }
  // TODO handle exceptions appropriately
  // TODO is chunked transfer encoding implemented correctly?
  ec = {};
  return {{
      const_buffers_type{stringBuffer.data(),
                         stringBuffer.size()},  // buffer to return.
      iterator != body_.end()  // `true` if there are more buffers.
  }};
}

static_assert(boost::beast::http::is_body<streamable_body>::value,
              "Body type requirements not met");

}  // namespace http_streams
