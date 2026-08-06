//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// `sendHttpOrHttpsRequest` (declared in `HttpClient.h`) for
// Emscripten/WebAssembly, which has no raw TCP sockets and hence cannot use the
// Boost.Beast-based implementation from `HttpClient.cpp`. We use the `fetch`
// API of the surrounding JavaScript environment instead, which makes the
// operations that need HTTP requests (in particular `SERVICE` and `LOAD`) work.
//
// `fetch` is asynchronous, but our callers are synchronous, and a thread that
// blocks cannot run the event loop that would settle the promises of its own
// `fetch` call. A dedicated thread (`networkThread`) therefore performs the
// requests and never blocks, while the requesting thread waits for it.
// Emscripten implements its synchronous filesystem API on top of the
// asynchronous OPFS API in the same way, see `ProxyWorker` in
// `system/lib/wasmfs/thread_utils.h`.
//
// All the state of a request lives in one `Request` object that the two sides
// share: `qleverFetchStart` starts a request, the `EMSCRIPTEN_KEEPALIVE`
// functions report its response through the queue of that object, and
// `Request::wakeUpJavaScript` is how the requesting thread asks the JavaScript
// side to look at the object again.
//
// Differences to the native implementation:
//
// * Requests must not be issued from the main thread of a browser, where
//   blocking is not allowed. This is checked, so that the result is an error
//   and not a frozen page.
// * In a browser, CORS applies, and `Content-Type` is the only response header
//   that is reliably readable for a cross-origin response.
// * `maxRedirects` only distinguishes 0 ("don't follow", a redirect then fails
//   the request, as natively) from larger values ("follow up to the limit of
//   the JavaScript environment"). Following redirects ourselves would need
//   `redirect: "manual"`, whose response is opaque in a browser, without a
//   readable `Location`.
// * A browser does not let us set `User-Agent`.
// * A request that fails is reported when the consumer asks for the next chunk,
//   and whatever was received but not yet consumed is discarded. A response
//   that fails right after its head may therefore make `sendHttpOrHttpsRequest`
//   itself throw, rather than the reading of the body.

#if defined(__EMSCRIPTEN__) && !defined(QLEVER_REDUCED_FEATURE_SET_FOR_CPP17)

#include <absl/strings/str_cat.h>
#include <emscripten/em_js.h>
#include <emscripten/emscripten.h>
#include <emscripten/proxying.h>
#include <emscripten/threading.h>
#include <pthread.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <variant>

#include "util/Exception.h"
#include "util/ThreadSafeQueue.h"
#include "util/UninitializedAllocator.h"
#include "util/http/HttpClient.h"

namespace {

namespace http = boost::beast::http;
using ad_utility::data_structures::ThreadSafeQueue;
using ad_utility::data_structures::TryPushResult;

// The number of body chunks that the JavaScript side may read ahead of the
// consumer, and the largest chunk it may hand over at once. Their product (plus
// one chunk that is being handed over) bounds how much of a response is
// buffered, however large it is.
constexpr size_t MAX_CHUNKS_AHEAD = 8;
constexpr int MAX_CHUNK_SIZE = 1 << 17;  // 128 KiB

// The capacity of the buffers for the short strings of a response, see
// `StringFromJavaScript`.
constexpr int STRING_CAPACITY = 1024;

// The interval at which the requesting thread wakes up while it waits for the
// JavaScript side, in order to check whether the query has been cancelled.
constexpr std::chrono::milliseconds CANCELLATION_CHECK_INTERVAL{100};

// A buffer for one of the short strings that the JavaScript side reports: the
// value of a response header we are interested in, or the message of an error.
// A fixed capacity means that such a string needs neither an allocation nor an
// owner across the language boundary; a longer one is truncated.
class StringFromJavaScript {
 private:
  std::array<char, STRING_CAPACITY> characters_{};

 public:
  // The address that the JavaScript side writes to, see `writeString` there.
  char* buffer() { return characters_.data(); }
  static constexpr int capacity() { return STRING_CAPACITY; }

  // What was written, or an empty string if nothing was. The JavaScript side
  // always writes a null terminator, and the buffer starts out zeroed.
  std::string value() const { return std::string{characters_.data()}; }
};

// The head of a response, which the JavaScript side reports as soon as it has
// received it, before any of the body.
struct ResponseHead {
  http::status status_;
  std::string contentType_;
  std::string location_;
};

// A chunk of a response body. Uninitialized, because the JavaScript side
// overwrites it completely right after room has been made for it.
using BodyChunk = ad_utility::UninitializedVector<std::byte>;

// What the JavaScript side hands over through the queue of a request: first the
// head of the response, then the chunks of its body.
using ResponseEvent = std::variant<ResponseHead, BodyChunk>;

// The head or the chunk that the given event has to be at this point of the
// protocol: the head comes first, and everything after it is a chunk.
template <typename Kind>
Kind& eventAs(ResponseEvent& event) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<Kind>(event));
  return std::get<Kind>(event);
}

// All the state of a single HTTP request, shared by the requesting thread and
// the JavaScript code that runs on the network thread.
struct Request {
  // What is requested. Set by the requesting thread before the request is
  // started, and only read afterwards.
  std::string url_;
  std::string method_;
  std::string requestBody_;
  std::string contentTypeHeader_;
  std::string acceptHeader_;
  bool followRedirects_ = false;

  // The response, as far as it has been received. This is the only channel from
  // the JavaScript side to the requesting thread, and its capacity is the back
  // pressure.
  ThreadSafeQueue<ResponseEvent> events_{MAX_CHUNKS_AHEAD};

  // The buffers that the JavaScript side writes to, and the chunk it is
  // currently handing over (see `qleverFetchChunkBuffer`). These need no
  // synchronization, because only the network thread ever touches them: their
  // contents reach the requesting thread through the queue above. The pending
  // chunk is already a `ResponseEvent`, so that handing it over needs no
  // conversion, which would move it out even if the queue turns out to be full.
  StringFromJavaScript contentType_;
  StringFromJavaScript location_;
  StringFromJavaScript errorMessage_;
  ResponseEvent pendingChunk_{BodyChunk{}};

  // Whether the requesting thread has lost interest in the response, see
  // `abandon` and `qleverFetchIsAbandoned`.
  std::atomic<bool> abandoned_{false};

  // The number of wake-ups that have been sent to the JavaScript side, which
  // waits for this to change. That side sees it as a plain 32-bit integer, and
  // bumps it itself to end the watch in `qleverFetchStart`.
  std::atomic<int32_t> wakeUps_{0};
  static_assert(sizeof(std::atomic<int32_t>) == sizeof(int32_t));
  static_assert(std::atomic<int32_t>::is_always_lock_free);
  int32_t* wakeUpAddress() { return reinterpret_cast<int32_t*>(&wakeUps_); }

  // Tell the JavaScript side to look at the request again, because there is
  // space in the queue now or because the request was abandoned. Doing so while
  // it is not waiting is harmless: it always rechecks what it waited for.
  void wakeUpJavaScript() {
    wakeUps_.fetch_add(1);
    // The JavaScript side may be waiting twice (for space and for an abort),
    // hence not just for a single waiter.
    emscripten_futex_wake(&wakeUps_, std::numeric_limits<int>::max());
  }

  // Give up on the response: nothing is accepted into the queue anymore, and
  // the JavaScript side aborts the `fetch` as soon as it notices.
  void abandon() {
    abandoned_.store(true);
    events_.finish();
    wakeUpJavaScript();
  }
};

// The reference of the JavaScript side to a request: a `shared_ptr` on the
// heap, which keeps the request alive for as long as that side may still access
// it, and which `qleverFetchOnEnd` deletes. The JavaScript side treats it as an
// opaque address.
using RequestHandle = std::shared_ptr<Request>*;

// The request that the given handle refers to.
Request& requestFor(void* handle) {
  return **static_cast<RequestHandle>(handle);
}
}  // namespace

// The functions in this block are the ones that the JavaScript code below calls
// while it performs a request; `EMSCRIPTEN_KEEPALIVE` makes them available
// there under their name with a leading underscore. They all run on the network
// thread, and they all take the handle of the request (see `RequestHandle`).
extern "C" {

// Report the head of the response, whose header values the JavaScript side has
// written to `Request::contentType_` and `Request::location_`. Returns whether
// it should go on at all, which it should not if the requesting thread has
// given up in the meantime.
EMSCRIPTEN_KEEPALIVE bool qleverFetchOnHead(void* handle, int status) {
  Request& request = requestFor(handle);
  ResponseEvent head =
      ResponseHead{static_cast<http::status>(status),
                   request.contentType_.value(), request.location_.value()};
  TryPushResult result = request.events_.tryPush(std::move(head));
  // The head is the first thing that is pushed, so the queue cannot be full.
  AD_CORRECTNESS_CHECK(result != TryPushResult::Full);
  return result == TryPushResult::Pushed;
}

// Make room for a body chunk of `size` bytes and return the address at which
// the JavaScript side has to write it, before handing it over with
// `qleverFetchPushChunk`. Writing the bytes to the place where they are
// consumed right away is what keeps the body from being copied twice.
EMSCRIPTEN_KEEPALIVE void* qleverFetchChunkBuffer(void* handle, int size) {
  AD_CORRECTNESS_CHECK(size >= 0 && size <= MAX_CHUNK_SIZE);
  BodyChunk& chunk = eventAs<BodyChunk>(requestFor(handle).pendingChunk_);
  chunk.resize(static_cast<size_t>(size));
  return chunk.data();
}

// Hand the chunk that was written to the buffer above over to the consumer, and
// report whether it was accepted. If it was not, then either the queue is full
// or the requesting thread has given up (`qleverFetchIsAbandoned` tells the two
// apart); the chunk stays in the buffer either way, so handing it over again
// later does not copy it again.
EMSCRIPTEN_KEEPALIVE bool qleverFetchPushChunk(void* handle) {
  Request& request = requestFor(handle);
  return request.events_.tryPush(std::move(request.pendingChunk_)) ==
         TryPushResult::Pushed;
}

// Whether the requesting thread has given up on the response, so that the
// JavaScript side should stop and abort the `fetch`. It asks this when a chunk
// was not accepted, and it also watches it while it waits for the endpoint,
// where nothing but an `AbortController` can interrupt it.
EMSCRIPTEN_KEEPALIVE bool qleverFetchIsAbandoned(void* handle) {
  return requestFor(handle).abandoned_.load();
}

// Report that the JavaScript side is done with the request, either because the
// body has ended or because the request `failed`, in which case it has written
// the reason to `Request::errorMessage_`. This releases the reference of the
// JavaScript side, which must not touch the request afterwards.
EMSCRIPTEN_KEEPALIVE void qleverFetchOnEnd(void* handle, bool failed) {
  Request& request = requestFor(handle);
  if (failed) {
    request.events_.pushException(std::make_exception_ptr(std::runtime_error{
        absl::StrCat("The HTTP request to <", request.url_,
                     "> failed: ", request.errorMessage_.value())}));
  } else {
    request.events_.finish();
  }
  delete static_cast<RequestHandle>(handle);
}
}  // extern "C"

// NOTE: `clang-format` is disabled below because it breaks JavaScript (it turns
// `===` into `== =`).
// clang-format off

// Perform a whole request: fetch the URL, report the head of the response, then
// hand the chunks of its body over until it ends, fails, or the requesting
// thread gives up. Runs on the network thread and returns as soon as the `fetch`
// is under way; everything else happens in its callbacks.
//
// The parameters are everything the JavaScript side needs: what to request
// (`url` to `followRedirects`), the `handle` that names the request in the calls
// back into C++, the address of its wake-up counter, and the buffers to write
// the short strings of the response to. Note that `MEMORY64` passes pointers as
// `BigInt`, which cannot index the heap views, hence the `Number(...)`
// conversions (which are no-ops without it). `handle` is passed on to the
// functions above unchanged, because they expect it in exactly the form in
// which it arrived here.
EM_JS(void, qleverFetchStart,
      (const char* url, const char* method, const char* requestBody,
       int requestBodySize, const char* contentTypeHeader,
       const char* acceptHeader, bool followRedirects, void* handle,
       int32_t* wakeUps, int maxChunkSize, char* contentType, char* location,
       char* errorMessage, int stringCapacity),
      {
        // Views of the WebAssembly memory. They are built fresh on every use,
        // because Emscripten's cached `HEAPU8` can be stale after the memory
        // grew; that is cheap, because with pthreads the memory is a
        // `SharedArrayBuffer`, which grows in place.
        const bytes = () => new Uint8Array(HEAPU8.buffer);
        const integers = () => new Int32Array(HEAPU8.buffer);

        const readString = (buffer) => UTF8ArrayToString(bytes(), Number(buffer));
        // `stringToUTF8Array` truncates to the capacity of the buffer (never in
        // the middle of a UTF-8 sequence) and always writes a null terminator.
        const writeString = (buffer, value) => {
          const text = value === null || value === undefined ? "" : String(value);
          stringToUTF8Array(text, bytes(), Number(buffer), stringCapacity);
        };

        // How the requesting thread reaches us: it bumps the counter at
        // `wakeUps` whenever there may be something new for us to see (space in
        // the queue of the request, or the request being abandoned), see
        // `Request::wakeUpJavaScript`.
        const wakeUpIndex = Number(wakeUps) / 4;
        const wakeUpCount = () => Atomics.load(integers(), wakeUpIndex);
        const wakeUpOurselves = () => {
          const array = integers();
          Atomics.add(array, wakeUpIndex, 1);
          Atomics.notify(array, wakeUpIndex);
        };

        // Call `attempt` until it returns something other than `RETRY`, waiting
        // for a wake-up in between. Reading the counter before each attempt is
        // what makes this race-free: a wake-up that arrives while we attempt
        // changes the counter, so that the wait afterwards returns right away.
        // We wait asynchronously, because blocking would stop the event loop
        // that our own `fetch` needs to make progress.
        //
        // `RETRY` is the "not yet, wake me up when something changed" of an
        // `attempt`, and has to be distinguishable from every value that an
        // `attempt` may legitimately return. A `Symbol` is exactly that: a value
        // that is equal to no other value in the program, so no `attempt` can
        // return it by accident.
        const RETRY = Symbol("retry");
        const retryOnWakeUp = async (attempt) => {
          while (true) {
            const seen = wakeUpCount();
            const result = attempt();
            if (result !== RETRY) {
              return result;
            }
            const wait = Atomics.waitAsync(integers(), wakeUpIndex, seen);
            if (wait.async) {
              await wait.value;
            }
          }
        };

        const controller = new AbortController();
        let responseIsDone = false;

        // Watch for the requesting thread giving up, until the response is done.
        // This has to be able to abort in the middle of a chunk, not only in
        // between two of them: an endpoint that stops responding must not keep a
        // cancelled query waiting, and nothing but an `AbortController` can
        // interrupt a pending read.
        const abortWhenAbandoned = () => retryOnWakeUp(() => {
          if (_qleverFetchIsAbandoned(handle)) {
            controller.abort();
          } else if (!responseIsDone) {
            return RETRY;
          }
        });

        // Hand one chunk of the body over to the consumer, waiting for space in
        // the queue of the request if there is none. Resolves to false if the
        // requesting thread has given up, in which case we stop reading.
        const pushChunk = (chunk) => {
          // Make room first and only then build the view to write through:
          // making room allocates, which can grow the memory.
          const buffer = Number(_qleverFetchChunkBuffer(handle, chunk.length));
          bytes().set(chunk, buffer);
          return retryOnWakeUp(() => {
            if (_qleverFetchPushChunk(handle)) {
              return true;
            }
            // The chunk stays in the buffer, so handing it over again later does
            // not copy it again. It was not accepted because the queue is full,
            // unless the requesting thread has given up.
            return _qleverFetchIsAbandoned(handle) ? false : RETRY;
          });
        };

        // Hand a chunk over in pieces of at most `maxChunkSize` bytes, so that
        // the memory a response needs stays bounded however large it is.
        const pushChunkInPieces = async (chunk) => {
          let rest = chunk;
          while (rest.length > 0) {
            const size = Math.min(rest.length, maxChunkSize);
            if (!(await pushChunk(rest.subarray(0, size)))) {
              return false;
            }
            rest = rest.subarray(size);
          }
          return true;
        };

        const requestOptions = () => {
          const options = {
            method : readString(method),
            headers : {},
            signal : controller.signal,
            // The JavaScript environment follows the redirects; if we are not
            // allowed to follow them, a redirect makes the request fail.
            redirect : followRedirects ? "follow" : "error"
          };
          const accept = readString(acceptHeader);
          if (accept) {
            options.headers["Accept"] = accept;
          }
          if (requestBodySize > 0) {
            const contentType = readString(contentTypeHeader);
            if (contentType) {
              options.headers["Content-Type"] = contentType;
            }
            // Copy the body out of the WebAssembly memory: `fetch` rejects
            // bodies that are backed by a `SharedArrayBuffer`.
            const start = Number(requestBody);
            options.body = new Uint8Array(
                bytes().subarray(start, start + requestBodySize));
          }
          return options;
        };

        const streamResponse = async () => {
          const response = await fetch(readString(url), requestOptions());
          writeString(contentType, response.headers.get("content-type"));
          writeString(location, response.headers.get("location"));
          if (!_qleverFetchOnHead(handle, response.status)) {
            return;
          }
          // A `204 No Content` for example has no body at all.
          if (!response.body) {
            return;
          }
          const reader = response.body.getReader();
          while (true) {
            const chunk = await reader.read();
            if (chunk.done || !(await pushChunkInPieces(chunk.value))) {
              return;
            }
          }
        };

        // The message of an error thrown by `fetch` is often unhelpful (Node.js
        // only says "fetch failed"); the interesting part is in its `cause`,
        // which typically has a `code` such as `ECONNREFUSED`.
        const describeError = (error) => {
          if (!error) {
            return "unknown error";
          }
          const message = error.message || String(error);
          const cause = error.cause;
          if (!cause) {
            return message;
          }
          const code = cause.code ? `${cause.code}: ` : "";
          return `${message} (${code}${cause.message || String(cause)})`;
        };

        // Report the outcome and release the handle. The C++ side may destroy
        // the request as soon as we do, so the watch below, which uses the
        // handle, has to have ended before.
        const finish = async (failed, error) => {
          // Let the watch look again, so that it sees that it is done and ends.
          responseIsDone = true;
          wakeUpOurselves();
          await watching;
          // A no-op unless we stopped early, in which case this closes the
          // connection of a response that nobody is going to read.
          controller.abort();
          if (failed) {
            writeString(errorMessage, describeError(error));
          }
          _qleverFetchOnEnd(handle, failed);
        };

        // The watch should never fail, but if it does (which would be a bug in
        // the code above), then it must not take the whole request down with it:
        // `finish` awaits it, and an exception there would skip the report that
        // the requesting thread is waiting for, leaving it waiting forever. So
        // we log the failure and go on. Handling it here rather than at the
        // `await` also keeps it from being an unhandled rejection in the
        // meantime.
        const watching = abortWhenAbandoned().catch(
            (error) => console.error(
                "QLever: watching for an abandoned HTTP request failed:",
                describeError(error)));
        streamResponse().then(() => finish(false, null),
                              (error) => finish(true, error));
      });

// Whether the JavaScript environment provides the `fetch` function that the code
// above performs the requests with.
EM_JS(bool, hasFetch, (void), {
  return typeof fetch === "function";
});

// Whether the JavaScript environment provides `Atomics.waitAsync`, which the
// code above needs in order to wait without blocking its thread.
EM_JS(bool, hasAtomicsWaitAsync, (void), {
  return typeof Atomics.waitAsync === "function";
});

// Whether the calling thread is the main thread of a browser, where blocking is
// not allowed. Note that the main thread of Node.js may block, and that Web
// Workers (including the threads that Emscripten's pthreads are) may block.
EM_JS(bool, isBrowserMainThread, (void), {
  return typeof WorkerGlobalScope === "undefined" && typeof window !== "undefined";
});

// The JavaScript code above uses these functions of Emscripten's JavaScript
// runtime, which have to be kept alive explicitly.
EM_JS_DEPS(qleverFetch, "$stringToUTF8Array,$UTF8ArrayToString");

// clang-format on

namespace {

// The thread on which the requests are performed, created on first use and then
// alive until the process exits. We deliberately don't use the main runtime
// thread (although it always has a live event loop), because it might itself be
// the thread that waits, for example when a query is run directly from the main
// thread of a Node.js application.
pthread_t networkThread() {
  static pthread_t thread = []() {
    std::thread thread{[]() {
      // Keep the Web Worker of this thread (and hence its event loop) alive, so
      // that it can run the proxied calls and the `fetch` callbacks they start.
      // In contrast to Emscripten's `ProxyWorker` we don't wait for the thread
      // to have started, which would deadlock on a browser's main thread; the
      // proxied calls simply run once it is up.
      emscripten_exit_with_live_runtime();
    }};
    // The thread never returns, so it can neither be joined nor be a
    // `std::jthread`. Emscripten's proxying API identifies it by its
    // `pthread_t`, which stays valid for as long as it runs.
    pthread_t handle = thread.native_handle();
    thread.detach();
    return handle;
  }();
  return thread;
}

// Hand a request over to the JavaScript side, see `qleverFetchStart`. Runs on
// the network thread, which is the only thread that may call into that code.
void runRequest(void* handle) {
  Request& request = requestFor(handle);
  qleverFetchStart(
      request.url_.c_str(), request.method_.c_str(),
      request.requestBody_.data(),
      static_cast<int>(request.requestBody_.size()),
      request.contentTypeHeader_.c_str(), request.acceptHeader_.c_str(),
      request.followRedirects_, handle, request.wakeUpAddress(), MAX_CHUNK_SIZE,
      request.contentType_.buffer(), request.location_.buffer(),
      request.errorMessage_.buffer(), StringFromJavaScript::capacity());
}

// The reference of the C++ side to a request. When it is destroyed, the request
// is abandoned: the JavaScript side stops reading the response and aborts the
// `fetch`, so that a response nobody reads does not keep its connection open.
// The JavaScript side holds a reference of its own, hence the `shared_ptr`:
// whichever of the two sides is done last destroys the request.
class RequestOwner {
 private:
  std::shared_ptr<Request> request_ = std::make_shared<Request>();

 public:
  RequestOwner() = default;
  RequestOwner(RequestOwner&&) = default;
  RequestOwner(const RequestOwner&) = delete;
  RequestOwner& operator=(RequestOwner&&) = delete;
  RequestOwner& operator=(const RequestOwner&) = delete;
  ~RequestOwner() {
    // Null if this owner was moved from, which happens when the request is
    // handed over to the generator of the body below.
    if (request_ != nullptr) {
      request_->abandon();
    }
  }

  Request& operator*() const { return *request_; }
  Request* operator->() const { return request_.get(); }

  // Start the request on the network thread. Throws if that thread cannot be
  // reached, in which case nothing was started.
  void start() const {
    // The reference of the JavaScript side, which `qleverFetchOnEnd` releases.
    auto* handle = new std::shared_ptr<Request>{request_};
    // The system queue is the one that every thread with a live event loop (see
    // `networkThread`) processes automatically.
    if (!emscripten_proxy_async(emscripten_proxy_get_system_queue(),
                                networkThread(), &runRequest, handle)) {
      delete handle;
      AD_THROW("Could not reach the thread that performs the HTTP requests");
    }
  }
};

// Wait for the next event of the response and return it, or `nullopt` if the
// body has ended. Throws if the request failed, or if the query was cancelled
// while we were waiting.
std::optional<ResponseEvent> nextEvent(
    Request& request, const ad_utility::SharedCancellationHandle& handle) {
  auto event = request.events_.pop(CANCELLATION_CHECK_INTERVAL,
                                   [&handle]() { handle->throwIfCancelled(); });
  // There is space in the queue now, so let the JavaScript side go on if it
  // stopped because there was none.
  if (event.has_value()) {
    request.wakeUpJavaScript();
  }
  return event;
}

// Read the response body chunk by chunk from the queue of the request. A
// yielded chunk is only valid until the next iteration, exactly as in the
// native implementation. The request is a parameter rather than a local
// variable, because a `cppcoro::generator` that is destroyed without ever being
// iterated destroys its parameters but never runs its body, which is how a
// response that is not read at all still abandons its request.
cppcoro::generator<ql::span<std::byte>> readResponseBody(
    RequestOwner request, ad_utility::SharedCancellationHandle handle) {
  while (auto event = nextEvent(*request, handle)) {
    co_yield ql::span<std::byte>{eventAs<BodyChunk>(event.value())};
  }
}
}  // namespace

// ____________________________________________________________________________
HttpOrHttpsResponse sendHttpOrHttpsRequest(
    const ad_utility::httpUtils::Url& url,
    ad_utility::SharedCancellationHandle handle, const http::verb& method,
    std::string_view requestData, std::string_view contentTypeHeader,
    std::string_view acceptHeader, size_t maxRedirects) {
  // The handle is dereferenced while we wait for the response, exactly as in
  // the native implementation.
  AD_CONTRACT_CHECK(handle != nullptr);
  if (!hasFetch()) {
    throw std::runtime_error(
        "HTTP requests from WebAssembly require the `fetch` function, which "
        "this JavaScript environment does not provide");
  }
  // Without this, the JavaScript side would have to block, and both sides would
  // then wait for each other forever.
  if (!hasAtomicsWaitAsync()) {
    throw std::runtime_error(
        "HTTP requests from WebAssembly require `Atomics.waitAsync`, which "
        "this JavaScript environment does not provide");
  }
  // This would wait for a response that can never arrive.
  if (isBrowserMainThread()) {
    throw std::runtime_error(absl::StrCat(
        "The HTTP request to <", url.asString(),
        "> was issued from the main thread of a browser, where waiting for "
        "the response is not possible. Run QLever in a Web Worker (which is "
        "required for its potentially long-running operations anyway)."));
  }
  AD_CORRECTNESS_CHECK(requestData.size() <=
                       static_cast<size_t>(std::numeric_limits<int>::max()));

  RequestOwner request;
  request->url_ = url.asString();
  request->method_ = http::to_string(method);
  request->requestBody_ = requestData;
  request->contentTypeHeader_ = contentTypeHeader;
  request->acceptHeader_ = acceptHeader;
  request->followRedirects_ = maxRedirects > 0;
  request.start();

  // Wait for the head of the response; a request that fails throws here.
  std::optional<ResponseEvent> event = nextEvent(*request, handle);
  AD_CORRECTNESS_CHECK(event.has_value());
  ResponseHead& head = eventAs<ResponseHead>(event.value());
  return {.status_ = head.status_,
          .contentType_ = std::move(head.contentType_),
          .location_ = std::move(head.location_),
          .body_ = readResponseBody(std::move(request), std::move(handle))};
}

#endif  // defined(__EMSCRIPTEN__) && !defined(QLEVER_REDUCED_FEATURE_SET...)
