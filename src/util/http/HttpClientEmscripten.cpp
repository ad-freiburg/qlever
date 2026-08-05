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
// requests and never blocks, while the requesting thread waits for it. The two
// communicate via a ring buffer in the shared WebAssembly memory: only one call
// crosses the C++/JavaScript boundary per request, the body is streamed through
// the slots, and the capacity of the ring buffer is the back pressure.
// Emscripten implements its synchronous filesystem API on top of the
// asynchronous OPFS API in the same way, see `ProxyWorker` in
// `system/lib/wasmfs/thread_utils.h`.
//
// Differences to the native implementation:
//
// 1. Requests must not be issued from the main thread of a browser, where
//    blocking is not allowed. This is checked, so that the result is an error
//    and not a frozen page.
// 2. In a browser, CORS applies, and `Content-Type` is the only response header
//    that is reliably readable for a cross-origin response.
// 3. Redirects are followed by the JavaScript environment; `maxRedirects` only
//    distinguishes 0 ("don't follow", a redirect then fails the request, as
//    natively) from larger values ("follow up to the limit of the
//    environment"). Following them ourselves would need `redirect: "manual"`,
//    whose response is opaque in a browser, without a readable `Location`.
// 4. A browser does not let us set `User-Agent`.

#if defined(__EMSCRIPTEN__) && !defined(QLEVER_REDUCED_FEATURE_SET_FOR_CPP17)

#include <absl/strings/str_cat.h>
#include <emscripten/em_js.h>
#include <emscripten/emscripten.h>
#include <emscripten/proxying.h>
#include <emscripten/threading.h>
#include <pthread.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/http/HttpClient.h"
#include "util/json.h"

namespace {

namespace http = boost::beast::http;

// The ring buffer for the response body: the number of chunks that may be
// received ahead of the consumer, and the maximum size of a single chunk.
constexpr int RING_SLOTS = 8;
constexpr int SLOT_CAPACITY = 1 << 17;  // 128 KiB

// The capacity of the buffer for the metadata of a response (the headers we are
// interested in, or an error message), which the JavaScript side writes as
// JSON.
constexpr int METADATA_CAPACITY = 8192;

// The interval at which the requesting thread wakes up while it waits for the
// JavaScript side, in order to check whether the query has been cancelled.
constexpr double CANCELLATION_CHECK_INTERVAL_MS = 100;

// How long we wait for the JavaScript side to react to an abort. Aborting
// settles its promises immediately, so this is only reached if something is
// seriously wrong.
constexpr double ABORT_TIMEOUT_MS = 5000;

// The 32-bit integers via which the two sides communicate. The `Field` enum and
// the `FIELD` object of the JavaScript code below are both generated from this
// list, so they cannot drift apart.
//
// EVENTS is incremented whenever the JavaScript side changes anything below,
// and is the only address the requesting thread ever waits on, which rules out
// lost wakeups. HEADERS_READY (status code and metadata are valid), FINISHED
// (the JavaScript side is done and will not touch the state again) and FAILED
// (the error message is in the metadata) report the progress. ABORTED is set by
// the requesting thread to abort, see `RequestState::abort`. WRITE_COUNT and
// READ_COUNT count the body chunks written and consumed; chunk `n` lives in
// slot `n % RING_SLOTS` and its size in the CHUNK_SIZES field of that slot.
#define QLEVER_FETCH_FIELDS(FIELD) \
  FIELD(EVENTS, 0)                 \
  FIELD(HEADERS_READY, 1)          \
  FIELD(FINISHED, 2)               \
  FIELD(FAILED, 3)                 \
  FIELD(ABORTED, 4)                \
  FIELD(STATUS, 5)                 \
  FIELD(WRITE_COUNT, 6)            \
  FIELD(READ_COUNT, 7)             \
  FIELD(CHUNK_SIZES, 8)

#define QLEVER_FETCH_ENUMERATOR(name, index) name = index,
enum class Field { QLEVER_FETCH_FIELDS(QLEVER_FETCH_ENUMERATOR) };
#undef QLEVER_FETCH_ENUMERATOR

// The index of the given field, and the total number of fields.
constexpr size_t index(Field field) { return static_cast<size_t>(field); }
constexpr size_t NUM_FIELDS = index(Field::CHUNK_SIZES) + RING_SLOTS;

// All the state of a single HTTP request that the requesting thread and the
// JavaScript code running on the network thread share.
struct RequestState {
  // The request; set by the requesting thread before the request is started.
  std::string url_;
  std::string method_;
  std::string requestBody_;
  std::string contentTypeHeader_;
  std::string acceptHeader_;
  bool followRedirects_ = false;

  // The state above is only read, the state below is written by both sides. The
  // JavaScript side sees `fields_` as a plain array of 32-bit integers.
  std::array<std::atomic<int32_t>, NUM_FIELDS> fields_{};
  static_assert(sizeof(std::atomic<int32_t>) == sizeof(int32_t));
  static_assert(std::atomic<int32_t>::is_always_lock_free);

  std::string metadata_ = std::string(METADATA_CAPACITY, '\0');
  std::string ring_ = std::string(size_t{RING_SLOTS} * SLOT_CAPACITY, '\0');

  int32_t field(Field field) const { return fields_[index(field)].load(); }
  void setField(Field field, int32_t value) {
    fields_[index(field)].store(value);
  }
  // The address of a field, for `emscripten_futex_wait` and `..._wake`.
  std::atomic<int32_t>* fieldAddress(Field field) {
    return &fields_[index(field)];
  }
  // The array of all fields, as the JavaScript side sees it.
  int32_t* fields() { return reinterpret_cast<int32_t*>(fields_.data()); }

  // The chunk with the given number, which has to be one that the JavaScript
  // side has already written (see `WRITE_COUNT`).
  ql::span<std::byte> chunk(size_t number) {
    size_t slot = number % RING_SLOTS;
    auto size = fields_[index(Field::CHUNK_SIZES) + slot].load();
    return ql::span{
        reinterpret_cast<std::byte*>(ring_.data()) + slot * SLOT_CAPACITY,
        static_cast<size_t>(size)};
  }

  // Tell the JavaScript side to abort the request. Also wakes it up if it is
  // waiting for a free slot, which would never be freed now.
  void abort() {
    setField(Field::ABORTED, 1);
    emscripten_futex_wake(fieldAddress(Field::ABORTED), 1);
    emscripten_futex_wake(fieldAddress(Field::READ_COUNT), 1);
  }

  // The metadata that the JavaScript side has written, as JSON. Anything
  // unexpected becomes an empty object, so callers can use `value()` directly.
  nlohmann::json metadata() const {
    // `c_str` is important because of the null termination.
    auto json = nlohmann::json::parse(metadata_.c_str(), nullptr, false);
    return json.is_object() ? json : nlohmann::json::object();
  }

  // The error message of a failed request.
  std::string errorMessage() const {
    return metadata().value("error", "unknown error");
  }
};

// `EM_JS` stringifies the body it is given, which suppresses macro expansion
// inside it; this extra level of indirection restores it, so that the
// JavaScript code below can use `QLEVER_FETCH_FIELDS`.
#define QLEVER_EM_JS(ret, name, params, ...) \
  EM_JS(ret, name, params, __VA_ARGS__)

// The entry of the `FIELD` object of the JavaScript code for one field.
#define QLEVER_FETCH_JS_PROPERTY(name, index) \
  name:                                       \
  index,

// NOTE: `clang-format` is disabled below because it breaks JavaScript (it turns
// `===` into `== =`).
// clang-format off

// Perform a whole request: fetch the URL, report the response headers, then
// stream the body into the ring buffer until it ends, fails, or is aborted. Runs
// on the network thread and returns immediately.
//
// Note that `MEMORY64` passes pointers as `BigInt`, which cannot index the heap
// views, hence the `Number(...)` conversions (which are no-ops without it). And
// Emscripten's cached `HEAPU8` can be stale after a memory growth, so we build
// fresh views; that works because with pthreads the memory is a
// `SharedArrayBuffer`, which grows in place.
QLEVER_EM_JS(void, qleverFetchRun,
      (int32_t* fields, char* ring, int slotCapacity, int numSlots,
       char* metadata, int metadataCapacity, const char* url,
       const char* method, const char* requestBody, int requestBodySize,
       const char* contentTypeHeader, const char* acceptHeader,
       bool followRedirects),
      {
        // Generated from the same list as the `Field` enum above.
        const FIELD = { QLEVER_FETCH_FIELDS(QLEVER_FETCH_JS_PROPERTY) };
        // Fresh views of the memory, see above.
        const i32 = () => new Int32Array(HEAPU8.buffer);
        const u8 = () => new Uint8Array(HEAPU8.buffer);
        const base = Number(fields) / 4;
        const ringBase = Number(ring);
        const metadataBase = Number(metadata);
        const get = (field) => Atomics.load(i32(), base + field);
        const set = (field, value) => Atomics.store(i32(), base + field, value);
        // Publish the changes made so far and wake up the requesting thread.
        const publish = () => {
          const array = i32();
          Atomics.add(array, base + FIELD.EVENTS, 1);
          Atomics.notify(array, base + FIELD.EVENTS);
        };
        // Truncate a header value or error message, so that the metadata fits.
        const truncate = (value) =>
            String(value === null || value === undefined ? "" : value)
                .slice(0, 512);
        // Write the given object to the metadata buffer as JSON. If it does not
        // fit even though its values are truncated, report that rather than
        // writing something unparseable.
        const writeMetadata = (object) => {
          let json = JSON.stringify(object);
          // A string of length `n` needs at most `3 * n` bytes in UTF-8, plus
          // the null terminator.
          if (3 * json.length + 1 > metadataCapacity) {
            json = JSON.stringify({error : "the metadata is too large"});
          }
          stringToUTF8Array(json, u8(), metadataBase, metadataCapacity);
        };
        // The message of an error thrown by `fetch` is often unhelpful (Node.js
        // only says "fetch failed"); the interesting part is in its `cause`.
        const describeError = (error) => {
          if (!error) {
            return "unknown error";
          }
          let message = error.message ? error.message : String(error);
          if (error.cause) {
            const cause = error.cause;
            message += " (" + (cause.code ? cause.code + ": " : "") +
                       (cause.message ? cause.message : String(cause)) + ")";
          }
          return message;
        };

        const controller = new AbortController();
        // React to an abort immediately, not only in between two chunks: an
        // endpoint that stops responding must not block a cancelled query. The
        // wait is resolved by any notification, so `finish` ends this watch by
        // storing 2.
        const watchForAbort = () => {
          const wait = Atomics.waitAsync(i32(), base + FIELD.ABORTED, 0);
          const check = () => {
            if (get(FIELD.ABORTED) === 1) {
              controller.abort();
            }
          };
          if (wait.async) {
            wait.value.then(check);
          } else {
            check();
          }
        };
        // Report that we are done; the requesting thread may then destroy the
        // state of the request.
        const finish = () => {
          set(FIELD.ABORTED, 2);
          Atomics.notify(i32(), base + FIELD.ABORTED);
          set(FIELD.FINISHED, 1);
          publish();
        };
        const fail = (message) => {
          writeMetadata({error : truncate(message)});
          set(FIELD.FAILED, 1);
          finish();
        };

        const heap = u8();
        const headers = {};
        const contentType = UTF8ArrayToString(heap, Number(contentTypeHeader));
        const accept = UTF8ArrayToString(heap, Number(acceptHeader));
        if (accept) {
          headers["Accept"] = accept;
        }
        const options = {
          method : UTF8ArrayToString(heap, Number(method)),
          headers : headers,
          signal : controller.signal,
          // Redirects are followed by the JavaScript environment; if we are not
          // allowed to follow them, a redirect makes the request fail.
          redirect : followRedirects ? "follow" : "error"
        };
        if (requestBodySize > 0) {
          if (contentType) {
            headers["Content-Type"] = contentType;
          }
          // Copy the body out of the WebAssembly memory: `fetch` rejects
          // bodies that are backed by a `SharedArrayBuffer`.
          const body = Number(requestBody);
          options.body =
              new Uint8Array(heap.subarray(body, body + requestBodySize));
        }

        // Wait for a free slot, which is where the back pressure comes from: we
        // stop reading the body when the consumer cannot keep up. Returns false
        // if the request was aborted while waiting.
        const waitForFreeSlot = async () => {
          while (get(FIELD.WRITE_COUNT) - get(FIELD.READ_COUNT) >= numSlots) {
            if (get(FIELD.ABORTED) === 1) {
              return false;
            }
            // Waiting for a change of the value we just read means that a slot
            // freed in between cannot lead to a lost wakeup.
            const readCount = get(FIELD.READ_COUNT);
            if (get(FIELD.WRITE_COUNT) - readCount < numSlots) {
              break;
            }
            const wait =
                Atomics.waitAsync(i32(), base + FIELD.READ_COUNT, readCount);
            if (wait.async) {
              await wait.value;
            }
          }
          return get(FIELD.ABORTED) !== 1;
        };

        const run = async () => {
          const response =
              await fetch(UTF8ArrayToString(u8(), Number(url)), options);
          set(FIELD.STATUS, response.status);
          writeMetadata({
            contentType : truncate(response.headers.get("content-type")),
            location : truncate(response.headers.get("location"))
          });
          set(FIELD.HEADERS_READY, 1);
          publish();
          // A `204 No Content` for example has no body at all.
          if (!response.body) {
            return;
          }
          const reader = response.body.getReader();
          let pending = new Uint8Array(0);
          while (true) {
            if (pending.length === 0) {
              const result = await reader.read();
              if (result.done) {
                return;
              }
              pending = result.value;
            }
            if (!(await waitForFreeSlot())) {
              reader.cancel().catch(() => {});
              return;
            }
            const writeCount = get(FIELD.WRITE_COUNT);
            const slot = writeCount % numSlots;
            const size = Math.min(pending.length, slotCapacity);
            u8().set(pending.subarray(0, size), ringBase + slot * slotCapacity);
            set(FIELD.CHUNK_SIZES + slot, size);
            set(FIELD.WRITE_COUNT, writeCount + 1);
            publish();
            pending = pending.subarray(size);
          }
        };

        watchForAbort();
        if (typeof fetch !== "function") {
          fail("the JavaScript environment provides no `fetch` function");
          return;
        }
        run().then(finish, (error) => {
          // An abort is not an error: the requesting thread knows about it and
          // reports the reason itself.
          if (get(FIELD.ABORTED) === 1) {
            finish();
          } else {
            fail(describeError(error));
          }
        });
      });

// Whether the JavaScript environment provides `Atomics.waitAsync`, which the
// code above needs to wait for a free slot without blocking its thread.
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

// The single entry point into the JavaScript code above, see `qleverFetchRun`.
void runRequest(void* argument) {
  auto& state = *static_cast<RequestState*>(argument);
  qleverFetchRun(state.fields(), state.ring_.data(), SLOT_CAPACITY, RING_SLOTS,
                 state.metadata_.data(), METADATA_CAPACITY, state.url_.c_str(),
                 state.method_.c_str(), state.requestBody_.data(),
                 static_cast<int>(state.requestBody_.size()),
                 state.contentTypeHeader_.c_str(), state.acceptHeader_.c_str(),
                 state.followRedirects_);
}

// Wait until the `EVENTS` counter differs from `lastEvent`. Throws if the query
// is cancelled while waiting, which also aborts the request.
void waitForEvent(RequestState& state, int32_t lastEvent,
                  const ad_utility::SharedCancellationHandle& handle) {
  while (state.field(Field::EVENTS) == lastEvent) {
    emscripten_futex_wait(state.fieldAddress(Field::EVENTS), lastEvent,
                          CANCELLATION_CHECK_INTERVAL_MS);
    if (handle != nullptr && handle->isCancelled()) {
      state.abort();
      handle->throwIfCancelled();
    }
  }
}

// Create the shared state of a request, with a deleter that makes it outlive
// all accesses from the JavaScript side (hence no `make_shared`).
std::shared_ptr<RequestState> makeRequestState() {
  auto deleter = [](RequestState* state) {
    if (state->field(Field::FINISHED) != 1) {
      // The JavaScript side is still working on the request (the response was
      // not read completely, or the query was cancelled). Abort and wait for
      // its confirmation, which comes as soon as the network thread runs again.
      state->abort();
      double waited = 0;
      while (waited < ABORT_TIMEOUT_MS) {
        int32_t lastEvent = state->field(Field::EVENTS);
        if (state->field(Field::FINISHED) == 1) {
          break;
        }
        emscripten_futex_wait(state->fieldAddress(Field::EVENTS), lastEvent,
                              CANCELLATION_CHECK_INTERVAL_MS);
        waited += CANCELLATION_CHECK_INTERVAL_MS;
      }
      if (state->field(Field::FINISHED) != 1) {
        // Deliberately leak the state: freeing it while the JavaScript side
        // may still write to it would be much worse.
        AD_LOG_WARN << "The JavaScript side did not react to the abort of the "
                       "HTTP request to <"
                    << state->url_ << ">, leaking its state" << std::endl;
        return;
      }
    }
    delete state;
  };
  return {new RequestState{}, std::move(deleter)};
}

// Read the response body chunk by chunk from the ring buffer. A yielded chunk
// is only valid until the next iteration, exactly as in the native
// implementation.
cppcoro::generator<ql::span<std::byte>> readResponseBody(
    std::shared_ptr<RequestState> state,
    ad_utility::SharedCancellationHandle handle) {
  size_t readCount = 0;
  while (true) {
    int32_t lastEvent = state->field(Field::EVENTS);
    // Consume what was received before reacting to the end or to an error.
    if (readCount < static_cast<size_t>(state->field(Field::WRITE_COUNT))) {
      co_yield state->chunk(readCount);
      ++readCount;
      state->setField(Field::READ_COUNT, static_cast<int32_t>(readCount));
      // Wake up the JavaScript side, which may be waiting for a free slot.
      emscripten_futex_wake(state->fieldAddress(Field::READ_COUNT), 1);
      continue;
    }
    if (state->field(Field::FAILED) == 1) {
      throw std::runtime_error(absl::StrCat(
          "The HTTP request to <", state->url_,
          "> failed while reading the response body: ", state->errorMessage()));
    }
    if (state->field(Field::FINISHED) == 1) {
      co_return;
    }
    waitForEvent(*state, lastEvent, handle);
  }
}
}  // namespace

// ____________________________________________________________________________
HttpOrHttpsResponse sendHttpOrHttpsRequest(
    const ad_utility::httpUtils::Url& url,
    ad_utility::SharedCancellationHandle handle, const http::verb& method,
    std::string_view requestData, std::string_view contentTypeHeader,
    std::string_view acceptHeader, size_t maxRedirects) {
  // Both of these would otherwise wait forever.
  if (!hasAtomicsWaitAsync()) {
    throw std::runtime_error(
        "HTTP requests from WebAssembly require `Atomics.waitAsync`, which "
        "this JavaScript environment does not provide");
  }
  if (isBrowserMainThread()) {
    throw std::runtime_error(absl::StrCat(
        "The HTTP request to <", url.asString(),
        "> was issued from the main thread of a browser, where waiting for "
        "the response is not possible. Run QLever in a Web Worker (which is "
        "required for its potentially long-running operations anyway)."));
  }
  AD_CORRECTNESS_CHECK(requestData.size() <=
                       static_cast<size_t>(std::numeric_limits<int>::max()));

  auto state = makeRequestState();
  state->url_ = url.asString();
  state->method_ = http::to_string(method);
  state->requestBody_ = requestData;
  state->contentTypeHeader_ = contentTypeHeader;
  state->acceptHeader_ = acceptHeader;
  state->followRedirects_ = maxRedirects > 0;

  if (!emscripten_proxy_async(emscripten_proxy_get_system_queue(),
                              networkThread(), &runRequest, state.get())) {
    // Nothing was started, so nothing has to be aborted.
    state->setField(Field::FINISHED, 1);
    AD_THROW("Could not reach the thread that performs the HTTP requests");
  }

  // Wait for the response headers.
  while (true) {
    int32_t lastEvent = state->field(Field::EVENTS);
    if (state->field(Field::FAILED) == 1) {
      throw std::runtime_error(
          absl::StrCat("The HTTP request to <", state->url_,
                       "> failed: ", state->errorMessage()));
    }
    if (state->field(Field::HEADERS_READY) == 1) {
      break;
    }
    waitForEvent(*state, lastEvent, handle);
  }

  nlohmann::json metadata = state->metadata();
  return {.status_ = static_cast<http::status>(state->field(Field::STATUS)),
          .contentType_ = metadata.value("contentType", ""),
          .location_ = metadata.value("location", ""),
          .body_ = readResponseBody(std::move(state), std::move(handle))};
}

#endif  // defined(__EMSCRIPTEN__) && !defined(QLEVER_REDUCED_FEATURE_SET...)
