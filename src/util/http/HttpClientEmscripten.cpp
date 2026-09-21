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
// blocks cannot run the event loop that settles the promises of its own `fetch`
// call. A dedicated thread (`networkThread`) therefore performs the requests
// and never blocks, while the requesting thread waits for it. Emscripten's
// synchronous filesystem API works the same way, see `ProxyWorker` in
// `system/lib/wasmfs/thread_utils.h`.
//
// The JavaScript side (`qleverFetch`) is a generator over one response: each
// step yields the head, the next chunk of the body, or its end. The C++ side
// takes one step at a time (`Request::nextStep`), which `performStep` awaits on
// the network thread. Nothing is read ahead, so a response is never further
// along than its consumer.
//
// Differences to the native implementation:
//
// * Requests must not be issued from the main thread of a browser, where
//   blocking is not allowed. This is checked, so the result is an error and not
//   a frozen page.
// * In a browser, CORS applies, and `Content-Type` is the only response header
//   that is reliably readable for a cross-origin response.
// * `maxRedirects` only distinguishes 0 ("don't follow", a redirect then fails
//   the request, as natively) from larger values ("follow up to the limit of
//   the JavaScript environment"). Following them ourselves would need
//   `redirect: "manual"`, whose response is opaque in a browser.
// * The `Location` header is never reported (`location_` stays empty).
//   `fetch` only ever hands us the *final* response of a redirect chain, and
//   the one non-redirect status that may carry the header (`300`, `305`) is
//   not one that our caller follows either.
// * `User-Agent` cannot be set in a browser, `Content-Type` is only sent with a
//   non-empty body, and `fetch` rejects a `GET` or `HEAD` that has one.
// * The proxy configured for the process (see `globalProxy()`) is ignored,
//   because `fetch` has no such setting. Configure the browser or Node.js.
// * A request that fails is reported when the consumer asks for the next chunk,
//   so a response that fails right after its head makes
//   `sendHttpOrHttpsRequest` itself throw.

#if defined(__EMSCRIPTEN__) && !defined(QLEVER_REDUCED_FEATURE_SET_FOR_CPP17)

#include <absl/base/no_destructor.h>
#include <absl/strings/str_cat.h>
#include <emscripten/em_js.h>
#include <emscripten/emscripten.h>
#include <emscripten/proxying.h>
#include <emscripten/val.h>
#include <pthread.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/http/HttpClient.h"

namespace {

namespace http = boost::beast::http;
using emscripten::EM_VAL;
using emscripten::val;

// How often the requesting thread looks up from waiting to check whether the
// query has been cancelled.
constexpr std::chrono::milliseconds CANCELLATION_CHECK_INTERVAL{100};

// NOTE: `clang-format` is disabled below because it breaks JavaScript (it turns
// `===` into `== =`).
// clang-format off

// Perform one HTTP request as a generator over its response: `next()` takes the
// next step and resolves to what that step yielded, `cancel()` gives up on the
// response and closes the connection. A step yields one of
//   {kind: "head", status, contentType}
//   {kind: "chunk", data}
//   {kind: "done"}
//   {kind: "error", message}
//
// NOTE: The parameters arrive as properties of `response`, and `next` and
// `cancel` are added to that same object, rather than the object being created
// and returned here. That is because an `EM_JS` function cannot return a
// JavaScript value without a workaround: `Emval.toHandle` gives a number, while
// `MEMORY64` expects a `BigInt` (emscripten-core/emscripten#16975), so the
// return would have to be wrapped in `BigInt(...)` — which would silently
// become wrong the day that bug is fixed. Taking the object as a parameter
// needs no such workaround (see `Number` below).
//
// NOTE: Nothing here ever rejects. A rejected promise destroys the C++
// coroutine that awaits it instead of resuming it (see `performStep`), so a
// failure has to be a value like any other outcome.
EM_JS(void, qleverFetch, (EM_VAL handle), {
  // `Number` because `MEMORY64` passes pointers as `BigInt`, which cannot index
  // the table of the `val` handles; a no-op for one that already is a number,
  // so this stays correct whatever Emscripten does.
  const response = Emval.toValue(Number(handle));
  const {url, method, body, contentType, accept, followRedirects} = response;
  const controller = new AbortController();

  // The message of an error thrown by `fetch` is often unhelpful (Node.js only
  // says "fetch failed"); the interesting part is in its `cause`, which
  // typically has a `code` such as `ECONNREFUSED`.
  const describe = (error) => {
    const message = error?.message ?? String(error);
    const cause = error?.cause;
    if (!cause) {
      return message;
    }
    const code = cause.code ? `${cause.code}: ` : "";
    return `${message} (${code}${cause.message ?? String(cause)})`;
  };

  const options = () => {
    const result = {
      method,
      headers : {},
      signal : controller.signal,
      // The JavaScript environment follows the redirects; if we may not follow
      // them, a redirect makes the request fail.
      redirect : followRedirects ? "follow" : "error"
    };
    if (accept) {
      result.headers["Accept"] = accept;
    }
    // `fetch` rejects a `GET` or a `HEAD` that has a body, and a `Content-Type`
    // without a body describes nothing. NOTE: `body` is a `Uint8Array` and
    // hence truthy even when it is empty, so ask for its length.
    if (body.length > 0) {
      result.body = body;
      if (contentType) {
        result.headers["Content-Type"] = contentType;
      }
    }
    return result;
  };

  const steps = async function*() {
    try {
      const fetched = await fetch(url, options());
      yield {
        kind : "head",
        status : fetched.status,
        contentType : fetched.headers.get("content-type") ?? ""
      };
      // A `204 No Content` for example has no body at all.
      if (fetched.body) {
        const reader = fetched.body.getReader();
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) {
            break;
          }
          yield {kind : "chunk", data : chunk.value};
        }
      }
    } catch (error) {
      yield {kind : "error", message : describe(error)};
    } finally {
      // A no-op for a response that we read to its end; for one that we stopped
      // reading, this closes the connection.
      controller.abort();
    }
  }();

  response.next = async () => {
    const step = await steps.next();
    return step.done ? {kind : "done"} : step.value;
  };
  response.cancel = () => controller.abort();
});

// clang-format on

// `qleverFetch` uses this part of Emscripten's JavaScript runtime.
EM_JS_DEPS(qleverFetchDependencies, "$Emval");

// What to request. Plain C++ values, so that it can simply be copied to the
// network thread, which is the only place that may build JavaScript objects.
struct RequestDescription {
  std::string url_;
  std::string method_;
  std::string body_;
  std::string contentType_;
  std::string accept_;
  bool followRedirects_ = false;
};

// The given bytes as a `Uint8Array` of their own. A copy, because `fetch`
// rejects a body that is backed by a `SharedArrayBuffer`, which a view into the
// WebAssembly memory of a threaded build is.
val toUint8Array(std::string_view bytes) {
  return val::global("Uint8Array")
      .new_(val(emscripten::typed_memory_view(
          bytes.size(), reinterpret_cast<const uint8_t*>(bytes.data()))));
}

// Start the request and return the generator over its response, see
// `qleverFetch`. Only to be called on the network thread.
val startRequest(const RequestDescription& description) {
  val response = val::object();
  response.set("url", description.url_);
  response.set("method", description.method_);
  // As bytes, not as a string: `embind` would convert an `std::string` to
  // JavaScript by decoding it as UTF-8, which does not round-trip for a body
  // that is binary or otherwise not valid UTF-8.
  response.set("body", toUint8Array(description.body_));
  response.set("contentType", description.contentType_);
  response.set("accept", description.accept_);
  response.set("followRedirects", description.followRedirects_);
  qleverFetch(response.as_handle());
  return response;
}

// The head of a response, which the first step yields.
struct ResponseHead {
  http::status status_;
  std::string contentType_;
};

// The next chunk of a response body, or `nullopt` at its end. An `std::string`
// because that is what `embind` turns the `Uint8Array` into, in a single call.
using BodyChunk = std::optional<std::string>;

// What one step yields. A request that failed is not one of these: it becomes
// the exception of the `std::future` that the step is reported in.
using Step = std::variant<ResponseHead, BodyChunk>;
using StepPromise = std::promise<Step>;

// The thread that performs the requests, created on first use and then alive
// until the process exits. Deliberately not the main runtime thread, which
// might itself be the thread that waits, for example when a query is run from
// the main thread of a Node.js application.
//
// NOTE: Emscripten spawns Web Workers only from the JavaScript main thread, so
// this thread only comes up once that thread reaches its event loop. A build
// whose `main` may block (one without `-sPROXY_TO_PTHREAD`) must therefore not
// block it while waiting for the very first request.
pthread_t networkThread() {
  static pthread_t thread = []() {
    // A live runtime keeps the Web Worker of this thread, and hence its event
    // loop, alive; that loop is what runs the proxied calls and the `fetch`
    // callbacks they start. In contrast to Emscripten's `ProxyWorker` we don't
    // wait for the thread to have started, which would deadlock on a browser's
    // main thread; the proxied calls simply run once it is up.
    std::thread thread{[]() { emscripten_exit_with_live_runtime(); }};
    // The thread never returns, so it can neither be joined nor be a
    // `std::jthread`. Emscripten's proxying API identifies it by its
    // `pthread_t`, which stays valid for as long as it runs.
    pthread_t handle = thread.native_handle();
    thread.detach();
    return handle;
  }();
  return thread;
}

// Hand `work` over to the network thread and return right away. Throws if that
// thread cannot be reached, in which case `work` never runs and is destroyed
// right here.
void runOnNetworkThread(std::function<void()> work) {
  // Deliberately never destroyed: `em_proxying_queue_destroy` frees the queue
  // and its pending tasks without draining them, and the network thread
  // outlives the static objects of the process, so it could still run a task
  // that a request destroyed shortly before the exit left behind.
  static absl::NoDestructor<emscripten::ProxyingQueue> queue;
  if (!queue->proxyAsync(networkThread(), std::move(work))) {
    AD_THROW("Could not reach the thread that performs the HTTP requests");
  }
}

// Take the next step of `response` and report what it yielded through
// `promise`. Runs on the network thread, which it gives back while it waits for
// JavaScript; that wait is what makes this a coroutine. The JavaScript promise
// it returns itself is of no interest to anyone.
//
// NOTE: A `co_await` of a *rejected* promise destroys the coroutine rather than
// resuming it, which is why `qleverFetch` reports a failure as a value. Should
// it happen anyway, destroying `promise` unset still wakes the requesting
// thread, with a "broken promise" error rather than not at all.
val performStep(val response, std::string url,
                std::shared_ptr<StepPromise> promise) {
  val step = co_await response.call<val>("next");
  // Nothing may escape from here: an exception of a coroutine becomes a
  // rejected JavaScript promise that nobody handles, which Node.js answers by
  // terminating the process.
  try {
    std::string kind = step["kind"].as<std::string>();
    if (kind == "head") {
      promise->set_value(
          ResponseHead{static_cast<http::status>(step["status"].as<int>()),
                       step["contentType"].as<std::string>()});
    } else if (kind == "chunk") {
      promise->set_value(BodyChunk{step["data"].as<std::string>()});
    } else if (kind == "done") {
      promise->set_value(BodyChunk{std::nullopt});
    } else {
      AD_CORRECTNESS_CHECK(kind == "error");
      throw std::runtime_error(
          absl::StrCat("The HTTP request to <", url,
                       "> failed: ", step["message"].as<std::string>()));
    }
  } catch (...) {
    promise->set_exception(std::current_exception());
  }
  co_return val::undefined();
}

// Give up on a response and destroy it. Both may only happen on the network
// thread, so both are proxied there.
struct GiveUpOnResponse {
  void operator()(val* response) const noexcept {
    ad_utility::ignoreExceptionIfThrows(
        [response]() {
          runOnNetworkThread([response]() {
            response->call<void>("cancel");
            std::default_delete<val>{}(response);
          });
        },
        "An HTTP request whose response nobody reads any more was therefore "
        "not cancelled; its connection stays open until the process exits.");
  }
};

// Owns the generator over one response, see `qleverFetch`.
using ResponsePtr = std::unique_ptr<val, GiveUpOnResponse>;

// Start the request and take ownership of the generator over its response.
// Throws if the network thread cannot be reached, in which case nothing was
// started.
ResponsePtr startRequestOnNetworkThread(RequestDescription description) {
  // Plainly owned until the hand-over has succeeded: an `undefined` value
  // belongs to no thread and may therefore be destroyed right here.
  auto response = std::make_unique<val>(val::undefined());
  runOnNetworkThread(
      [response = response.get(), description = std::move(description)]() {
        *response = startRequest(description);
      });
  return ResponsePtr{response.release()};
}

// A request that is being performed, as the requesting thread sees it. The
// generator over its response may only be touched on the network thread, so
// this holds it from a distance and proxies everything it does with it.
// Destroying it gives up on the response, so that one that nobody reads does
// not keep its connection open.
class Request {
 private:
  // Only for the message of a request that fails.
  std::string url_;
  ResponsePtr response_;

 public:
  // Start the request. Throws if the network thread cannot be reached, in which
  // case nothing was started.
  explicit Request(RequestDescription description)
      : url_{description.url_},
        response_{startRequestOnNetworkThread(std::move(description))} {}

  // Take the next step of the request and return what it yielded. Throws if the
  // request failed, or if the query was cancelled while we waited.
  Step nextStep(const ad_utility::SharedCancellationHandle& handle) const {
    auto promise = std::make_shared<StepPromise>();
    std::future<Step> step = promise->get_future();
    runOnNetworkThread([response = response_.get(), url = url_,
                        promise = std::move(promise)]() {
      performStep(*response, url, promise);
    });
    // Waiting in intervals rather than for the whole step is what lets a
    // cancelled query stop even when the endpoint never answers.
    while (true) {
      handle->throwIfCancelled();
      if (step.wait_for(CANCELLATION_CHECK_INTERVAL) ==
          std::future_status::ready) {
        return step.get();
      }
    }
  }
};

// Read the response body chunk by chunk, one step of the request per chunk. A
// yielded chunk is only valid until the next iteration, exactly as in the
// native implementation. The request is a parameter rather than a local
// variable, because a `cppcoro::generator` that is destroyed without ever being
// iterated destroys its parameters but never runs its body, which is how a
// response that is not read at all still gives up on its request.
cppcoro::generator<ql::span<std::byte>> readResponseBody(
    Request request, ad_utility::SharedCancellationHandle handle) {
  while (true) {
    Step step = request.nextStep(handle);
    BodyChunk& chunk = std::get<BodyChunk>(step);
    if (!chunk.has_value()) {
      co_return;
    }
    co_yield ql::span<std::byte>{reinterpret_cast<std::byte*>(chunk->data()),
                                 chunk->size()};
  }
}

// Complain if the request is issued from a thread that may not block, which
// the main thread of a browser is: waiting there for a response that can only
// arrive once we return to the event loop freezes the page, and the very first
// request deadlocks outright, because the Web Worker of the network thread
// only comes up once the main thread reaches its event loop. The main thread
// of Node.js may block, and so may a Web Worker (which is what Emscripten's
// threads are), hence the check for a browser rather than just for the main
// thread. This is the same distinction that Emscripten itself makes in
// `emscripten_check_blocking_allowed`, which only warns.
//
// NOTE: There is deliberately no check for `fetch` itself. Every environment
// that Emscripten supports has it (Node.js >= 18.3 and browsers far older than
// the oldest it targets), this is not even the thread that calls it (each
// Emscripten thread is a Web Worker with a JavaScript global scope of its
// own), and were it missing after all, `qleverFetch` would report a plain
// "fetch is not defined" like any other failure of the request.
void checkThatThisThreadMayBlock(const ad_utility::httpUtils::Url& url) {
  bool isMainThreadOfABrowser =
      val::global("WorkerGlobalScope").isUndefined() &&
      !val::global("window").isUndefined();
  if (isMainThreadOfABrowser) {
    throw std::runtime_error(absl::StrCat(
        "The HTTP request to <", url.asString(),
        "> was issued from the main thread of a browser, where waiting for "
        "the response is not possible. Run QLever in a Web Worker (which is "
        "required for its potentially long-running operations anyway)."));
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
  checkThatThisThreadMayBlock(url);

  Request request{
      RequestDescription{.url_ = url.asString(),
                         .method_ = std::string{http::to_string(method)},
                         .body_ = std::string{requestData},
                         .contentType_ = std::string{contentTypeHeader},
                         .accept_ = std::string{acceptHeader},
                         .followRedirects_ = maxRedirects > 0}};

  // Wait for the head of the response; a request that fails throws here.
  Step step = request.nextStep(handle);
  ResponseHead& head = std::get<ResponseHead>(step);
  return {.status_ = head.status_,
          .contentType_ = std::move(head.contentType_),
          // Deliberately empty, see the note at the top of this file.
          .location_ = {},
          .body_ = readResponseBody(std::move(request), std::move(handle))};
}

#endif  // defined(__EMSCRIPTEN__) && !defined(QLEVER_REDUCED_FEATURE_SET...)
