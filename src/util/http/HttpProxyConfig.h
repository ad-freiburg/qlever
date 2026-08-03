//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#ifndef QLEVER_SRC_UTIL_HTTP_HTTPPROXYCONFIG_H
#define QLEVER_SRC_UTIL_HTTP_HTTPPROXYCONFIG_H

#include <optional>
#include <string>
#include <string_view>

#include "backports/three_way_comparison.h"

// Support for routing QLever's outgoing HTTP requests (`SERVICE` and `LOAD`)
// through an HTTP proxy, configured via the `http_proxy` environment variable.
// If a proxy is configured, then *all* outgoing requests go through it, no
// matter whether they use HTTP or HTTPS, and no matter which host they address.
// In particular, the other variables that some tools understand (`HTTP_PROXY`,
// `https_proxy`, `all_proxy`, `no_proxy`) are deliberately not supported.
namespace ad_utility::httpProxy {

// An HTTP proxy to route outgoing requests through.
struct Proxy {
  // Host and port of the proxy itself (not of the request's target).
  std::string host_;
  std::string port_;
  // The value for the `Proxy-Authorization` header, or empty if the proxy
  // requires no authentication.
  std::string authorization_;

  // A human-readable description for the startup log. Deliberately does not
  // contain the credentials encoded in `authorization_`.
  std::string asStringForLogging() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Proxy, host_, port_,
                                              authorization_)
};

// Parse a proxy URL of the form `[http://][user[:password]@]host[:port]`. The
// port defaults to 80 (unlike `curl`, which defaults to 1080; QLever only
// supports HTTP proxies, for which 80 is the natural default). Percent-encoded
// credentials are decoded, and turned into `Proxy::authorization_` as a `Basic`
// credential.
//
// Returns `std::nullopt` if `proxyUrl` is empty or consists only of
// whitespace, which is the conventional way of saying "use no proxy".
//
// Throws `std::runtime_error` if the URL is malformed, has no host, or names a
// scheme other than `http`. In particular, `https://` (TLS to the proxy
// itself) and `socks5://` are rejected explicitly, rather than silently
// treated as a plain HTTP proxy.
std::optional<Proxy> parseProxyUrl(std::string_view proxyUrl);

// The request target for a plain HTTP request that is relayed by a proxy: the
// proxy needs to know where to relay the request to, so the target has to be
// given in absolute form (`http://host:port/path`) instead of in the usual
// origin form (`/path`), see RFC 9112, 3.2.2. The default port 80 is omitted,
// as `curl` does, so that no proxy can trip over a redundant `:80`. The
// `target` must be in origin form, that is, start with a `/` (as
// `Url::target()` does).
//
// Note that this is only needed for plain HTTP. An HTTPS connection is tunneled
// through the proxy via the `CONNECT` method, so the proxy never sees the
// request itself and the target stays in origin form.
std::string absoluteFormTarget(std::string_view host, std::string_view port,
                               std::string_view target);

// Read the proxy from the `http_proxy` environment variable, or return
// `std::nullopt` if it is unset or empty (setting it to the empty string is the
// conventional way of undoing a setting inherited from a parent process).
// Throws if the variable holds a malformed proxy URL, see `parseProxyUrl`.
// Exposed mostly for testing; production code should use `globalProxy()` below.
std::optional<Proxy> proxyFromEnvironment();

// The proxy for this process, read from the environment on first use. Throws if
// the environment holds a malformed proxy URL; `qlever-server` calls this early
// during startup so that such a misconfiguration is reported before the index
// is loaded, rather than on the first federated query.
//
// The returned reference stays valid for the rest of the process (but the value
// it refers to changes if `resetGlobalProxyForTesting` below is called).
const std::optional<Proxy>& globalProxy();

// Read the environment again and replace the value cached by `globalProxy()`
// with the result. Only for tests: a test that wants to observe the caching
// behavior cannot know whether some other test in the same binary has already
// called `globalProxy()` (all unit tests are linked into a single binary in
// some configurations). Throws if the environment holds a malformed proxy URL,
// see `parseProxyUrl`. Must not be called concurrently with `globalProxy()`.
void resetGlobalProxyForTesting();

}  // namespace ad_utility::httpProxy

#endif  // QLEVER_SRC_UTIL_HTTP_HTTPPROXYCONFIG_H
#endif
