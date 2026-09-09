# Copyright 2026 The QLever Authors, in particular:
#
# 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
#
# UFR = University of Freiburg, Chair of Algorithms and Data Structures
#
# You may not use this file except in compliance with the Apache 2.0 License,
# which can be found in the `LICENSE` file at the root of the QLever project.

"""The dependencies that are not available in the Bazel Central Registry.

Each of these mirrors one `FetchContent_Declare` call in the top-level
`CMakeLists.txt` and is pinned to the same commit. When bumping a dependency in
`CMakeLists.txt`, bump it here as well. The `sha256` of a new commit can be
obtained by running

    curl -sL <url> | sha256sum

The `BUILD.bazel` files live next to this file, because these projects do not
ship one themselves.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _github_archive(name, repository, commit, sha256, build_file, patch_cmds = None):
    """Declare an `http_archive` for a GitHub commit with a hand-written BUILD file."""
    http_archive(
        name = name,
        build_file = build_file,
        patch_cmds = patch_cmds,
        sha256 = sha256,
        strip_prefix = "%s-%s" % (repository.split("/")[1], commit),
        url = "https://github.com/%s/archive/%s.tar.gz" % (repository, commit),
    )

def _qlever_third_party_impl(_module_ctx):
    # QLever's fork of `range-v3`. The fork removes some differences between
    # `range-v3` and `std::ranges` so that the former can be used as an
    # (almost) drop-in replacement for the latter. The upstream `range-v3` in
    # the Bazel Central Registry does NOT work here, which is why this is an
    # `http_archive` and not a `bazel_dep`.
    _github_archive(
        name = "range-v3",
        repository = "joka921/range-v3",
        commit = "0e2a41b61694e823df1b7d77174b139253c7ac39",  # branch fork-for-qlever
        sha256 = "abdf3623b91a9c381bbe6578ce16bb601a4675e4d5ecf9bd1fe12c648d214b20",
        build_file = "//bazel/third_party:range-v3.BUILD.bazel",
    )

    # CTRE, Compile-Time-Regular-Expressions. Like the CMake build, we use the
    # single-header distribution.
    _github_archive(
        name = "ctre",
        repository = "hanickadot/compile-time-regular-expressions",
        commit = "e34c26ba149b9fd9c34aa0f678e39739641a0d1e",  # v3.10.0
        sha256 = "2bf1482da366c7f0461e5cbbdb7a65474cdefe0eb7f8d994c6d79121f09e1806",
        build_file = "//bazel/third_party:ctre.BUILD.bazel",
    )

    _github_archive(
        name = "fsst",
        repository = "cwida/fsst",
        commit = "b228af6356196095eaf9f8f5654b0635f969661e",  # main branch from 27th May 2025
        sha256 = "29d3df7b5ab19e0218f8c14831212983f252989c992e17e7a39528e547873af6",
        build_file = "//bazel/third_party:fsst.BUILD.bazel",
    )

    # `uriparser` needs `src/UriConfig.h`, which its CMake build generates from
    # `src/UriConfig.h.in`. `UriMemory.c` includes it relative to itself, so it
    # has to be a real file next to the sources and cannot be a `genrule`
    # output. Neither `HAVE_WPRINTF` (the wide-character API is disabled) nor
    # `HAVE_REALLOCARRAY` (only an optimization, `uriparser` has a fallback) is
    # needed, so the generated file is answered conservatively with "no" for
    # both and is the same on every platform.
    _github_archive(
        name = "uriparser",
        repository = "uriparser/uriparser",
        commit = "04d8b8df5e0c6bf6c06e472540c015943a613bd2",  # 1.0.0
        sha256 = "1577e0267263625dbfb53ca18a1530142b89e2762593a7c6cbcec2d730f83007",
        build_file = "//bazel/third_party:uriparser.BUILD.bazel",
        patch_cmds = [
            """cat > src/UriConfig.h <<'EOF'
#if !defined(URI_CONFIG_H)
#  define URI_CONFIG_H 1
#  define PACKAGE_VERSION "1.0.0"
#endif /* !defined(URI_CONFIG_H) */
EOF""",
        ],
    )

    _github_archive(
        name = "spatialjoin",
        repository = "ad-freiburg/spatialjoin",
        commit = "344a3fa41e96db38ac775cc78224847255d0152f",
        sha256 = "25eebe3036fa5c88bad158bfd9b7eba12ca597a1ea128474b4df6845065ccab3",
        build_file = "//bazel/third_party:spatialjoin.BUILD.bazel",
    )

    # `pbutil` is a git submodule of `spatialjoin` at `src/util`, which the
    # CMake build gets for free because `FetchContent` initializes submodules.
    # A release tarball does not contain submodules, so it is a separate
    # repository here; `@spatialjoin` refers to it via its `deps`. The commit is
    # the one that the pinned `spatialjoin` commit points its submodule at.
    _github_archive(
        name = "pbutil",
        repository = "ad-freiburg/util",
        commit = "f3d23af88d95475fda6fa91b1d3bd318de1a91b0",
        sha256 = "b14754688d0c4cc0aac0a668054ec1609ace3a89cc14a19a8cab9273e564462f",
        build_file = "//bazel/third_party:pbutil.BUILD.bazel",
    )

qlever_third_party = module_extension(
    implementation = _qlever_third_party_impl,
    doc = "Declares the QLever dependencies that are not in the Bazel Central Registry.",
)
