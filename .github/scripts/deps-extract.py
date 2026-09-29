#!/usr/bin/env python3
# deps-extract.py <cmake-build-dir> <target>
#
# Print the dependency closure of <target>: every source/header file of the
# source tree that the compiler read for the target's objects and for the
# objects of each static library on its link line. The list is sorted, one
# repo-relative path per line. This is the format of
# bin-cache/<sha>/deps-<target>.txt that Ural's `jobq` uses as the cache key
# ("dc1") of a measurement (see `jobq deps-extract`, which it mirrors).
#
# Input: the `*.o.d` depfiles that CMake's Makefile generator writes next to
# each object. Third-party `_deps`, system headers and generated files in the
# build directory are left out; the build files and the build profile pin them.
# Depfile paths are absolute, or relative to the compile directory (the
# directory above `CMakeFiles/`) when ccache's CCACHE_BASEDIR rewrote them.
# Both forms are resolved.
# Exit 3 when no depfile is found (e.g. a Ninja build keeps its deps in
# .ninja_deps). The caller then ships no list, and jobq falls back to its
# coarse key.
import glob
import os
import re
import sys


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: deps-extract.py <cmake-build-dir> <target>")
    build = os.path.realpath(sys.argv[1])
    target = sys.argv[2]
    home = ""
    with open(os.path.join(build, "CMakeCache.txt"), errors="replace") as f:
        for line in f:
            if line.startswith("CMAKE_HOME_DIRECTORY:"):
                home = os.path.realpath(line.split("=", 1)[1].strip())
    tdirs = glob.glob(os.path.join(build, "**", "CMakeFiles", f"{target}.dir"), recursive=True)
    if not tdirs or not home:
        sys.exit(f"no CMakeFiles/{target}.dir or CMAKE_HOME_DIRECTORY in {build}")
    dirs = set(tdirs)
    link = os.path.join(tdirs[0], "link.txt")
    if os.path.exists(link):
        with open(link) as f:
            libs = set(re.findall(r"lib([A-Za-z0-9_.+-]+)\.a\b", f.read()))
        for lib in libs:
            dirs.update(glob.glob(os.path.join(build, "**", "CMakeFiles", f"{lib}.dir"), recursive=True))
    paths, ndep = set(), 0
    for d in dirs:
        cwd = d.split(os.sep + "CMakeFiles" + os.sep)[0]
        for df in glob.glob(os.path.join(d, "**", "*.o.d"), recursive=True):
            ndep += 1
            with open(df, errors="replace") as f:
                body = f.read().replace("\\\n", " ")
            body = body.split(":", 1)[1] if ":" in body else ""
            for tok in body.split():
                if tok.endswith(":"):  # phony targets of -MP
                    continue
                p = os.path.normpath(tok if os.path.isabs(tok) else os.path.join(cwd, tok))
                if (p.startswith(home + os.sep) and "/_deps/" not in p
                        and not p.startswith(build + os.sep)):
                    paths.add(p[len(home) + 1:])
    if ndep == 0:
        print(f"deps-extract: no *.o.d depfiles for {target} in {build}", file=sys.stderr)
        sys.exit(3)
    for p in sorted(paths):
        print(p)


if __name__ == "__main__":
    main()
