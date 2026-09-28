# Copyright 2026 The QLever Authors, in particular:
#
# 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
#
# UFR = University of Freiburg, Chair of Algorithms and Data Structures
#
# You may not use this file except in compliance with the Apache 2.0 License,
# which can be found in the `LICENSE` file at the root of the QLever project.

# This file deliberately shadows the file of the same name in HPX's own `cmake`
# directory (together with `HPX_AddPseudoDependencies.cmake` next to it). See
# the `USE_HPX` block of the top-level `CMakeLists.txt`, which puts the
# directory of this file into the `CMAKE_MODULE_PATH` before HPX appends its
# own directory, so that `include(HPX_AddPseudoTarget)` finds this file.
#
# The reason is that HPX creates convenience targets (it calls them "pseudo
# targets") that are named after its module and source directories, among them
# a target named `core`. QLever already has a target with that name (the `core`
# library of `prometheus-cpp`, which is vendored by `opentelemetry-cpp`), and
# CMake requires target names to be globally unique, so that adding HPX fails
# with `add_custom_target cannot create target "core" because another target
# with the same name already exists`.
#
# We therefore create all of HPX's pseudo targets with the prefix `hpx.`, which
# moves them out of the way of QLever's target names (`core` becomes
# `hpx.core`). Only the names of the convenience targets are affected, the
# names of the actual libraries (`hpx_core`, `hpx`, ...) are not.

set(HPX_ADDPSEUDOTARGET_LOADED TRUE)

# The prefix that this file and `HPX_AddPseudoDependencies.cmake` prepend to the
# name of every pseudo target.
set(QLEVER_HPX_PSEUDO_TARGET_PREFIX "hpx.")

function(add_hpx_pseudo_target)
  if(HPX_WITH_PSEUDO_DEPENDENCIES)
    set(prefixed_args)
    foreach(arg ${ARGV})
      set(prefixed_args ${prefixed_args}
                        "${QLEVER_HPX_PSEUDO_TARGET_PREFIX}${arg}"
      )
    endforeach()
    add_custom_target(${prefixed_args})
  endif()
endfunction()
