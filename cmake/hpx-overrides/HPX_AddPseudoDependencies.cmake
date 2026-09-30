# Copyright 2026 The QLever Authors, in particular:
#
# 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
#
# UFR = University of Freiburg, Chair of Algorithms and Data Structures
#
# You may not use this file except in compliance with the Apache 2.0 License,
# which can be found in the `LICENSE` file at the root of the QLever project.

# This file deliberately shadows the file of the same name in HPX's own `cmake`
# directory. See `HPX_AddPseudoTarget.cmake` next to it, which explains why
# QLever renames HPX's pseudo targets and how the shadowing works.
#
# HPX declares dependencies between its pseudo targets with the function below,
# and it also uses that function to make a pseudo target depend on one of its
# *real* targets (for example `add_hpx_pseudo_dependencies(core hpx_init)`).
# Only the former have been renamed, so an argument is renamed here if and only
# if a pseudo target of that name exists. That is always decidable, because HPX
# creates each of its pseudo targets before it declares a dependency on it.

set(HPX_ADDPSEUDODEPENDENCIES_LOADED TRUE)

include(HPX_AddPseudoTarget)

function(add_hpx_pseudo_dependencies)
  if(HPX_WITH_PSEUDO_DEPENDENCIES)
    set(prefixed_args)
    foreach(arg ${ARGV})
      set(prefixed_arg "${QLEVER_HPX_PSEUDO_TARGET_PREFIX}${arg}")
      if(TARGET ${prefixed_arg})
        set(prefixed_args ${prefixed_args} ${prefixed_arg})
      else()
        set(prefixed_args ${prefixed_args} ${arg})
      endif()
    endforeach()
    add_dependencies(${prefixed_args})
  endif()
endfunction()

function(add_hpx_pseudo_dependencies_no_shortening)
  if(HPX_WITH_PSEUDO_DEPENDENCIES)
    add_dependencies(${ARGV})
  endif()
endfunction()
