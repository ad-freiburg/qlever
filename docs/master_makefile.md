# QLever's "master" Makefile

If you work more with the QLever (and after you have tried the [Quickstart
demos](/docs/quickstart,md), the "master" Makefile is for you. It is both simple
and powerful. It provides Makefile targets for all the actions one typically
wants to do. For example

        make                # Shortcut for make show_config
        make show_config    # Show the configuration
        make index          # Build a QLever index (does NOT overwrite existing index)
        make remove-index   # Remove an existing index
        make start          # Start the engine
        make log            # Show the log
        ...                 # Many more

## How to configure the Makefile

How does the Makefile know which index you want to build for which datasets,
which index you want to start, etc.? This is controlled via Makefile variables.
For example, this is how the Makefile for build the "120 Years of Olympics"
dataset (see the Quickstart demo) looks like. It should be in the same folder,
where your dataset and index reside, that is
`$QLEVER_HOME/qlever-indices/<dataset_name>`.

        include $QLEVER_HOME/qlever-code/master.Makefile

        DB = olympics
        CAT_TTL = cat olympics.nt
        DOCKER_IMAGE = qlever
        MEMORY_FOR_QUERIES = 10
        CACHE_MAX_SIZE_GB = 10
        CACHE_MAX_SIZE_GB_SINGLE_ENTRY = 5
        CACHE_MAX_NUM_ENTRIES = 100
        PORT = 7001

The first line includes the "master" Makefile, where all the functionality for
the various Makefile targets is implemented. Then follow the variables specific
for a particular dataset.

To change the configuration, you can simple edit the Makefile. Alternatively,
you can also specify the desired value for a variable via the command line. For
example, if you want to start the engine on a different port, you can type `make
PORT=7024 start`. You can assign an arbitrary number of variables that way, just
separate the variable-assignement pairs by spaces.

## Explanation of the available configuration variables.

The variable `CAT_TTL` specifies how the input triples are written
to standard output. If you have several bz2-compressed turtle files, you could
simply write `CAT_TTL = bzcat *.ttl.bz2`. You get the idea.

The variable `DOCKER_IMAGE` specifies the docker image of the QLever code. This
is useful, if you want to test an experimental code branch.

The variable `MEMORY_FOR_QUERIES` specifies the amount of memory in GB that
QLever will allow to query processing. This is useful because QLever profits
from more RAM, but you don't want the engine to crash if a query uses more RAM
than is available (QLever is written in C++, and there is no fool-proof way to
prevent memory allocation errors from producing a segmentation fault).

TODO: explain the remaining variables. However, most of them have long explicit
names, which makes their function pretty self-explanatory.

##  Explanation of the available targets

TODO: explain the available targets.

In the meantime, just look at the targets in the
[master.Makefile](/master.Makefile). The Makefile syntax and semantics is very
simple. Targets are followed by a colon, followed by one or more lines of
commands, each of them indented by a tabulator. When invoking the target via
`make <target name>`, the sequence of commands is executed. The behavior can be
controlled by the variables mentioned above.
