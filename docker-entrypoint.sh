#!/bin/bash

# Entrypoint script for the QLever docker image. It sets the UID and GID of the
# user `qlever` inside the container to the UID and GID specified in the call of
# `docker run`, typically the UID and GID of the user outside the container.
# That way, we don't need to set special permissions for the mounted volume,
# and everthing looks nice inside of the container, too.
#
# NOTE: The container should be started with `-e UID=... -e GID=...` and not
# `-u ...:...` for the following reason. In order to change the UID and GID of
# the internal user `qlever`, we need `sudo` rights, which are granted to that
# user via the configuration in the Dockerfile. However, if we run the container
# with `-u ...:...`, the user changes and no longer has `sudo` rights.

# Help message that is printed if the container is not startes as recommended.
HELP_MESSAGE='
The recommended way to run a container with this image is as follows:

In interactive mode:

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever\x1b[0m

In batch mode (example, add `-p <outside port>:<inside port>` for outside access to the server):

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever -c "qlever setup-config olympics && qlever get-data && qlever index && qlever start && qlever example-queries"\x1b[0m
'

# Helper function to print an error message (in red) and the help message.
ERROR() {
  echo
  echo -e "\x1b[31m$1\x1b[0m"
  echo -e "$HELP_MESSAGE"
  exit 1
}

# Check that UID and GID are set and that the working directory is `/data`.
if [ -z "$UID" ] || [ -z "$GID" ]; then
  ERROR "Environment variables UID and GID not set"
elif [ "$(pwd)" != "/data" ]; then
  ERROR "The working directory should be /data, but it is $(pwd)"
fi

# If `docker run` is run without arguments, start an interactive shell. Otherwise,
# run the sequence of commands given as arguments (the first argument may be the
# standard `-c`, but it can also be omitted).
#
# NOTE: The call `su - qlever ...` has to be inside of the `sudo` call, because once
# the UID and GID of the user `qlever` have been changed, it no longer has `sudo`
# rights. And just remaining in the shell or starting a new shell (with `bash`) does
# not work, because neither of these would have the new UID and GID. Hence also
# the slight code duplication.
if [ $# -eq 0 ]; then
  echo
  sudo -E bash -c "usermod -u $UID -s /bin/bash qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && su - qlever --login"
else
  if [ "$1" == "-c" ]; then
    shift
  fi
  sudo -E bash -c "usermod -u $UID -s /bin/bash qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && su - qlever -s /bin/bash --login -c \"$@\""
fi
