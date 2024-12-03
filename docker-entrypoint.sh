#!/bin/bash

# Entrypoint script for the QLever docker image. It sets the UID and GID of the
# user `qlever` inside the container to the UID and GID specified in the call of
# `docker run`, typically the UID and GID of the user outside the container.
# That way, we don't need to set special permissions for the mounted volume,
# and everything looks nice inside of the container, too.
#
# NOTE: The container should be started with `-e UID=... -e GID=...` and not
# `-u ...:...` for the following reason. In order to change the UID and GID of
# the internal user `qlever`, we need `sudo` rights, which are granted to that
# user via the configuration in the Dockerfile. However, if we run the container
# with `-u ...:...`, the user changes and no longer has `sudo` rights.

# Help message that is printed if the container is not startes as recommended.
HELP_MESSAGE='
The recommended way to run a container with this image is as follows (run in a fresh directory, and adapt the ports to your needs):

In batch mode:

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -p 7019:7019 -v $(pwd):/data -w /data qlever -c "qlever setup-config olympics && qlever get-data && qlever index && qlever start && qlever example-queries"\x1b[0m

In interactive mode (you can then call `qlever` inside the container):

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -p 7019:7019 -v $(pwd):/data -w /data qlever\x1b[0m

If you prefer `-u $(id -u):$(id -g)`, set the entrypoint to `bash`:

\x1b[34mdocker run -it --rm -u $(id -u):$(id -g) -p 7019:7019 -v $(pwd):/data -w /data --entrypoint bash qlever -c "..."\x1b[0m

Explanation: With the first two options, there will be a user `qlever` inside the container, which acts like you when reading or writing outside files. With the third option, the user inside the container is you, but without a proper user and group name (which is fine for batch mode, but ugly in interactive mode).
'

# Show the `HELP_MESSAGE`. For now, don't show `$1` (see below), but start
# with a friendly welcome instead.
HELP() {
  echo
  # echo -e "\x1b[31m$1\x1b[0m"
  echo -e "\x1b[34mWELCOME TO THE QLEVER DOCKER IMAGE\x1b[0m"
  echo -e "$HELP_MESSAGE"
  exit 1
}

# Check that UID and GID are set and that the working directory is `/data`.
if [ -z "$UID" ] || [ -z "$GID" ]; then
  HELP "Environment variables UID and GID not set"
elif [ "$(pwd)" != "/data" ]; then
  HELP "The working directory should be /data, but it is $(pwd)"
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
