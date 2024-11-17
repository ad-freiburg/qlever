#!/bin/bash

# Entrypoint script for the QLever docker image.

HELP_MESSAGE='
The recommended way to run a container with this image is as follows:

In interactive mode:

docker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever

In batch mode (example):

docker run -it --rm -u $(id -u):$(id -g) -v $(pwd):/data -w /data --entrypoint bash qlever -c "qlever setup-config olympics && qlever get-data && qlever index && qlever start && qlever example-queries"
'

ERROR() {
  echo -e "\x1b[31m$1\x1b[0m"
  echo -e "$HELP_MESSAGE"
  exit 1
}

if [ -z "$UID" ] || [ -z "$GID" ]; then
  ERROR "Environment variables UID and GID not set"
elif [ "$(pwd)" != "/data" ]; then
  ERROR "The working directory must be /data, but it is $(pwd)"
else
  sudo -E bash -c "usermod -u $UID qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && sudo -E -u qlever bash"
  # Run the command as the qlever user.
  # exec su-exec qlever "$@"
fi
