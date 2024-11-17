#!/bin/bash

# Entrypoint script for the QLever docker image.

HELP_MESSAGE='
The recommended way to run a container with this image is as follows:

In interactive mode:

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever\x1b[0m

In batch mode (example):

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever -c "qlever setup-config olympics && qlever get-data && qlever index && qlever start && qlever example-queries"\x1b[0m
'

ERROR() {
  echo
  echo -e "\x1b[31m$1\x1b[0m"
  echo -e "$HELP_MESSAGE"
  exit 1
}

if [ -z "$UID" ] || [ -z "$GID" ]; then
  ERROR "Environment variables UID and GID not set"
elif [ "$(pwd)" != "/data" ]; then
  ERROR "The working directory should be /data, but it is $(pwd)"
else
  if [ $# -eq 0 ]; then
    echo
    sudo -E bash -c "usermod -u $UID -s /bin/bash qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && su - qlever --login"
  else
    if [ "$1" == "-c" ]; then
      shift
    fi
    sudo -E bash -c "usermod -u $UID -s /bin/bash qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && su - qlever -s /bin/bash --login -c \"$@\""
  fi
fi
