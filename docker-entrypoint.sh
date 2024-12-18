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
The recommended way to run a container with this image is as follows. Run in a fresh directory. Add `-p <outside port>:<inside port>` if you want to expose ports. Inside the container, the `qlever` command-line tool is available, as well as the QLever binaries (which you need not call directly, they are called by the various `qlever` commands).

In batch mode (user `qlever` inside the container, with the same UID and GID as outside):

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever -c "qlever setup-config olympics && qlever get-data && qlever index"\x1b[0m

The same, but in interactive mode:

\x1b[34mdocker run -it --rm -e UID=$(id -u) -e GID=$(id -g) -v $(pwd):/data -w /data qlever\x1b[0m

It also works with  `-u $(id -u):$(id -g)` (but then the user inside the container has no proper name):

\x1b[34mdocker run -it --rm -u $(id -u):$(id -g) -v $(pwd):/data -w /data qlever\x1b[0m
\x1b[34mdocker run -it --rm -u $(id -u):$(id -g) -v $(pwd):/data -w /data qlever -c "..."\x1b[0m

With podman you should use `-u $(id -u):$(id -g)` together with `--userns=keep-id`:

\x1b[34mpodman run -it --rm -u $(id -u):$(id -g) --userns=keep-id -v $(pwd):/data -w /data qlever\x1b[0m
\x1b[34mpodman run -it --rm -u $(id -u):$(id -g) --userns=keep-id -v $(pwd):/data -w /data qlever -c "..."\x1b[0m
'

# If the container is run without `-v ...:/data -w /data` (in particular, without
# any arguments), show the help message and exit.
if [ "$(pwd)" != "/data" ]; then
  echo
  echo -e "\x1b[34mWELCOME TO THE QLEVER DOCKER IMAGE\x1b[0m"
  echo -e "$HELP_MESSAGE"
  exit 1
fi

# If the container is run with arguments, but the first argument is not `-c`,
# prepend `-c` to the arguments (so that the user can omit the `-c`).
if [[ $# -gt 0 && "$1" != "-c" ]]; then
    set -- -c "$@"
fi

# If the user is not `qlever`, start a new login shell (to make sure that the
# profile script from the Dockerfile is executed).
# specified.
if ! whoami > /dev/null || [ "$(whoami)" != "qlever" ]; then
  exec bash --login "$@"
fi

# With `-e UID=... -e GID=...`, change the UID and GID of the user `qlever` inside
# the container accordingly.
#
# NOTE: The call `su - qlever ...` has to be inside of the `sudo` call, because
# once the UID and GID of the user `qlever` have been changed, it no longer has
# `sudo` rights. And just remaining in the shell or starting a new shell (with
# `bash`) does not work, because neither of these would have the new UID and GID.
if [ $# -eq 0 ]; then
  echo
  sudo -E bash -c "usermod -u $UID -s /bin/bash qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && su - qlever --login"
else
  if [ "$1" == "-c" ]; then
    shift
  fi
  sudo -E bash -c "usermod -u $UID -s /bin/bash qlever && groupmod -g $GID qlever && chown -R qlever:qlever /qlever && su - qlever -s /bin/bash --login -c \"$@\""
fi
