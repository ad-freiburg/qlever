#!/usr/bin/env bash
# fetch-hosted-bin.sh — install a build-binaries.yml artifact on Ural.
#
#   fetch-hosted-bin.sh <run-id>          download artifact bin-<sha> of that run
#   fetch-hosted-bin.sh --tar <file.tar>  install a bin-<sha>.tar fetched elsewhere
#
# Unpacks into $STORE/<sha>/ (the Wolga u24 layout: <target>, CMakeCache.txt,
# META-<target>, plus META), links $STORE/by-branch/<ref>/<target> when the
# ref is a branch, and checks every binary with ldd and --version/--help.
#
# Download auth: listing a public repo's artifacts works without a token, but
# GitHub serves the artifact zip only to authenticated requests (HTTP 401
# otherwise). The script uses, in order: `gh` (if installed and logged in),
# then $GH_TOKEN / $GITHUB_TOKEN, then the file ~/.config/hosted-bin/token.
# Any token works for a public repo (fine-grained, "Public repositories
# (read-only)", no extra permissions). Without a token: download the tar on a
# machine with gh and use --tar (see the usage notes).
#
# Env: HOSTED_BIN_REPO (marvin7122/qlever), WOLGA_BIN_STORE
# (/local/data-ssd/stoetzem/bin-cache), URING_LIB
# (/local/data-ssd/stoetzem/liburing-install/lib), FORCE=1 overwrites
# binaries already in the store (default: keep them, e.g. Wolga builds).
set -euo pipefail

REPO="${HOSTED_BIN_REPO:-marvin7122/qlever}"
STORE="${WOLGA_BIN_STORE:-/local/data-ssd/stoetzem/bin-cache}"
URING_LIB="${URING_LIB:-/local/data-ssd/stoetzem/liburing-install/lib}"
API="https://api.github.com/repos/$REPO"
FORCE="${FORCE:-0}"

die() { echo "FATAL: $*" >&2; exit 1; }
usage() { sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/hosted-bin.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

token() {
  if [ -n "${GH_TOKEN:-}" ]; then printf '%s' "$GH_TOKEN"
  elif [ -n "${GITHUB_TOKEN:-}" ]; then printf '%s' "$GITHUB_TOKEN"
  elif [ -r "$HOME/.config/hosted-bin/token" ]; then tr -d ' \n\r' < "$HOME/.config/hosted-bin/token"
  fi
}

download() { # run-id -> $tmp/bin-<sha>.tar
  local run="$1" meta name id tok
  case "$run" in *[!0-9]*|'') die "run id must be numeric, got '$run'" ;; esac
  meta=$(curl -fsS "$API/actions/runs/$run/artifacts?per_page=100") \
    || die "cannot list artifacts of run $run in $REPO"
  read -r name id < <(printf '%s' "$meta" | python3 -c '
import json, sys
arts = [a for a in json.load(sys.stdin)["artifacts"]
        if a["name"].startswith("bin-") and not a["expired"]]
print(arts[0]["name"], arts[0]["id"]) if arts else print("", "")') || true
  [ -n "$name" ] || die "run $run has no (unexpired) bin-* artifact yet; still running? gh run view $run -R $REPO"
  echo "artifact $name (id $id) from run $run"
  if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
    gh run download "$run" -R "$REPO" -n "$name" -D "$tmp/dl"
  else
    tok=$(token)
    [ -n "$tok" ] || die "downloading an artifact needs a GitHub token (HTTP 401 without).
  Put one in ~/.config/hosted-bin/token (chmod 600) or export GH_TOKEN,
  or download elsewhere: gh run download $run -R $REPO -n $name
  and run: $0 --tar $name.tar"
    curl -fsSL -H "Authorization: Bearer $tok" -o "$tmp/a.zip" \
      "$API/actions/artifacts/$id/zip" || die "artifact download failed (token valid?)"
    mkdir -p "$tmp/dl" && unzip -q "$tmp/a.zip" -d "$tmp/dl"
  fi
  TAR=$(ls "$tmp"/dl/bin-*.tar 2>/dev/null | head -1)
  [ -n "$TAR" ] || die "artifact does not contain bin-<sha>.tar"
}

case "${1:-}" in
  ''|-h|--help) usage ;;
  --tar) [ -f "${2:-}" ] || die "--tar needs an existing file"; TAR="$2" ;;
  *) download "$1" ;;
esac

mkdir -p "$tmp/x"
tar -C "$tmp/x" -xf "$TAR"
set -- "$tmp"/x/*/META
[ -f "$1" ] || die "no <sha>/META in $TAR"
src=$(dirname "$1")
sha=$(sed -n 's/^sha=//p' "$1"); ref=$(sed -n 's/^ref=//p' "$1")
targets=$(sed -n 's/^targets=//p' "$1")
[ "$(basename "$src")" = "$sha" ] || die "tar dir $(basename "$src") != META sha $sha"
cat "$1"

dst="$STORE/$sha"
mkdir -p "$dst"
# Branch refs get a by-branch link like Wolga's; SHAs, tags-as-sha and
# empty refs do not.
link_branch=""
if [ -n "$ref" ] && ! printf '%s' "$ref" | grep -Eq '^[0-9a-f]{7,40}$'; then link_branch="$ref"; fi

status=0
installed=0
for t in $targets; do
  [ -x "$src/$t" ] || die "$t missing in artifact"
  if [ -e "$dst/$t" ] && [ "$FORCE" != 1 ]; then
    echo "KEEP $dst/$t (exists; FORCE=1 to overwrite)"
  else
    cp -f "$src/META-$t" "$dst/META-$t"
    # Binary last, atomically (a --wait-for on the path sees a whole file).
    cp -f "$src/$t" "$dst/.$t.tmp.$$" && mv -f "$dst/.$t.tmp.$$" "$dst/$t"
    installed=1
    echo "INSTALLED $dst/$t"
  fi
  if [ -n "$link_branch" ]; then
    mkdir -p "$STORE/by-branch/$link_branch"
    ln -sfn "$dst/$t" "$STORE/by-branch/$link_branch/$t"
  fi
  # Runs on Ural? All libraries resolve, and the binary starts.
  if LD_LIBRARY_PATH="$URING_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ldd "$dst/$t" | grep -q 'not found'; then
    LD_LIBRARY_PATH="$URING_LIB" ldd "$dst/$t" | grep 'not found' >&2
    echo "FAIL $t: unresolved libraries" >&2; status=1; continue
  fi
  if [ "$t" = qlever-server ]; then chk=(--version); else chk=(--help); fi
  # Benchmarks print usage on --help and exit 1; a loader failure is 127, a
  # crash >128. Accept 0 and 1 with output, and no loader error.
  rc=0
  out=$(LD_LIBRARY_PATH="$URING_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" timeout 30 "$dst/$t" "${chk[@]}" 2>&1) || rc=$?
  if [ "$rc" -le 1 ] && [ -n "$out" ] && ! printf '%s' "$out" | grep -q 'error while loading shared libraries'; then
    echo "OK   $t ${chk[*]} (exit $rc): $(printf '%s\n' "$out" | head -1)"
  else
    printf '%s\n' "$out" | tail -5 >&2
    echo "FAIL $t ${chk[*]} exit $rc" >&2; status=1
  fi
done
# The CMake cache describes the build next to it: take the hosted one only
# when this dir has none, or when we installed a binary with FORCE.
if [ ! -e "$dst/CMakeCache.txt" ] || { [ "$FORCE" = 1 ] && [ "$installed" = 1 ]; }; then
  cp -f "$src/CMakeCache.txt" "$dst/CMakeCache.txt"
fi
cp -f "$src/META" "$dst/META"
echo "bin dir: $dst${link_branch:+  (by branch: $STORE/by-branch/$link_branch/)}"
exit "$status"
