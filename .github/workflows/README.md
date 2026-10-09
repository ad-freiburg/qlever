# QLever's CI and the `run-ci` / `run-full-ci` labels

To save CI resources, the checks of a pull request are controlled by labels:

| PR labels             | What is run                                         |
|-----------------------|-----------------------------------------------------|
| none                  | Nothing at all.                                     |
| `run-ci`              | The cheap subset of the checks (see below).         |
| `run-full-ci`         | All checks (the cheap and the expensive ones).      |

Pushes to `master`, tags, the merge queue, and manual dispatches always run
all checks.

All workflows that are triggered by `pull_request` also listen to the
`labeled` event, so applying one of the labels immediately triggers the
corresponding checks. GitHub cannot filter the `labeled` event by the name of
the label, so applying any other label also triggers a run (which behaves like
a run for a new push, in particular it cancels a run that is still in
progress).

## The cheap subset (`run-ci` or `run-full-ci`)

- `native-build-always.yml` (GCC 13, ASan, UBSan, C++17 backports)
- `cpp-17-libqlever.yml`
- `code-coverage.yml`
- `format-check.yml`
- `codespell.yml`
- `macos-appleclang-native.yml`
- `sparql-conformance-new.yml`

## The expensive checks (only `run-full-ci`)

- `native-build-only-if-complete-ci.yml` (all other native builds, incl. TSAN)
- `docker-publish.yml`
- `check_index_version.yml`
- `portable-binaries.yml`
- `native-build-with-conan-and-emscripten.yml`
- `native-build-conan.yml`
- `macos.yml`
- `sonarcloud.yml`

## Implementation

Each of the workflows above has the following `if:` condition on its first
job (later jobs depend on it via `needs:` and are hence skipped as well):

```yaml
# The cheap subset.
if: >-
  github.event_name != 'pull_request' ||
  contains(github.event.pull_request.labels.*.name, 'run-ci') ||
  contains(github.event.pull_request.labels.*.name, 'run-full-ci')
# The expensive checks.
if: >-
  github.event_name != 'pull_request' ||
  contains(github.event.pull_request.labels.*.name, 'run-full-ci')
```

GitHub Actions has no way to share such a condition between workflows. A
reusable workflow that computes it would itself need a runner for every PR
event, and it would make every run conclude with `success`, which in turn
triggers the `workflow_run`-based uploaders (`upload-coverage.yml`,
`upload-sonarcloud.yml`, `sparql-conformance-new-uploader.yml`). With the
inline conditions, a run in which all jobs are skipped concludes with
`skipped`, and the uploaders (which require `success`) are skipped as well, so
for a PR without a label really nothing runs.

When adding a new workflow that is triggered by `pull_request`, add the
`labeled` event type and one of the two conditions above.
