# Releasing

wire_rpc is published to PyPI as `wire-rpc` by `.github/workflows/release.yml`,
using PyPI trusted publishing: GitHub proves the workflow's identity to PyPI, so
no API token is stored anywhere.

## Versioning

Semantic versioning, as described at the top of [CHANGELOG.md](../CHANGELOG.md).
While the version is `0.x`, a minor release may break compatibility and a patch
release never does. Every breaking change goes under **Breaking** with what users
should do.

## Steps

1. Make sure `main` is green in CI.
2. Set `version` in `pyproject.toml`, then run `uvx --from uv==0.12.18 uv lock`
   so the lockfile's project version matches.
3. In `CHANGELOG.md`, add a `## [x.y.z] - YYYY-MM-DD` heading under
   `## [Unreleased]`, so the unreleased entries now sit under it, and leave
   `## [Unreleased]` empty above it.
4. Commit and push.
5. Optional dry run: Actions → Release → Run workflow. It runs every check and
   uploads to TestPyPI.
6. Create a GitHub Release with the tag `vx.y.z` and publish it.

The workflow then:

1. checks that the tag matches the version in `pyproject.toml`, and stops if not,
2. runs pyrefly, ruff, and the full test suite including the Docker tests,
3. builds the sdist and wheel and smoke-tests the installed wheel,
4. uploads to PyPI.

## A version number can't be reused

Once a version is uploaded to PyPI, that number can never be uploaded again, even
if you delete the release. If something is wrong with a published release, fix it
and release the next patch version. Use the TestPyPI dry run when in doubt.

## One-time setup

Needed before the workflow can publish. Check these are in place before your
first release through it.

1. On PyPI and on TestPyPI, add a trusted publisher for owner `AbdullaK123`,
   repository `wire_rpc`, workflow `release.yml`, with environment `pypi` (PyPI)
   and `testpypi` (TestPyPI).
2. In the GitHub repository, create the `pypi` and `testpypi` environments.
   Adding a required reviewer to `pypi` makes every release wait for approval.
3. Turn on private vulnerability reporting (Settings → Security) so the link in
   `SECURITY.md` works.

## Publishing by hand

If the workflow is unavailable:

```sh
uv build --no-sources
uv publish --publish-url https://test.pypi.org/legacy/ --token <TestPyPI token>   # dry run
uv publish --token <PyPI token>
```

Create tokens under Account settings → API tokens, scoped to the `wire-rpc`
project. Don't commit them.
