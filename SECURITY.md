# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub's private vulnerability
reporting: open the repository's **Security** tab and choose **Report a
vulnerability** (https://github.com/AbdullaK123/wire_rpc/security/advisories/new).

Do not open a public issue for a vulnerability. Include what you found, how to
reproduce it, and which version or commit you tested.

## Supported versions

Only the latest release receives security fixes while wire_rpc is at `0.x`.

## Scope

In scope: anything in the `wire_rpc` package, including authentication, session
handling, transport limits and error sanitisation. Deployment responsibilities are
listed in `docs/security.md` (TLS and proxy setup, rate limiting,
application authorization) and are out of scope unless the library makes them
impossible to do safely.
