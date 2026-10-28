# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately, not in a public issue or pull request:

- Open a private report at https://github.com/xingxerx/QALLOW/security/advisories/new
- Include what you found, how to reproduce it, and the version or commit you tested.

This project is maintained by one person, so replies are best effort. Please give a reasonable
amount of time to investigate and fix before any public disclosure. Do not include real credentials,
tokens or personal data in a report; redact them.

## Supported versions

Only the latest code on the protected branch (`main` or `master`) receives security fixes.

## What is in scope

Anything in this repository that could expose credentials, run code with more privilege than intended,
bypass an authentication or integrity check, or let untrusted input reach a privileged component.
Hardening guidance for this stack is kept in the ATRIUM repository (`SECURITY-BASELINE.md`).
