# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x (latest patch) | Yes, until Fabric Runtime 1.3 leaves support (currently March 2027) |
| Earlier 1.x releases | No: upgrade to the latest 1.x patch |

Security fixes for 1.x ship as patch releases from the `1.x` branch. See the
support window in the [compatibility reference](docs/reference/compatibility.md).

## Reporting a Vulnerability

If you believe you have discovered a security vulnerability in **weevr**, please report it responsibly.

* Do **not** open a public GitHub issue for security-sensitive findings.
* Instead, contact the project maintainers directly with a clear description of the issue.

When reporting, please include:

* A brief description of the vulnerability
* Steps to reproduce (if applicable)
* Potential impact
* Any relevant logs, traces, or proof-of-concept details (sanitized)

## Scope

This security policy applies to:

* Source code in this repository
* Release artifacts produced from this repository
* CI and release workflows defined in this repository

It does not apply to downstream systems, third-party services, or environments where **weevr** may be deployed.

## Disclosure and Response

The project maintainers will:

* Acknowledge receipt of the report
* Assess the issue in a timely manner
* Determine appropriate remediation steps

Fixes may be released without advance notice. Public disclosure should be coordinated with the maintainers to avoid unnecessary risk.

## Intent

This policy exists to encourage responsible disclosure and to keep the project and its users safe, without introducing unnecessary process overhead.
