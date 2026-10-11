# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately via GitHub's private vulnerability
reporting: open the [Security tab](https://github.com/ad-freiburg/qlever/security)
of this repository and click "Report a vulnerability". The report is visible
only to you and the maintainers until a fix is published.

Please do not report vulnerabilities in public issues or pull requests. If
reporting via the Security tab is not possible, or you get no response within
7 days, write an e-mail to one of the core developers, with reminders until you
get a response.

## What a report should contain

- The affected component (for example, the SPARQL parser, the HTTP server, or
  the index builder) and the QLever version or commit.
- A reproducer: a query, an input file, or a sequence of requests that
  triggers the problem, together with the observed and the expected behavior.
- Your assessment of the impact (for example, crash of a public endpoint,
  memory disclosure, or code execution).

Reports found with the help of automated tools or AI are welcome if you have
verified them and can provide a reproducer. However, please make sure that
reports are succinct and not in the typical verbose AI-style; otherwise they
risk being closed without further action. We do not run a bug bounty program.

## What to expect

We acknowledge reports within one week. We will work with you on a fix, credit
you in the advisory unless you prefer otherwise, and publish the advisory once
a fixed release is available.

## Scope

In scope is the code in this repository, that is, the QLever graph database
system, its HTTP API, and its build and CI configuration. Issues in third-party
dependencies should be reported to the respective project.
