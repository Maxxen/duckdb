#!/usr/bin/env python3
"""Run the sqllogic suite under both executors and fail only on cpp_api divergences.

The C-API sqllogic runner (DUCKDB_SQLLOGIC_EXECUTOR=cpp_api) should produce the
same pass/fail verdict as the internal executor (=internal) for every test. A
plain single-executor CI job cannot tell a genuine cpp_api regression apart from
an environment-driven failure (resource-heavy .test_slow that fail on small CI
runners regardless of executor), so it goes red on noise.

This wrapper runs the given command once per executor, extracts each run's set of
failing tests, and fails iff some test fails under cpp_api but NOT under internal:

    divergences = cpp_api_failures - internal_failures

Environment failures appear in both sets and cancel out; quarantined tests are
skipped under cpp_api (never in its failure set) and pass under internal, so they
do not show up either. Only a real C-API runner regression survives the diff.

Usage:
    sqllogic_executor_diff.py -- <command that runs the suite> [args...]

e.g. sqllogic_executor_diff.py -- ./build/reldebug/test/run --test-config=c.json "*"
"""
import os
import re
import subprocess
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
# run_tests.py emits one of these per failing batch (see format_fail_header /
# render_failure_lines). The test name is what we diff on.
FAIL_RE = re.compile(r"^error:\s+FAIL\s+(.+?)\s*$")
TIMEOUT_RE = re.compile(r"^error:\s+timeout\s+\([0-9.]+s\)\s+for\s+(.+?)\.?\s*$")


def run(command, executor):
    """Run command with the given executor, streaming output and collecting failures."""
    env = dict(os.environ, DUCKDB_SQLLOGIC_EXECUTOR=executor)
    print(f"::group::sqllogic run (executor={executor}): {' '.join(command)}", flush=True)
    proc = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf8",
        errors="backslashreplace",
    )
    assert proc.stdout is not None  # guaranteed by stdout=PIPE
    failing = set()
    for line in proc.stdout:
        sys.stdout.write(line)
        clean = ANSI.sub("", line).rstrip("\n")
        match = FAIL_RE.match(clean) or TIMEOUT_RE.match(clean)
        if match:
            failing.add(match.group(1))
    returncode = proc.wait()
    print("::endgroup::", flush=True)
    print(f"executor={executor}: exit={returncode}, {len(failing)} failing test(s)", flush=True)
    return returncode, failing


def main(argv):
    command = argv[argv.index("--") + 1 :] if "--" in argv else argv
    if not command:
        print("usage: sqllogic_executor_diff.py -- <command that runs the suite>", file=sys.stderr)
        return 2

    internal_rc, internal_fail = run(command, "internal")
    cpp_rc, cpp_fail = run(command, "cpp_api")

    divergences = sorted(cpp_fail - internal_fail)
    shared = sorted(cpp_fail & internal_fail)

    print("")
    print("================ executor diff ================")
    print(f"internal failures: {len(internal_fail)}  (exit {internal_rc})")
    print(f"cpp_api  failures: {len(cpp_fail)}  (exit {cpp_rc})")
    if shared:
        print(f"shared failures (environment, not a divergence): {len(shared)}")
        for test in shared:
            print(f"  = {test}")

    if divergences:
        print(f"cpp_api-only DIVERGENCES ({len(divergences)}) -- failing the job:")
        for test in divergences:
            print(f"  ! {test}")
        return 1

    # Safety net for the rare failure run_tests.py reports without a resolvable
    # test name: if internal is fully clean but cpp_api is not, that gap is a
    # divergence even though we could not name it.
    if cpp_rc != 0 and internal_rc == 0:
        print("cpp_api failed while internal passed, but no failing test name was parsed -- failing the job.")
        return 1

    print("no cpp_api-specific divergences; any failures match the internal executor.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
