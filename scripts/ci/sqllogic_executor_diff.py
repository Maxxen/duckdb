#!/usr/bin/env python3
"""Run the sqllogic suite under cpp_api and flag only cpp_api-specific divergences.

Some tests fail for reasons unrelated to the executor (resource-heavy tests on small
CI runners), so a single-executor job goes red on noise. Running the whole suite
under both executors would double the wall time. The asymmetric diff:

  1. Run the full suite under cpp_api -> failing set C.
  2. If C is empty: green.
  3. Re-run C under internal (with retries). Tests that also fail there are
     environment failures; the rest are divergence candidates.
  4. Re-run the candidates under cpp_api (with retries). Tests that still fail are
     real divergences and fail the job; the rest were flaky.

A run that exits nonzero without reporting a single failing test fails the job:
an empty parse must never read as green.

Usage:
    sqllogic_executor_diff.py -- <command that runs the suite> [args...]

The command's LAST argument must be the test pattern (e.g. "*"); re-runs replace it
with a --test-list file.

e.g. sqllogic_executor_diff.py -- ./build/reldebug/test/run --test-config=c.json "*"
"""
import os
import re
import subprocess
import sys
import tempfile

ANSI = re.compile(r"\x1b\[[0-9;]*m")
# Failure lines emitted by run_tests.py. "test batch" means no attributed test; the
# "suspect tests:" line that follows lists the whole batch, and each one counts as failing.
FAIL_RE = re.compile(r"^error:\s+FAIL\s+(.+?)\s*$")
TIMEOUT_RE = re.compile(r"^error:\s+timeout\s+\([0-9.]+s\)\s+for\s+(.+?)\.?\s*$")
SUSPECTS_RE = re.compile(r"^suspect tests: (.+?)\s*$")
UNATTRIBUTED = "test batch"
RERUN_FLAGS = ["--retry", "3", "--batch-size", "1"]


def run(command, executor, label):
    """Run command with the given executor, streaming output and collecting failures."""
    env = dict(os.environ, DUCKDB_SQLLOGIC_EXECUTOR=executor)
    print(f"::group::{label} (executor={executor}): {' '.join(command)}", flush=True)
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
            if match.group(1) != UNATTRIBUTED:
                failing.add(match.group(1))
            continue
        suspects = SUSPECTS_RE.match(clean)
        if suspects:
            failing.update(name for name in suspects.group(1).split("\t") if name)
    returncode = proc.wait()
    print("::endgroup::", flush=True)
    print(f"{label}: exit={returncode}, {len(failing)} failing test(s)", flush=True)
    return returncode, failing


def run_targeted(base, tests, executor, label):
    """Re-run an explicit test list.

    Names go through a --test-list file because Catch2 ANDs positional test specs:
    two or more names as patterns select zero tests. One test per batch, so a
    timeout or crash cannot hide or misattribute its batch mates.
    """
    fd, path = tempfile.mkstemp(prefix="executor-diff-", suffix=".list")
    try:
        with os.fdopen(fd, "w", encoding="utf8") as test_list:
            test_list.write("\n".join(tests) + "\n")
        return run(base + RERUN_FLAGS + ["--test-list", path], executor, label)
    finally:
        os.unlink(path)


def infra_error(returncode, failing, label):
    if returncode != 0 and not failing:
        print(f"error: {label} exited {returncode} without reporting failing tests; cannot diff.")
        return True
    return False


def main(argv):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    command = argv[argv.index("--") + 1 :] if "--" in argv else argv
    if len(command) < 2:
        print("usage: sqllogic_executor_diff.py -- <runner> [flags...] <pattern>", file=sys.stderr)
        return 2
    base = command[:-1]  # runner + flags; the trailing pattern is dropped for re-runs

    # 1. Full suite under cpp_api.
    returncode, cpp_fail = run(command, "cpp_api", "cpp_api: full suite")
    if infra_error(returncode, cpp_fail, "cpp_api: full suite"):
        return 2
    if not cpp_fail:
        print("\ncpp_api passed the full suite; no divergences.")
        return 0

    # 2. Re-check the cpp_api failures under internal (retried to stabilize env flakes).
    returncode, internal_fail = run_targeted(base, sorted(cpp_fail), "internal", "internal: re-check cpp_api failures")
    if infra_error(returncode, internal_fail, "internal: re-check cpp_api failures"):
        return 2
    env_shared = sorted(cpp_fail & internal_fail)
    candidates = sorted(cpp_fail - internal_fail)

    # 3. Confirm candidates under cpp_api with retries; drop those that pass on retry (flaky).
    confirmed, flaky = candidates, []
    if candidates:
        returncode, confirm_fail = run_targeted(base, candidates, "cpp_api", "cpp_api: confirm candidates (retried)")
        if infra_error(returncode, confirm_fail, "cpp_api: confirm candidates (retried)"):
            return 2
        confirmed = sorted(set(candidates) & confirm_fail)
        flaky = sorted(set(candidates) - confirm_fail)

    print("")
    print("================ executor diff ================")
    print(f"cpp_api failures (full suite): {len(cpp_fail)}")
    if env_shared:
        print(f"  environment (fail under internal too): {len(env_shared)}")
        for test in env_shared:
            print(f"    = {test}")
    if flaky:
        print(f"  flaky (passed cpp_api on retry): {len(flaky)}")
        for test in flaky:
            print(f"    ~ {test}")
    if confirmed:
        print(f"cpp_api-only DIVERGENCES ({len(confirmed)}) -- failing the job:")
        for test in confirmed:
            print(f"    ! {test}")
        return 1
    print("no cpp_api-specific divergences; all cpp_api failures are environment or flaky.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
