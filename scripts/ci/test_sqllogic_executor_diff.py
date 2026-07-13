#!/usr/bin/env python3
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DIFF_SCRIPT = REPO_ROOT / "scripts" / "ci" / "sqllogic_executor_diff.py"

# Stands in for run_tests.py: the full-suite phase (no --test-list) reports a mix
# of failure shapes; the targeted re-runs read the --test-list file, record what
# they were asked to run, and fail a phase-specific subset.
STUB_RUNNER = """
import os
import sys

record_dir = os.environ["DIFF_STUB_RECORD_DIR"]
executor = os.environ["DUCKDB_SQLLOGIC_EXECUTOR"]
args = sys.argv[1:]
tests = None
if "--test-list" in args:
    with open(args[args.index("--test-list") + 1], encoding="utf8") as f:
        tests = [line.rstrip("\\n") for line in f if line.strip()]
    with open(os.path.join(record_dir, executor + ".list"), "w", encoding="utf8") as f:
        f.write("\\n".join(tests))

if tests is None:
    print("error: FAIL V2: a test with spaces")
    print("error: FAIL test/sql/b.test_slow")
    print("error: timeout (600s) for test/sql/c.test_slow.")
    print("error: test batch failed")
    print("suspect tests: test/sql/d.test\\ttest/sql/e.test")
    print("recovered: FAIL test/sql/recovered.test")
    print("error: timeout (600s) for test batch.")
    raise SystemExit(1)
if executor == "internal":
    print("error: FAIL V2: a test with spaces")
    raise SystemExit(1)
print("error: FAIL test/sql/b.test_slow")
raise SystemExit(1)
"""


def write_stub(directory: Path, source: str) -> Path:
    stub_path = directory / "stub_runner.py"
    stub_path.write_text(source, encoding="utf8")
    return stub_path


def run_diff(stub_path: Path, record_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(DIFF_SCRIPT), "--", sys.executable, str(stub_path), "*"],
        capture_output=True,
        text=True,
        encoding="utf8",
        env=dict(os.environ, DIFF_STUB_RECORD_DIR=str(record_dir)),
    )


class SqllogicExecutorDiffTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.record_dir = Path(self.tmp.name)

    def read_record(self, executor: str) -> list[str]:
        return (self.record_dir / f"{executor}.list").read_text(encoding="utf8").splitlines()

    def test_classifies_env_flaky_and_confirmed(self):
        stub = write_stub(self.record_dir, STUB_RUNNER)
        proc = run_diff(stub, self.record_dir)

        # The re-runs received the failing set through the --test-list file,
        # including the Catch2 name with spaces and the suspect-tests batch;
        # the recovered test and the unattributed timeout placeholder did not
        # count as failures.
        self.assertEqual(
            self.read_record("internal"),
            sorted(
                [
                    "V2: a test with spaces",
                    "test/sql/b.test_slow",
                    "test/sql/c.test_slow",
                    "test/sql/d.test",
                    "test/sql/e.test",
                ]
            ),
        )
        self.assertEqual(
            self.read_record("cpp_api"),
            sorted(["test/sql/b.test_slow", "test/sql/c.test_slow", "test/sql/d.test", "test/sql/e.test"]),
        )

        self.assertEqual(proc.returncode, 1)
        self.assertIn("environment (fail under internal too): 1", proc.stdout)
        self.assertIn("= V2: a test with spaces", proc.stdout)
        self.assertIn("flaky (passed cpp_api on retry): 3", proc.stdout)
        self.assertIn("cpp_api-only DIVERGENCES (1)", proc.stdout)
        self.assertIn("! test/sql/b.test_slow", proc.stdout)

    def test_green_when_full_suite_passes(self):
        stub = write_stub(self.record_dir, "raise SystemExit(0)\n")
        proc = run_diff(stub, self.record_dir)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("no divergences", proc.stdout)

    def test_infra_error_when_run_reports_no_failures(self):
        # A nonzero exit with no parseable failing tests (e.g. the runner
        # selected zero tests or crashed) must fail the job, not read as green.
        stub = write_stub(self.record_dir, "print('error: no tests selected')\nraise SystemExit(1)\n")
        proc = run_diff(stub, self.record_dir)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("without reporting failing tests", proc.stdout)

    def test_infra_error_when_rerun_reports_no_failures(self):
        stub_source = STUB_RUNNER.replace(
            'if executor == "internal":\n    print("error: FAIL V2: a test with spaces")\n    raise SystemExit(1)\n',
            'if executor == "internal":\n    print("error: no tests selected")\n    raise SystemExit(1)\n',
        )
        self.assertNotEqual(stub_source, STUB_RUNNER)
        stub = write_stub(self.record_dir, stub_source)
        proc = run_diff(stub, self.record_dir)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("without reporting failing tests", proc.stdout)


if __name__ == "__main__":
    unittest.main()
