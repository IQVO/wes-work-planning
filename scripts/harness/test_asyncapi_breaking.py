#!/usr/bin/env python3
"""Tests for asyncapi_breaking.py. Run: python3 scripts/harness/test_asyncapi_breaking.py

The in-script self-check is the substance (three breaking rules + additive
non-breaking fixture); this wrapper pins it as a named local/CI gate and adds
the argument-contract checks. Stdlib-only: PyYAML is NOT required here (the
real-spec comparison that needs it runs in the spec-breaking CI job).
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "asyncapi_breaking.py")

sys.path.insert(0, HERE)
import asyncapi_breaking  # noqa: E402


def main() -> int:
    asyncapi_breaking.self_test()
    print("in-process self-check: ok")

    proc = subprocess.run([sys.executable, SCRIPT, "--self-test"], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "self-check passed" in proc.stdout, proc.stdout

    proc = subprocess.run([sys.executable, SCRIPT], capture_output=True, text=True)
    assert proc.returncode != 0 and "BASE_YAML HEAD_YAML" in proc.stderr, (proc.returncode, proc.stderr)

    pinners = set(asyncapi_breaking.PINNERS)
    assert pinners == {"exception-management", "fulfillment-execution",
                       "network-fulfillment", "order-management"}, sorted(pinners)
    print("test_asyncapi_breaking: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
