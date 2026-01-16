#!/usr/bin/env python3
"""Coverage validation script for CI pipelines.

This script runs pytest with coverage measurement and validates that
the minimum coverage threshold is met. It provides detailed reporting
and exit codes suitable for CI/CD integration.

Usage:
    python scripts/run_coverage.py [OPTIONS]

Options:
    --threshold PERCENT    Minimum coverage percentage (default: 95)
    --html                 Generate HTML coverage report
    --xml                  Generate XML coverage report for CI tools
    --verbose              Show detailed coverage output
    --module MODULE        Specific module to test (default: hazelcast)
    --strict               Fail on any coverage warnings

Exit Codes:
    0 - Success, coverage threshold met
    1 - Tests failed
    2 - Coverage below threshold
    3 - Configuration or runtime error
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_THRESHOLD = 95
DEFAULT_MODULE = "hazelcast"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run pytest with coverage validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Minimum coverage percentage (default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        help="Generate HTML coverage report in htmlcov/",
    )
    parser.add_argument(
        "--xml",
        action="store_true",
        help="Generate XML coverage report (coverage.xml)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed coverage output with missing lines",
    )
    parser.add_argument(
        "--module",
        default=DEFAULT_MODULE,
        help=f"Module to measure coverage for (default: {DEFAULT_MODULE})",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on coverage warnings",
    )
    parser.add_argument(
        "--tests",
        default="tests/",
        help="Test directory or file pattern (default: tests/)",
    )
    parser.add_argument(
        "--integration",
        action="store_true",
        help="Include integration tests",
    )
    parser.add_argument(
        "--parallel", "-n",
        type=int,
        default=0,
        help="Number of parallel workers (0 = auto, -1 = disabled)",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only generate report from existing .coverage file",
    )
    return parser.parse_args()


def build_pytest_command(args: argparse.Namespace) -> list:
    """Build the pytest command with coverage options."""
    cmd = [
        sys.executable, "-m", "pytest",
        f"--cov={args.module}",
        f"--cov-fail-under={args.threshold}",
    ]

    if args.verbose:
        cmd.append("--cov-report=term-missing")
    else:
        cmd.append("--cov-report=term")

    if args.html:
        cmd.append("--cov-report=html:htmlcov")

    if args.xml:
        cmd.append("--cov-report=xml:coverage.xml")

    if args.strict:
        cmd.append("--cov-config=.coveragerc")

    if args.parallel > 0:
        cmd.extend(["-n", str(args.parallel)])
    elif args.parallel == 0:
        cmd.extend(["-n", "auto"])

    if not args.integration:
        cmd.append("-m")
        cmd.append("not integration")

    cmd.append(args.tests)

    return cmd


def build_report_command(args: argparse.Namespace) -> list:
    """Build coverage report command."""
    cmd = [sys.executable, "-m", "coverage", "report"]

    if args.verbose:
        cmd.append("--show-missing")

    cmd.extend([f"--fail-under={args.threshold}"])

    if args.html:
        subprocess.run(
            [sys.executable, "-m", "coverage", "html", "-d", "htmlcov"],
            cwd=PROJECT_ROOT,
        )

    if args.xml:
        subprocess.run(
            [sys.executable, "-m", "coverage", "xml", "-o", "coverage.xml"],
            cwd=PROJECT_ROOT,
        )

    return cmd


def run_coverage(args: argparse.Namespace) -> int:
    """Run coverage measurement and validation.

    Returns:
        Exit code (0=success, 1=test failure, 2=coverage failure, 3=error)
    """
    os.chdir(PROJECT_ROOT)

    if args.report_only:
        cmd = build_report_command(args)
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        return 2 if result.returncode != 0 else 0

    cmd = build_pytest_command(args)

    print("=" * 70)
    print("HAZELCAST PYTHON CLIENT - COVERAGE VALIDATION")
    print("=" * 70)
    print(f"Module:    {args.module}")
    print(f"Threshold: {args.threshold}%")
    print(f"Tests:     {args.tests}")
    print(f"Command:   {' '.join(cmd)}")
    print("=" * 70)
    print()

    result = subprocess.run(cmd, cwd=PROJECT_ROOT)

    print()
    print("=" * 70)

    if result.returncode == 0:
        print(f"SUCCESS: Coverage meets or exceeds {args.threshold}% threshold")
        print("=" * 70)
        return 0
    elif result.returncode == 1:
        print("FAILURE: Tests failed")
        print("=" * 70)
        return 1
    elif result.returncode == 2:
        print(f"FAILURE: Coverage below {args.threshold}% threshold")
        print("=" * 70)
        return 2
    else:
        print(f"ERROR: Unexpected exit code {result.returncode}")
        print("=" * 70)
        return 3


def print_coverage_summary() -> None:
    """Print a summary of coverage by module."""
    print()
    print("Coverage by Module:")
    print("-" * 50)

    modules = [
        ("hazelcast.config", "Configuration"),
        ("hazelcast.exceptions", "Exceptions"),
        ("hazelcast.auth", "Authentication"),
        ("hazelcast.metrics", "Metrics"),
        ("hazelcast.failover", "Failover"),
        ("hazelcast.logging_config", "Logging"),
        ("hazelcast.near_cache", "Near Cache"),
        ("hazelcast.serialization", "Serialization"),
        ("hazelcast.service", "Services"),
        ("hazelcast.network", "Network"),
    ]

    for module, description in modules:
        print(f"  {description:<20} {module}")

    print("-" * 50)
    print()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    try:
        import pytest
        import coverage
    except ImportError as e:
        print(f"ERROR: Required package not installed: {e}")
        print("Install with: pip install pytest pytest-cov coverage")
        return 3

    if args.verbose:
        print_coverage_summary()

    return run_coverage(args)


if __name__ == "__main__":
    sys.exit(main())
