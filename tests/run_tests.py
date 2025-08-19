#!/usr/bin/env python3
"""
Test runner script for market_data_etl package.

This script provides convenient ways to run different test suites:
- Unit tests only (fast)
- Integration tests only (slower) 
- All tests
- Specific test categories

Usage:
    python tests/run_tests.py                    # Run all tests
    python tests/run_tests.py --unit            # Run only unit tests
    python tests/run_tests.py --integration     # Run only integration tests
    python tests/run_tests.py --file test_config.py  # Run specific test file
    python tests/run_tests.py --coverage        # Run with coverage report
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    if description:
        print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"\n✅ {description or 'Command'} completed successfully")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description or 'Command'} failed with exit code {e.returncode}")
        return e.returncode
    except FileNotFoundError:
        print(f"\n❌ pytest not found. Please install: pip install pytest")
        return 1


def get_project_root():
    """Get the project root directory."""
    current_file = Path(__file__)
    # Go up from tests/run_tests.py to project root
    return current_file.parent.parent


def main():
    parser = argparse.ArgumentParser(
        description="Test runner for market_data_etl package",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tests/run_tests.py                    # Run all tests
  python tests/run_tests.py --unit            # Run only unit tests  
  python tests/run_tests.py --integration     # Run only integration tests
  python tests/run_tests.py --slow            # Run slow tests
  python tests/run_tests.py --file test_config.py  # Run specific file
  python tests/run_tests.py --coverage        # Run with coverage
  python tests/run_tests.py --verbose         # Extra verbose output
        """
    )
    
    # Test selection options
    test_group = parser.add_mutually_exclusive_group()
    test_group.add_argument(
        '--unit', 
        action='store_true',
        help='Run only unit tests (fast, no external dependencies)'
    )
    test_group.add_argument(
        '--integration',
        action='store_true', 
        help='Run only integration tests (slower, may use database)'
    )
    test_group.add_argument(
        '--slow',
        action='store_true',
        help='Run only slow tests'
    )
    test_group.add_argument(
        '--external-api',
        action='store_true',
        help='Run only tests requiring external APIs'
    )
    test_group.add_argument(
        '--file',
        type=str,
        help='Run specific test file (e.g., test_config.py)'
    )
    
    # Output options
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run tests with coverage report'
    )
    parser.add_argument(
        '--verbose',
        action='store_true', 
        help='Extra verbose output'
    )
    parser.add_argument(
        '--no-warnings',
        action='store_true',
        help='Disable warnings'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Run tests in parallel (requires pytest-xdist)'
    )
    
    args = parser.parse_args()
    
    # Change to project root directory
    project_root = get_project_root()
    os.chdir(project_root)
    print(f"Running tests from: {project_root}")
    
    # Build pytest command
    cmd = ['python', '-m', 'pytest']
    
    # Add test selection
    if args.unit:
        cmd.extend(['-m', 'unit'])
        description = "unit tests"
    elif args.integration:
        cmd.extend(['-m', 'integration'])
        description = "integration tests"
    elif args.slow:
        cmd.extend(['-m', 'slow'])
        description = "slow tests"
    elif args.external_api:
        cmd.extend(['-m', 'external_api'])
        description = "external API tests"
    elif args.file:
        test_file = f"tests/{args.file}" if not args.file.startswith('tests/') else args.file
        cmd.append(test_file)
        description = f"tests from {args.file}"
    else:
        description = "all tests"
    
    # Add coverage
    if args.coverage:
        cmd.extend([
            '--cov=market_data_etl',
            '--cov-report=html:htmlcov',
            '--cov-report=term-missing'
        ])
        description += " with coverage"
    
    # Add verbosity
    if args.verbose:
        cmd.append('-vvv')
    
    # Add warning control
    if args.no_warnings:
        cmd.append('--disable-warnings')
    
    # Add parallel execution
    if args.parallel:
        cmd.extend(['-n', 'auto'])
    
    # Run the tests
    exit_code = run_command(cmd, f"Running {description}")
    
    # Print coverage report location if coverage was run
    if args.coverage and exit_code == 0:
        coverage_dir = project_root / 'htmlcov'
        if coverage_dir.exists():
            print(f"\n📊 Coverage report generated at: {coverage_dir / 'index.html'}")
    
    # Print summary
    if exit_code == 0:
        print(f"\n🎉 All {description} passed!")
    else:
        print(f"\n💥 Some {description} failed.")
        print("\nTo run specific failing tests:")
        print("  python tests/run_tests.py --file <test_file.py>")
        print("\nTo run with more verbosity:")
        print("  python tests/run_tests.py --verbose")
    
    return exit_code


if __name__ == '__main__':
    sys.exit(main())