#!/usr/bin/env python3
"""
Test runner script for video pipeline.

This script provides a unified way to run all tests or specific test categories.
"""

import os
import sys
import argparse
import unittest
import pytest
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_runner.log')
    ]
)
logger = logging.getLogger('test_runner')

# Ensure the project root is in the path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))


def run_unittest_tests(test_pattern=None, verbose=False):
    """
    Run tests using the unittest framework.
    
    Args:
        test_pattern: Optional pattern to filter tests
        verbose: Whether to show verbose output
    
    Returns:
        True if all tests pass, False otherwise
    """
    logger.info(f"Running unittest tests{f' matching {test_pattern}' if test_pattern else ''}")
    
    # Discover and run tests
    loader = unittest.TestLoader()
    
    if test_pattern:
        suite = loader.loadTestsFromName(test_pattern)
    else:
        start_dir = os.path.join(project_root, 'tests')
        suite = loader.discover(start_dir, pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
    result = runner.run(suite)
    
    # Return True if all tests pass
    return result.wasSuccessful()


def run_pytest_tests(test_pattern=None, verbose=False):
    """
    Run tests using pytest.
    
    Args:
        test_pattern: Optional pattern to filter tests
        verbose: Whether to show verbose output
    
    Returns:
        True if all tests pass, False otherwise
    """
    logger.info(f"Running pytest tests{f' matching {test_pattern}' if test_pattern else ''}")
    
    # Build pytest arguments
    pytest_args = []
    
    if verbose:
        pytest_args.append('-v')
    
    if test_pattern:
        pytest_args.append(test_pattern)
    else:
        pytest_args.append(os.path.join(project_root, 'tests'))
    
    # Run pytest
    result = pytest.main(pytest_args)
    
    # Return True if all tests pass (pytest.ExitCode.OK == 0)
    return result == 0


def run_specific_test_category(category, verbose=False):
    """
    Run tests for a specific category.
    
    Args:
        category: Test category to run
        verbose: Whether to show verbose output
    
    Returns:
        True if all tests pass, False otherwise
    """
    logger.info(f"Running {category} tests")
    
    # Map categories to test patterns
    category_patterns = {
        'scrapers': 'tests.test_scrapers',
        'scrapers_comprehensive': 'tests.test_scrapers_comprehensive',
        'validators': 'tests.test_validators',
        'storage': 'tests.test_storage_and_processing',
        'integration': 'tests.test_integration',
        'pixabay': 'tests.test_pixabay_integration'
    }
    
    if category not in category_patterns:
        logger.error(f"Unknown test category: {category}")
        return False
    
    # Run tests for the specified category
    return run_unittest_tests(category_patterns[category], verbose)


def setup_test_environment():
    """
    Set up the test environment with necessary environment variables and directories.
    """
    logger.info("Setting up test environment")
    
    # Create test directories if they don't exist
    test_dirs = ['logs', 'temp', 'output']
    for directory in test_dirs:
        os.makedirs(os.path.join(project_root, directory), exist_ok=True)
    
    # Set up mock environment variables for testing if not already set
    test_env_vars = {
        'PEXELS_API_KEY': 'test_pexels_key',
        'VIDEVO_API_KEY': 'test_videvo_key',
        'NASA_API_KEY': 'test_nasa_key',
        'IA_ACCESS_KEY': 'test_ia_key',
        'IA_SECRET_KEY': 'test_ia_secret',
        'NOAA_API_TOKEN': 'test_noaa_token',
        'PIXABAY_API_KEY': 'test_pixabay_key',
        'AWS_ACCESS_KEY_ID': 'test_aws_key',
        'AWS_SECRET_ACCESS_KEY': 'test_aws_secret'
    }
    
    for var_name, default_value in test_env_vars.items():
        if var_name not in os.environ:
            os.environ[var_name] = default_value
            logger.info(f"Set test environment variable: {var_name}")


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(description='Run video pipeline tests')
    
    # Add arguments
    parser.add_argument('--category', '-c', choices=[
        'all', 'scrapers', 'scrapers_comprehensive', 'validators', 
        'storage', 'integration', 'pixabay'
    ], default='all', help='Test category to run')
    
    parser.add_argument('--framework', '-f', choices=['unittest', 'pytest'], 
                      default='unittest', help='Test framework to use')
    
    parser.add_argument('--pattern', '-p', help='Test pattern to match')
    
    parser.add_argument('--verbose', '-v', action='store_true', 
                      help='Show verbose output')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up test environment
    setup_test_environment()
    
    # Run tests based on arguments
    if args.category != 'all':
        success = run_specific_test_category(args.category, args.verbose)
    elif args.pattern:
        if args.framework == 'unittest':
            success = run_unittest_tests(args.pattern, args.verbose)
        else:
            success = run_pytest_tests(args.pattern, args.verbose)
    else:
        if args.framework == 'unittest':
            success = run_unittest_tests(verbose=args.verbose)
        else:
            success = run_pytest_tests(verbose=args.verbose)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
