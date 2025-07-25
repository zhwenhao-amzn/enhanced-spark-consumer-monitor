"""
Test runner script for the Spark Checkpoint Monitor project.

This script provides a unified way to run all tests with different configurations
and generate comprehensive test reports.
"""

import unittest
import sys
import os
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any
import subprocess

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'plugins'))

# Import test modules
from test_s3_checkpoint_reader import TestS3CheckpointReader
from test_kafka_offset_committer import TestKafkaOffsetCommitter, TestMultiConsumerGroupKafkaCommitter
from test_config_manager import TestConfigManager
from test_integration import TestS3Integration, TestKafkaIntegration, TestConfigurationIntegration, TestEndToEndIntegration


class TestRunner:
    """Manages test execution and reporting."""
    
    def __init__(self):
        """Initialize test runner."""
        self.test_results = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'unit_tests': {},
            'integration_tests': {},
            'validation_results': {},
            'errors': []
        }
    
    def run_unit_tests(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Run all unit tests.
        
        Args:
            verbose: Enable verbose output
            
        Returns:
            Dictionary containing unit test results
        """
        print("Running unit tests...")
        
        # Define unit test classes
        unit_test_classes = [
            TestS3CheckpointReader,
            TestKafkaOffsetCommitter,
            TestMultiConsumerGroupKafkaCommitter,
            TestConfigManager
        ]
        
        unit_results = {}
        total_tests = 0
        total_failures = 0
        total_errors = 0
        
        for test_class in unit_test_classes:
            class_name = test_class.__name__
            print(f"  Running {class_name}...")
            
            # Create test suite
            suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
            
            # Run tests with custom result handler
            result = unittest.TestResult()
            suite.run(result)
            
            # Collect results
            class_result = {
                'tests_run': result.testsRun,
                'failures': len(result.failures),
                'errors': len(result.errors),
                'success_rate': (result.testsRun - len(result.failures) - len(result.errors)) / max(result.testsRun, 1),
                'failure_details': [str(failure[1]) for failure in result.failures],
                'error_details': [str(error[1]) for error in result.errors]
            }
            
            unit_results[class_name] = class_result
            total_tests += result.testsRun
            total_failures += len(result.failures)
            total_errors += len(result.errors)
            
            if verbose:
                if result.failures:
                    print(f"    Failures: {len(result.failures)}")
                    for failure in result.failures:
                        print(f"      - {failure[0]}: {failure[1].split(chr(10))[0]}")
                
                if result.errors:
                    print(f"    Errors: {len(result.errors)}")
                    for error in result.errors:
                        print(f"      - {error[0]}: {error[1].split(chr(10))[0]}")
            
            print(f"    ✓ {class_result['tests_run']} tests, {class_result['failures']} failures, {class_result['errors']} errors")
        
        # Summary
        unit_summary = {
            'total_tests': total_tests,
            'total_failures': total_failures,
            'total_errors': total_errors,
            'success_rate': (total_tests - total_failures - total_errors) / max(total_tests, 1),
            'status': 'PASSED' if total_failures == 0 and total_errors == 0 else 'FAILED'
        }
        
        print(f"Unit tests complete: {unit_summary['status']} ({total_tests - total_failures - total_errors}/{total_tests} passed)")
        
        return {
            'summary': unit_summary,
            'details': unit_results
        }
    
    def run_integration_tests(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Run integration tests (if enabled).
        
        Args:
            verbose: Enable verbose output
            
        Returns:
            Dictionary containing integration test results
        """
        print("Running integration tests...")
        
        # Check if integration tests are enabled
        s3_enabled = os.getenv('RUN_S3_INTEGRATION_TESTS', '').lower() == 'true'
        kafka_enabled = os.getenv('RUN_KAFKA_INTEGRATION_TESTS', '').lower() == 'true'
        e2e_enabled = os.getenv('RUN_E2E_INTEGRATION_TESTS', '').lower() == 'true'
        
        if not any([s3_enabled, kafka_enabled, e2e_enabled]):
            print("  Integration tests disabled. Set environment variables to enable:")
            print("    RUN_S3_INTEGRATION_TESTS=true")
            print("    RUN_KAFKA_INTEGRATION_TESTS=true")
            print("    RUN_E2E_INTEGRATION_TESTS=true")
            
            return {
                'summary': {
                    'status': 'SKIPPED',
                    'reason': 'Integration tests disabled'
                },
                'details': {}
            }
        
        # Define integration test classes
        integration_test_classes = []
        if s3_enabled:
            integration_test_classes.append(TestS3Integration)
        if kafka_enabled:
            integration_test_classes.append(TestKafkaIntegration)
        if any([s3_enabled, kafka_enabled, e2e_enabled]):
            integration_test_classes.extend([TestConfigurationIntegration, TestEndToEndIntegration])
        
        integration_results = {}
        total_tests = 0
        total_failures = 0
        total_errors = 0
        total_skipped = 0
        
        for test_class in integration_test_classes:
            class_name = test_class.__name__
            print(f"  Running {class_name}...")
            
            try:
                # Create test suite
                suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
                
                # Run tests
                result = unittest.TestResult()
                suite.run(result)
                
                # Count skipped tests
                skipped_count = result.testsRun - len(result.failures) - len(result.errors)
                if hasattr(result, 'skipped'):
                    skipped_count = len(result.skipped)
                
                # Collect results
                class_result = {
                    'tests_run': result.testsRun,
                    'failures': len(result.failures),
                    'errors': len(result.errors),
                    'skipped': skipped_count,
                    'success_rate': (result.testsRun - len(result.failures) - len(result.errors)) / max(result.testsRun, 1),
                    'failure_details': [str(failure[1]) for failure in result.failures],
                    'error_details': [str(error[1]) for error in result.errors]
                }
                
                integration_results[class_name] = class_result
                total_tests += result.testsRun
                total_failures += len(result.failures)
                total_errors += len(result.errors)
                total_skipped += skipped_count
                
                print(f"    ✓ {class_result['tests_run']} tests, {class_result['failures']} failures, {class_result['errors']} errors")
                
            except Exception as e:
                integration_results[class_name] = {
                    'error': str(e),
                    'status': 'ERROR'
                }
                print(f"    ✗ Error running {class_name}: {e}")
        
        # Summary
        integration_summary = {
            'total_tests': total_tests,
            'total_failures': total_failures,
            'total_errors': total_errors,
            'total_skipped': total_skipped,
            'success_rate': (total_tests - total_failures - total_errors) / max(total_tests, 1) if total_tests > 0 else 0,
            'status': 'PASSED' if total_failures == 0 and total_errors == 0 and total_tests > 0 else 'FAILED' if total_tests > 0 else 'SKIPPED'
        }
        
        print(f"Integration tests complete: {integration_summary['status']}")
        
        return {
            'summary': integration_summary,
            'details': integration_results
        }
    
    def run_validation(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Run system validation.
        
        Args:
            verbose: Enable verbose output
            
        Returns:
            Dictionary containing validation results
        """
        print("Running system validation...")
        
        try:
            # Import and run validation
            from validate_offsets import OffsetValidator
            
            validator = OffsetValidator()
            validation_results = validator.run_comprehensive_validation()
            
            status = validation_results.get('overall_status', 'unknown').upper()
            print(f"System validation complete: {status}")
            
            if verbose and status != 'HEALTHY':
                print("  Issues detected:")
                for error in validation_results.get('s3_validation', {}).get('errors', []):
                    print(f"    S3: {error}")
                for error in validation_results.get('kafka_validation', {}).get('errors', []):
                    print(f"    Kafka: {error}")
                for error in validation_results.get('consistency_validation', {}).get('errors', []):
                    print(f"    Consistency: {error}")
            
            return validation_results
            
        except Exception as e:
            print(f"  ✗ Validation failed: {e}")
            return {
                'overall_status': 'error',
                'error': str(e)
            }
    
    def generate_test_data(self) -> bool:
        """
        Generate test data for testing.
        
        Returns:
            True if successful, False otherwise
        """
        print("Generating test data...")
        
        try:
            from test_data_generator import main as generate_data
            generate_data()
            print("  ✓ Test data generated successfully")
            return True
        except Exception as e:
            print(f"  ✗ Failed to generate test data: {e}")
            return False
    
    def run_all_tests(self, include_integration: bool = False, include_validation: bool = False,
                     generate_data: bool = False, verbose: bool = False) -> Dict[str, Any]:
        """
        Run all tests and generate comprehensive report.
        
        Args:
            include_integration: Include integration tests
            include_validation: Include system validation
            generate_data: Generate test data before running tests
            verbose: Enable verbose output
            
        Returns:
            Dictionary containing all test results
        """
        print("=== Spark Checkpoint Monitor Test Suite ===\n")
        
        # Generate test data if requested
        if generate_data:
            self.generate_test_data()
            print()
        
        # Run unit tests
        unit_results = self.run_unit_tests(verbose=verbose)
        self.test_results['unit_tests'] = unit_results
        print()
        
        # Run integration tests if requested
        if include_integration:
            integration_results = self.run_integration_tests(verbose=verbose)
            self.test_results['integration_tests'] = integration_results
            print()
        
        # Run validation if requested
        if include_validation:
            validation_results = self.run_validation(verbose=verbose)
            self.test_results['validation_results'] = validation_results
            print()
        
        # Generate overall summary
        overall_status = 'PASSED'
        
        if unit_results['summary']['status'] != 'PASSED':
            overall_status = 'FAILED'
        
        if include_integration and self.test_results.get('integration_tests', {}).get('summary', {}).get('status') == 'FAILED':
            overall_status = 'FAILED'
        
        if include_validation and self.test_results.get('validation_results', {}).get('overall_status') not in ['healthy', 'HEALTHY']:
            overall_status = 'FAILED'
        
        self.test_results['summary'] = {
            'overall_status': overall_status,
            'unit_tests_status': unit_results['summary']['status'],
            'integration_tests_status': self.test_results.get('integration_tests', {}).get('summary', {}).get('status', 'SKIPPED'),
            'validation_status': self.test_results.get('validation_results', {}).get('overall_status', 'SKIPPED'),
            'total_unit_tests': unit_results['summary']['total_tests'],
            'unit_test_success_rate': unit_results['summary']['success_rate']
        }
        
        print(f"=== TEST SUMMARY ===")
        print(f"Overall Status: {overall_status}")
        print(f"Unit Tests: {unit_results['summary']['status']} ({unit_results['summary']['total_tests']} tests)")
        
        if include_integration:
            integration_summary = self.test_results['integration_tests']['summary']
            print(f"Integration Tests: {integration_summary['status']} ({integration_summary.get('total_tests', 0)} tests)")
        
        if include_validation:
            validation_status = self.test_results['validation_results'].get('overall_status', 'unknown')
            print(f"System Validation: {validation_status.upper()}")
        
        return self.test_results
    
    def save_results(self, filename: str = None) -> str:
        """
        Save test results to file.
        
        Args:
            filename: Output filename (defaults to timestamped filename)
            
        Returns:
            Path to saved file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'test_results_{timestamp}.json'
        
        filepath = os.path.join(os.path.dirname(__file__), filename)
        
        with open(filepath, 'w') as f:
            json.dump(self.test_results, f, indent=2, default=str)
        
        return filepath


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run Spark Checkpoint Monitor tests')
    parser.add_argument('--integration', action='store_true', help='Include integration tests')
    parser.add_argument('--validation', action='store_true', help='Include system validation')
    parser.add_argument('--generate-data', action='store_true', help='Generate test data before running tests')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    parser.add_argument('--output', '-o', help='Output file for test results')
    parser.add_argument('--unit-only', action='store_true', help='Run only unit tests')
    
    args = parser.parse_args()
    
    # Create test runner
    runner = TestRunner()
    
    # Run tests
    if args.unit_only:
        results = runner.run_unit_tests(verbose=args.verbose)
        runner.test_results['unit_tests'] = results
        runner.test_results['summary'] = {'overall_status': results['summary']['status']}
    else:
        results = runner.run_all_tests(
            include_integration=args.integration,
            include_validation=args.validation,
            generate_data=args.generate_data,
            verbose=args.verbose
        )
    
    # Save results
    output_file = runner.save_results(args.output)
    print(f"\nTest results saved to: {output_file}")
    
    # Exit with appropriate code
    overall_status = runner.test_results['summary']['overall_status']
    if overall_status == 'PASSED':
        print("\n✓ All tests passed!")
        exit(0)
    else:
        print(f"\n✗ Tests completed with status: {overall_status}")
        exit(1)


if __name__ == '__main__':
    main()
