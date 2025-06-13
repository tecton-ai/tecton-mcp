"""Main test runner for integration tests."""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass

from .config import TEST_CONFIG
from .test_case import TestCase
from .workspace_manager import TectonWorkspaceManager
from .claude_executor import ClaudeCodeExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    """Result of a single test execution."""
    test_name: str
    success: bool
    score: int
    execution_time: float
    error_message: str = ""
    evaluation_explanation: str = ""
    tecton_plan_passed: bool = False
    tecton_plan_output: str = ""
    test_type: str = ""  # "simple" or "integration"

class IntegrationTestRunner:
    """Runs integration tests for the Tecton MCP server."""
    
    def __init__(self):
        self.results: List[TestResult] = []
    
    def discover_test_cases(self) -> List[TestCase]:
        """Discover all test cases in the test cases directory."""
        test_cases_dir = Path(TEST_CONFIG.test_cases_dir)
        if not test_cases_dir.exists():
            logger.error(f"Test cases directory not found: {test_cases_dir}")
            return []
        
        test_cases = []
        for case_dir in test_cases_dir.iterdir():
            if case_dir.is_dir() and not case_dir.name.startswith('.'):
                test_case = TestCase(case_dir)
                errors = test_case.validate()
                if test_case.config.skip:
                    logger.info(f"Skipping test case {case_dir.name}")
                    continue
                if errors:
                    raise ValueError(f"Invalid test case {case_dir.name}: {errors}")
                test_cases.append(test_case)
        
        logger.info(f"Discovered {len(test_cases)} test cases")
        return test_cases
    
    def determine_test_type(self, test_case: TestCase) -> str:
        """Determine if this is a simple prompt/response test or a full integration test."""
        # If expected_prompt_response is set and non-empty, it's a simple test
        if test_case.config.expected_prompt_response:
            return "simple"
        # If it has expected repo, it's a full integration test
        elif test_case.has_expected_repo():
            return "integration"
        else:
            # Default to simple if neither condition is met
            return "simple"
    
    def run_simple_test(self, test_case: TestCase) -> TestResult:
        """Run a simple prompt/response test."""
        import time
        start_time = time.time()
        
        logger.info(f"Running simple test case: {test_case.name}")
        
        try:
            # Create a temporary directory for Claude execution
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Execute the prompt with Claude Code
                claude_executor = ClaudeCodeExecutor(temp_path)
                prompt = test_case.get_prompt()
                
                logger.info(f"Executing prompt: {prompt[:100]}...")
                success, stdout, stderr = claude_executor.execute_prompt(prompt)
                
                if not success:
                    execution_time = time.time() - start_time
                    return TestResult(
                        test_name=test_case.name,
                        success=False,
                        score=0,
                        execution_time=execution_time,
                        error_message=f"Claude Code execution failed: {stderr}",
                        test_type="simple"
                    )
                
                # Check if the response matches the expected response
                expected_response = test_case.config.expected_prompt_response.strip()
                actual_response = stdout.strip()
                
                # Simple string matching - you could make this more sophisticated
                response_matches = expected_response.lower() in actual_response.lower()
                
                execution_time = time.time() - start_time
                
                if response_matches:
                    logger.info(f"Response matches expected: '{expected_response}' found in '{actual_response}'")
                    return TestResult(
                        test_name=test_case.name,
                        success=True,
                        score=100,
                        execution_time=execution_time,
                        evaluation_explanation=f"Expected response '{expected_response}' found in actual response",
                        test_type="simple"
                    )
                else:
                    logger.info(f"Response does not match. Expected: '{expected_response}', Got: '{actual_response}'")
                    return TestResult(
                        test_name=test_case.name,
                        success=False,
                        score=0,
                        execution_time=execution_time,
                        error_message=f"Response mismatch. Expected: '{expected_response}', Got: '{actual_response}'",
                        test_type="simple"
                    )
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Simple test case {test_case.name} failed with exception: {e}")
            return TestResult(
                test_name=test_case.name,
                success=False,
                score=0,
                execution_time=execution_time,
                error_message=str(e),
                test_type="simple"
            )
    
    def run_integration_test(self, test_case: TestCase) -> TestResult:
        """Run a full integration test."""
        import time
        start_time = time.time()
        
        logger.info(f"Running integration test case: {test_case.name}")
        
        try:
            with TectonWorkspaceManager() as workspace_manager:
                # Create workspace and setup repository
                workspace_name = workspace_manager.create_workspace(test_case.name)
                logger.info(f"Created workspace: {workspace_name}")
                
                repo_dir = workspace_manager.setup_repo(
                    test_case.name,
                    test_case.initial_repo_dir if test_case.has_initial_repo() else None
                )
                logger.info(f"Setup repository at: {repo_dir}")
                
                # Execute the prompt with Claude Code
                claude_executor = ClaudeCodeExecutor(repo_dir)
                prompt = test_case.get_prompt()
                
                logger.info(f"Executing prompt: {prompt[:100]}...")
                success, stdout, stderr = claude_executor.execute_prompt(prompt)
                
                if not success:
                    execution_time = time.time() - start_time
                    return TestResult(
                        test_name=test_case.name,
                        success=False,
                        score=0,
                        execution_time=execution_time,
                        error_message=f"Claude Code execution failed: {stderr}",
                        test_type="integration"
                    )
                
                # Validate Tecton plan
                tecton_plan_passed, tecton_plan_output = workspace_manager.validate_tecton_plan()
                logger.info(f"Tecton plan validation: {'PASSED' if tecton_plan_passed else 'FAILED'}")
                
                # Evaluate the result
                if test_case.has_expected_repo():
                    logger.info("Evaluating result against expected repository")
                    score, explanation = claude_executor.evaluate_result(test_case.expected_repo_dir)
                else:
                    raise ValueError("No expected repository provided for integration test case: " + test_case.name)
                
                execution_time = time.time() - start_time
                test_success = (
                    success and 
                    score >= test_case.config.evaluation_threshold and
                    (tecton_plan_passed or not test_case.config.tecton_plan_should_pass)
                )
                
                return TestResult(
                    test_name=test_case.name,
                    success=test_success,
                    score=score,
                    execution_time=execution_time,
                    evaluation_explanation=explanation,
                    tecton_plan_passed=tecton_plan_passed,
                    tecton_plan_output=tecton_plan_output,
                    test_type="integration"
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Integration test case {test_case.name} failed with exception: {e}")
            return TestResult(
                test_name=test_case.name,
                success=False,
                score=0,
                execution_time=execution_time,
                error_message=str(e),
                test_type="integration"
            )
    
    def run_test_case(self, test_case: TestCase) -> TestResult:
        """Run a single test case, determining the appropriate type."""
        test_type = self.determine_test_type(test_case)
        
        if test_type == "simple":
            return self.run_simple_test(test_case)
        else:
            return self.run_integration_test(test_case)
    
    def run_all_tests(self) -> bool:
        """Run all discovered test cases."""
        test_cases = self.discover_test_cases()
        if not test_cases:
            logger.error("No test cases found")
            return False
        
        logger.info(f"Running {len(test_cases)} test cases")
        
        for test_case in test_cases:
            result = self.run_test_case(test_case)
            self.results.append(result)
            
            # Log result
            status = "PASSED" if result.success else "FAILED"
            logger.info(
                f"Test {result.test_name} ({result.test_type}): {status} "
                f"(Score: {result.score}, Time: {result.execution_time:.2f}s)"
            )
            
            if not result.success:
                logger.error(f"  Error: {result.error_message}")
            
            if result.evaluation_explanation:
                logger.info(f"  Evaluation: {result.evaluation_explanation}")
        
        return self.print_summary()
    
    def print_summary(self) -> bool:
        """Print test summary and return overall success."""
        passed = sum(1 for r in self.results if r.success)
        total = len(self.results)
        
        # Count by test type
        simple_tests = [r for r in self.results if r.test_type == "simple"]
        integration_tests = [r for r in self.results if r.test_type == "integration"]
        simple_passed = sum(1 for r in simple_tests if r.success)
        integration_passed = sum(1 for r in integration_tests if r.success)
        
        print(f"\n{'='*60}")
        print(f"INTEGRATION TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Total tests: {total}")
        print(f"  Simple tests: {len(simple_tests)} (Passed: {simple_passed})")
        print(f"  Integration tests: {len(integration_tests)} (Passed: {integration_passed})")
        print(f"Passed: {passed}")
        print(f"Failed: {total - passed}")
        print(f"Success rate: {(passed/total*100):.1f}%" if total > 0 else "N/A")
        
        if total - passed > 0:
            print(f"\nFAILED TESTS:")
            for result in self.results:
                if not result.success:
                    print(f"  - {result.test_name} ({result.test_type}): {result.error_message}")
        
        print(f"{'='*60}")
        
        return passed == total

def main():
    """Main entry point for running integration tests."""
    runner = IntegrationTestRunner()
    success = runner.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()