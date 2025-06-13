"""Pytest integration tests for Tecton MCP server."""

import pytest
from pathlib import Path
from .runner import IntegrationTestRunner
from .test_case import TestCase

class TestIntegration:
    """Integration test class for pytest."""
    
    @pytest.fixture(scope="class")
    def test_runner(self):
        """Create test runner fixture."""
        return IntegrationTestRunner()
    
    @pytest.fixture(scope="class") 
    def test_cases(self, test_runner):
        """Discover test cases fixture."""
        return test_runner.discover_test_cases()
    
    def test_basic_feature_view(self, test_runner):
        """Test basic feature view creation."""
        test_cases_dir = Path(__file__).parent / "test_cases"
        basic_test_dir = test_cases_dir / "basic_feature_view"
        
        if not basic_test_dir.exists():
            pytest.skip("Basic feature view test case not found")
        
        test_case = TestCase(basic_test_dir)
        result = test_runner.run_test_case(test_case)
        
        assert result.success, f"Test failed: {result.error_message}"
        assert result.score >= test_case.config.evaluation_threshold, f"Score {result.score} below threshold {test_case.config.evaluation_threshold}"
        assert result.tecton_plan_passed, f"Tecton plan failed: {result.tecton_plan_output}"
    
    @pytest.mark.parametrize("test_case", pytest.lazy_fixture("test_cases"))
    def test_all_cases(self, test_runner, test_case):
        """Run all discovered test cases."""
        result = test_runner.run_test_case(test_case)
        
        assert result.success, f"Test {test_case.name} failed: {result.error_message}"
        assert result.score >= test_case.config.evaluation_threshold, f"Score {result.score} below threshold"
        
        if test_case.config.tecton_plan_should_pass:
            assert result.tecton_plan_passed, f"Tecton plan failed: {result.tecton_plan_output}"