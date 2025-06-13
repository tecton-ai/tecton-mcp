"""Test case definition and utilities."""

import os
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional

@dataclass
class TestCaseConfig:
    """Configuration for a single test case."""
    name: str
    timeout: int = 900  # 15 minutes default
    evaluation_threshold: int = 70
    tecton_plan_should_pass: bool = True
    skip: bool = False
    expected_prompt_response: str = ""
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TestCaseConfig':
        """Create TestCaseConfig from dictionary."""
        return cls(**data)

class TestCase:
    """Represents a single integration test case."""
    
    def __init__(self, case_dir: Path):
        self.case_dir = case_dir
        self.name = case_dir.name
        
        # Load configuration
        config_path = case_dir / "config.yaml"
        if config_path.exists():
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                # Add the name to the config data since it's required
                config_data['name'] = self.name
                self.config = TestCaseConfig.from_dict(config_data)
        else:
            self.config = TestCaseConfig(
                name=self.name
            )
    
    @property
    def initial_repo_dir(self) -> Path:
        """Path to initial repository state."""
        return self.case_dir / "initial_repo"
    
    @property
    def expected_repo_dir(self) -> Path:
        """Path to expected repository state."""
        return self.case_dir / "expected_repo"
    
    @property
    def prompt_file(self) -> Path:
        """Path to prompt file."""
        return self.case_dir / "prompt.txt"
    
    def has_initial_repo(self) -> bool:
        """Check if test case has initial repository."""
        return self.initial_repo_dir.exists() and any(self.initial_repo_dir.iterdir())
    
    def has_expected_repo(self) -> bool:
        """Check if test case has expected repository."""
        return self.expected_repo_dir.exists() and any(self.expected_repo_dir.iterdir())
    
    def get_prompt(self) -> str:
        """Get the prompt for this test case."""
        if self.prompt_file.exists():
            return self.prompt_file.read_text().strip()
        return ""
    
    def validate(self) -> list[str]:
        """Validate test case structure and return any errors."""
        errors = []
        
        if not self.prompt_file.exists():
            errors.append(f"Missing prompt.txt in {self.case_dir}")
        
        # For simple tests (with expected_prompt_response), we don't need expected_repo
        # For integration tests, we need expected_repo
        if self.config.expected_prompt_response:
            # This is a simple prompt/response test - no expected repo needed
            pass
        else:
            # This is an integration test - expected repo is required
            if not self.has_expected_repo():
                errors.append(f"Missing or empty expected_repo in {self.case_dir} (required for integration tests)")
        
        return errors