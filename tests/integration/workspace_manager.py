"""Tecton workspace management for integration tests."""

import subprocess
import uuid
import os
import shutil
from pathlib import Path
from typing import Optional
from .config import TEST_CONFIG

class TectonWorkspaceManager:
    """Manages Tecton workspaces for integration tests."""
    
    def __init__(self):
        self.workspace_name: Optional[str] = None
        self.repo_dir: Optional[Path] = None
    
    def create_workspace(self, test_case_name: Optional[str] = None) -> str:
        """Create a new Tecton development workspace."""
        # Generate unique workspace name
        workspace_id = str(uuid.uuid4())[:8]
        if test_case_name:
            # Sanitize test case name for workspace naming (replace spaces and special chars with underscores)
            sanitized_name = "".join(c if c.isalnum() else "_" for c in test_case_name).lower()
            self.workspace_name = f"{TEST_CONFIG.tecton_workspace_prefix}_{sanitized_name}_{workspace_id}"
        else:
            self.workspace_name = f"{TEST_CONFIG.tecton_workspace_prefix}_{workspace_id}"
        
        # Create the workspace
        cmd = [
            "tecton", "workspace", "create", 
            self.workspace_name
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create workspace: {result.stderr}")
        
        return self.workspace_name
    
    def setup_repo(self, test_case_name: str, initial_repo_dir: Optional[Path] = None) -> Path:
        """Setup repository for testing by copying initial_repo to output directory."""
        # Create output directory structure
        output_base_dir = Path(TEST_CONFIG.test_root) / "output"
        output_base_dir.mkdir(exist_ok=True)
        
        # Create test-specific output directory
        test_output_dir = output_base_dir / test_case_name
        if test_output_dir.exists():
            shutil.rmtree(test_output_dir)
        test_output_dir.mkdir(parents=True)
        
        # Copy initial repo content to output directory
        if initial_repo_dir and initial_repo_dir.exists() and any(initial_repo_dir.iterdir()):
            # Copy all contents from initial_repo to test output directory
            for item in initial_repo_dir.iterdir():
                if item.is_dir():
                    shutil.copytree(item, test_output_dir / item.name)
                else:
                    shutil.copy2(item, test_output_dir)
        else:
            # Create a minimal tecton repository if no initial repo provided
            subprocess.run(["tecton", "init"], cwd=test_output_dir, check=True)
        
        self.repo_dir = test_output_dir
        
        # Apply Tecton configuration
        self._apply_tecton_config()
        
        return self.repo_dir

    
    def _apply_tecton_config(self):
        """Apply Tecton configuration using tecton apply --yes."""
        if not self.workspace_name or not self.repo_dir:
            raise RuntimeError("Workspace or repo not initialized")
        
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Running 'tecton apply --yes' in directory: {self.repo_dir}")
        
        # Change to repo directory and apply configuration
        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(self.repo_dir)
            
            # Apply the configuration with workspace set via environment variable
            env = os.environ.copy()
            env["TECTON_WORKSPACE"] = self.workspace_name
            cmd = ["tecton", "apply", "--yes"]
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to apply Tecton configuration: {result.stderr}")
        finally:
            os.chdir(original_cwd)
    
    def validate_tecton_plan(self) -> tuple[bool, str]:
        """Run tecton plan and return success status and output."""
        if not self.repo_dir:
            raise RuntimeError("Repository not initialized")
        
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Running 'tecton plan' in directory: {self.repo_dir}")
        
        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(self.repo_dir)
            
            env = os.environ.copy()
            env["TECTON_WORKSPACE"] = self.workspace_name
            cmd = ["tecton", "plan"]
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            # Tecton plan should succeed (return code 0) but may have warnings
            success = result.returncode == 0
            output = result.stdout + result.stderr
            
            return success, output
            
        finally:
            os.chdir(original_cwd)
    
    def cleanup(self):
        """Clean up workspace."""
        # Delete workspace
        if self.workspace_name:
            try:
                cmd = ["tecton", "workspace", "delete", self.workspace_name, "--yes"]
                subprocess.run(cmd, capture_output=True, text=True)
            except Exception:
                # Best effort cleanup - don't fail the test if workspace deletion fails
                pass
            finally:
                self.workspace_name = None
        
        # Clean up output directory if it was created (optional - for debugging, we might want to keep it)
        # Note: Leaving output directories for debugging purposes
        
        # Clear repo reference
        self.repo_dir = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()