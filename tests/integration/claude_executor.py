"""Claude Code execution for integration tests."""

import subprocess
import tempfile
from pathlib import Path
from typing import Tuple, Optional
from .config import TEST_CONFIG

class ClaudeCodeExecutor:
    """Executes Claude Code commands for integration tests."""
    
    def __init__(self, repo_dir: Path):
        self.repo_dir = repo_dir
    
    def execute_prompt(self, prompt: str) -> Tuple[bool, str, str]:
        """
        Execute a prompt using Claude Code in non-interactive mode.
        
        Returns:
            Tuple of (success, stdout, stderr)
        """
        
        try:
            # Build command using the config's claude_code_cmd and add the prompt
            cmd = TEST_CONFIG.claude_code_cmd + ["--print", prompt]
            
            import logging
            import shlex
            logger = logging.getLogger(__name__)
            # Create a shell-safe version of the command for logging (so it can be copy-pasted)
            shell_safe_cmd = ' '.join(shlex.quote(arg) for arg in cmd)
            logger.info(f"Executing claude command: {shell_safe_cmd}")
            
            # Execute Claude Code
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=TEST_CONFIG.claude_code_timeout,
                cwd=self.repo_dir
            )
            
            success = result.returncode == 0
            logger.info(f"Claude command completed with return code: {result.returncode}")
            logger.info(f"Claude stdout: {result.stdout}")
            if result.stderr:
                logger.info(f"Claude stderr: {result.stderr}")
            
            return success, result.stdout, result.stderr
            
        except subprocess.TimeoutExpired:
            return False, "", f"Claude Code execution timed out after {TEST_CONFIG.claude_code_timeout} seconds"
        except Exception as e:
            return False, "", f"Error executing Claude Code: {str(e)}"
    
    def evaluate_result(self, expected_repo_dir: Path) -> Tuple[int, str]:
        """
        Use Claude Code to evaluate how closely the result matches expectations.
        
        Returns:
            Tuple of (score, explanation)
        """
        evaluation_prompt = self._create_evaluation_prompt(expected_repo_dir)
        
        # Execute evaluation from parent directory so it can access both current and expected repos
        parent_dir = self.repo_dir.parent
        original_repo_dir = self.repo_dir
        self.repo_dir = parent_dir
        
        try:
            success, stdout, stderr = self.execute_prompt(evaluation_prompt)
        finally:
            # Restore original repo directory
            self.repo_dir = original_repo_dir
        
        if not success:
            return 0, f"Evaluation failed: {stderr}"
        
        # Parse the evaluation response
        return self._parse_evaluation_response(stdout)
    
    def _create_evaluation_prompt(self, expected_repo_dir: Path) -> str:
        """Create a prompt for evaluating the test result."""
        actual_repo_name = self.repo_dir.name
        expected_repo_name = expected_repo_dir.name
        
        return f"""
Please evaluate how closely the actual repository matches the expected repository structure and content.

The actual repository (current work) is located at: ./{actual_repo_name}/
The expected repository (target) is located at: {expected_repo_dir}

Please provide your evaluation in the following format:

SCORE: [number from 0-100]
EXPLANATION: [detailed explanation of the comparison]

Evaluation criteria:
1. Code structure and organization (25 points)
2. Functionality implementation (30 points) 
3. Tecton best practices adherence (25 points)
4. Completeness and correctness (20 points)

Please be thorough in your comparison and provide specific feedback on what matches well and what differs.
Compare the file structure, content, and implementation quality between the two directories.
"""
    
    def _parse_evaluation_response(self, response: str) -> Tuple[int, str]:
        """Parse the evaluation response to extract score and explanation."""
        lines = response.strip().split('\n')
        score = 0
        explanation = ""
        
        for line in lines:
            if line.startswith('SCORE:'):
                try:
                    score_str = line.replace('SCORE:', '').strip()
                    score = int(score_str)
                except (ValueError, IndexError):
                    score = 0
            elif line.startswith('EXPLANATION:'):
                explanation = line.replace('EXPLANATION:', '').strip()
                # Collect any additional explanation lines
                explanation_lines = [explanation]
                idx = lines.index(line) + 1
                while idx < len(lines) and not lines[idx].startswith('SCORE:'):
                    explanation_lines.append(lines[idx])
                    idx += 1
                explanation = '\n'.join(explanation_lines).strip()
        
        return score, explanation