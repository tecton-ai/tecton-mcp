"""
Shared utilities for working with remote Git repositories.

This module provides common functionality for cloning and managing remote
repositories, used by both generate_examples_parquet.py and generate_embeddings.py.
"""

import os
import tempfile
from pathlib import Path
from typing import Optional, Dict
from enum import Enum
from dataclasses import dataclass

from git import Repo

# Global in-memory cache to prevent duplicate clones per script run
_CLONED_REPOS: Dict[str, Path] = {}


class DirectoryType(Enum):
    """Types of directories for categorizing content."""
    RIFT = "rift"
    SPARK = "spark"
    DOCS = "docs"


@dataclass
class DirectoryConfig:
    """Configuration for a code directory that should always be fetched from a remote Git repository.

    A fresh shallow clone is performed every run – no local fallback logic.
    """

    directory_type: DirectoryType
    remote_url: str
    sub_dir: str = ""  # relative path inside the repo

    # internal cache for the current script invocation only
    _resolved: Optional[str] = None

    def resolve_path(self) -> Optional[str]:
        """Resolve the path to the directory by cloning the remote repository if needed."""
        if self._resolved is not None:
            return self._resolved

        try:
            if self.remote_url in _CLONED_REPOS:
                repo_path = _CLONED_REPOS[self.remote_url]
            else:
                repo_name = Path(self.remote_url).stem
                clone_base = Path(tempfile.mkdtemp(prefix="tecton_clone_"))
                repo_path = clone_base / repo_name

                print(f"Cloning {self.remote_url} into {repo_path} (depth=1, filter=blob:none)…")
                Repo.clone_from(
                    self.remote_url,
                    repo_path,
                    depth=1,
                    multi_options=["--filter=blob:none"],
                )
                _CLONED_REPOS[self.remote_url] = repo_path

            candidate = repo_path / self.sub_dir
            if not candidate.exists():
                print(f"Warning: Sub-directory {candidate} not found in cloned repo.")
                return None

            self._resolved = str(candidate)
            return self._resolved

        except Exception as e:
            print(f"Error cloning {self.remote_url}: {e}")
            return None


def clear_repo_cache():
    """Clear the repository cache. Useful for testing or cleanup."""
    global _CLONED_REPOS
    _CLONED_REPOS.clear()