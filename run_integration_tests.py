#!/usr/bin/env python3
"""CLI script to run integration tests."""

import sys
from pathlib import Path

# Add the src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Add tests directory to Python path  
tests_path = Path(__file__).parent / "tests"
sys.path.insert(0, str(tests_path))

from tests.integration.runner import main

if __name__ == "__main__":
    main()