"""
Benchmark adapter bridge.

Imports TeamAdapter from the project source so the benchmark harness
can resolve it via --adapter adapters.team:TeamAdapter.
"""
from __future__ import annotations

import sys
import os

# Add project root to sys.path so `src.team_adapter` is importable.
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, os.path.abspath(_PROJECT_ROOT))

from src.team_adapter import TeamAdapter  # noqa: F401
