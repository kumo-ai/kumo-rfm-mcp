"""Utility functions for KumoRFM MCP server."""

import os
from pathlib import Path


def get_project_root() -> Path:
    """Get the project root directory by searching for pyproject.toml.

    This is more robust than hardcoded parent navigation as it works
    regardless of where the module is located in the project structure.
    """
    current = Path(__file__).resolve()

    # Walk up the directory tree looking for pyproject.toml
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    # Fallback to environment variable or current working directory
    if "PROJECT_ROOT" in os.environ:
        return Path(os.environ["PROJECT_ROOT"])

    return Path.cwd()


def get_docs_path() -> Path:
    """Get the documentation directory path."""
    return get_project_root() / "docs"


def get_examples_path() -> Path:
    """Get the examples directory path."""
    return get_project_root() / "examples"
