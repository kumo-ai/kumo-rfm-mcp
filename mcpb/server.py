#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
VENV = ROOT / 'venv'
REQUIREMENTS = ROOT / 'requirements.txt'


def get_venv() -> Path:
    """Create a virtual environment for KumoRFM MCP via `uv`."""
    venv_exists = VENV.exists()

    if os.name == 'nt':
        python = VENV / 'Scripts' / 'python.exe'
    else:
        python = VENV / 'bin' / 'python'

    if not venv_exists:
        subprocess.check_call(
            [sys.executable, '-m', 'pip', 'install', 'uv'],
            stdout=subprocess.DEVNULL,
        )
        subprocess.check_call(
            [
                sys.executable, '-m', 'uv', 'venv', '--python', '3.11',
                str(VENV)
            ],
            stdout=subprocess.DEVNULL,
        )

    subprocess.check_call(
        [
            sys.executable, '-m', 'uv', 'pip', 'install', '-r',
            str(REQUIREMENTS), '--python', python
        ],
        stdout=subprocess.DEVNULL,
    )

    return python


if __name__ == '__main__':
    python = get_venv()
    os.execv(str(python), [str(python), '-m', 'kumo_rfm_mcp.server'])
