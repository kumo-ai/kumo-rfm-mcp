#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
SYS_VENV = ROOT / 'sys_venv'
UV_VENV = ROOT / 'uv_venv'
REQUIREMENTS = ROOT / 'requirements.txt'


def get_venv() -> Path:
    """Create two virtual environments for KumoRFM MCP.
    1. A venv that inherits system Python version and installs `uv`.
    2. A `uv` venv with Python 3.11.
    """
    if os.name == 'nt':
        sys_python = SYS_VENV / 'Scripts' / 'python.exe'
        uv_python = UV_VENV / 'Scripts' / 'python.exe'
    else:
        sys_python = SYS_VENV / 'bin' / 'python'
        uv_python = UV_VENV / 'bin' / 'python'

    if not SYS_VENV.exists():
        subprocess.check_call(
            [sys.executable, '-m', 'venv', SYS_VENV],
            stdout=subprocess.DEVNULL,
        )

    if not UV_VENV.exists():
        subprocess.check_call(
            [str(sys_python), '-m', 'pip', 'install', 'uv'],
            stdout=subprocess.DEVNULL,
        )
        subprocess.check_call(
            [
                str(sys_python), '-m', 'uv', 'venv', '--python', '3.11',
                str(UV_VENV)
            ],
            stdout=subprocess.DEVNULL,
        )

    subprocess.check_call(
        [
            str(sys_python), '-m', 'uv', 'pip', 'install', '-r',
            str(REQUIREMENTS), '--python',
            str(uv_python)
        ],
        stdout=subprocess.DEVNULL,
    )

    return uv_python


if __name__ == '__main__':
    python = get_venv()
    sys.exit(subprocess.call([str(python), '-m', 'kumo_rfm_mcp.server']))
