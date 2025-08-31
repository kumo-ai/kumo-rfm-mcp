#!/usr/bin/env python3
import importlib
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
VENV = ROOT / 'venv'
REQUIREMENTS = ROOT / "requirements.txt"


def get_venv() -> Path:
    venv_exists = VENV.exists()

    if os.name == "nt":
        python = VENV / "Scripts" / "python.exe"
    else:
        python = VENV / "bin" / "python"

    if not venv_exists:
        subprocess.check_call([str(python), "-m", "pip", "install", "uv"])
        subprocess.check_call(
            [str(python), "-m", "uv", "venv", "--python 3.11",
             str(VENV)])

    if not venv_exists:
        subprocess.check_call(
            [str(python), "-m", "pip", "install", "-r", REQUIREMENTS])

    return python


if __name__ == "__main__":
    python = get_venv()
    print("PYTHON PATH", python)
    install_mcp(python)

    from fastmcp import FastMCP

    mcp = FastMCP(
        name='KumoRFM (Relational Foundation Model)',
        instructions=(
            "KumoRFM is a pre-trained Relational Foundation Model (RFM) "
            "that generates training-free predictions on any relational "
            "multi-table data by interpreting the data as a (temporal) "
            "heterogeneous graph. It can be queried via the Predictive "
            "Query Language (PQL)."),
        version='0.1.0',
    )
    mcp.run(transport='stdio')
    # Launch your MCP server inside the venv
    # os.execv(str(python), [str(python), str(ROOT / "main.py")])
