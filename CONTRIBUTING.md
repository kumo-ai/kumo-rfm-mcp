# Contributing to KumoRFM MCP

Thank you for contributing! This guide covers development setup and the build/publish process.

## Development Setup

### Prerequisites

- Python 3.10+
- Git

### Setup

```bash
git clone https://github.com/kumo-ai/kumo-rfm-mcp.git
cd kumo-rfm-mcp
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
```

### Testing

```bash
# Run MCP server
python -m kumo_rfm_mcp.server

# Test with client
python scripts/test_mcp_client.py
```

See [`CLAUDE_DESKTOP_SETUP.md`](./CLAUDE_DESKTOP_SETUP.md) for Claude Desktop integration.

## Building and Publishing

### Local Build

```bash
pip install -U pip build twine
python -m build
twine check dist/*
```

### Publishing Process

**Automatic builds** happen on every push to `main` and PR.

**Publishing** only happens for releases.
1. To release `vX.{Y+1}.0rc0`, run:
```bash
gh workflow run release.yml --ref main -f dry-run=false
```
1. To release `vX.Y.0rc{N+1}`, run:
```bash
gh workflow run release.yml --ref vX.Y -f dry-run=false
```
1. To release `vX.Y.0`, run:
```bash
gh workflow run release.yml --ref vX.Y -f switch-from-rc-to-final=true -f dry-run=false
```
1. To release `vX.Y.{Z+1}`, run:
```bash
gh workflow run release.yml --ref vX.Y -f dry-run=false
```

For manual publishing, use the "Publish Package" workflow in the Actions tab.

### Required Secrets

- `TEST_PYPI_TOKEN`: For testing
- `PYPI_TOKEN`: For production releases
