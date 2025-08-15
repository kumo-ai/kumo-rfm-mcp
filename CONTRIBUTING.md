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
pip install build
python -m build
./scripts/validate_wheels_for_release.sh
```

### Publishing Process

**Automatic builds** happen on every push to `main` and PR.

**Publishing** only happens for releases:

1. **Update version** in `pyproject.toml`
1. **Create and push a version tag:**
   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```
1. **GitHub Actions automatically publishes to PyPI**

For manual publishing, use the "Publish Package" workflow in the Actions tab.

### Required Secrets

- `TEST_PYPI_TOKEN`: For testing
- `PYPI_TOKEN`: For production releases
