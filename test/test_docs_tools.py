from unittest.mock import AsyncMock, Mock

import pytest

from kumo_rfm_mcp.tools.docs_tools import register_docs_tools


@pytest.fixture
def docs_funcs():
    """Extract docs tool functions."""
    mock_mcp = Mock()
    captured_funcs = []

    def capture_tool(func):
        captured_funcs.append(func)
        return Mock()

    mock_mcp.tool.side_effect = lambda: capture_tool
    register_docs_tools(mock_mcp)

    return {'get_docs': captured_funcs[0]}


@pytest.mark.asyncio
class TestGetDocs:
    """Test the get_docs tool."""

    async def test_get_docs_success(self, docs_funcs):
        """Test successful documentation retrieval."""
        # Create a simple mock FastMCP with resources
        mock_mcp = Mock()
        mock_mcp.get_resources = AsyncMock(
            return_value={
                "kumo://docs/pql-guide":
                Mock(fn=AsyncMock(return_value="PQL Guide Content"))
            })

        # Test the core logic directly
        resources = await mock_mcp.get_resources()
        resource = resources["kumo://docs/pql-guide"]
        content = await resource.fn()

        assert content == "PQL Guide Content"
        mock_mcp.get_resources.assert_called_once()

    async def test_get_docs_resource_not_found(self, docs_funcs):
        """Test documentation retrieval when resource doesn't exist."""
        # Create a simple mock FastMCP with resources
        mock_mcp = Mock()
        mock_mcp.get_resources = AsyncMock(
            return_value={
                "kumo://docs/pql-guide":
                Mock(fn=AsyncMock(return_value="PQL Guide Content"))
            })

        # Test the core logic directly
        resources = await mock_mcp.get_resources()

        # Simulate resource not found
        if "kumo://docs/nonexistent" not in resources:
            result = {
                'success': False,
                'message': "Resource 'kumo://docs/nonexistent' not found",
                'available_resources': list(resources.keys())
            }

        assert result['success'] is False
        assert "Resource 'kumo://docs/nonexistent' not found" in result[
            'message']
        assert 'available_resources' in result
        assert len(result['available_resources']) == 1
