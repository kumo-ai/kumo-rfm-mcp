from unittest.mock import Mock, patch

import pytest

from kumo_rfm_mcp.tools.graph_tools import register_graph_tools


@pytest.fixture
def graph_funcs():
    """Extract graph tool functions."""

    mock_mcp = Mock()
    captured_funcs = []

    def capture_tool(func):
        captured_funcs.append(func)
        return Mock()

    mock_mcp.tool.side_effect = lambda: capture_tool
    register_graph_tools(mock_mcp)

    return {
        'infer_links': captured_funcs[0],
        'inspect_graph': captured_funcs[1],
        'link_tables': captured_funcs[2],
        'unlink_tables': captured_funcs[3]
    }


@pytest.mark.asyncio
class TestInferLinks:
    """Test the infer_links tool."""

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_infer_links_success(self, mock_get_session, graph_funcs):
        """Test successful link inference."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock existing edges and new edges
        mock_session.graph.edges = [
            Mock(src_table='users', fkey='id', dst_table='orders')
        ]
        mock_session.graph.infer_links = Mock()

        # Mock the edge difference calculation
        original_edges = {('users', 'id', 'orders')}
        new_edges = {('orders', 'user_id', 'users')}
        mock_session.graph.edges = list(original_edges) + list(new_edges)

        result = await graph_funcs['infer_links']()

        assert result['success'] is True
        assert "Link inference completed" in result['message']
        assert 'inferred_links' in result['data']
        mock_session.graph.infer_links.assert_called_once_with(verbose=False)

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_infer_links_no_new_links(self, mock_get_session,
                                            graph_funcs):
        """Test link inference when no new links are found."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock existing edges
        mock_session.graph.edges = [
            Mock(src_table='users', fkey='id', dst_table='orders')
        ]
        mock_session.graph.infer_links = Mock()

        # No new edges after inference
        result = await graph_funcs['infer_links']()

        assert result['success'] is True
        assert "Link inference completed" in result['message']
        assert len(result['data']['inferred_links']) == 0

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_infer_links_error(self, mock_get_session, graph_funcs):
        """Test link inference error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock edges to be iterable
        mock_session.graph.edges = []
        mock_session.graph.infer_links.side_effect = Exception("Graph error")

        result = await graph_funcs['infer_links']()

        assert result['success'] is False
        assert "Failed to infer links" in result['message']
        assert "Graph error" in result['message']


@pytest.mark.asyncio
class TestInspectGraph:
    """Test the inspect_graph tool."""

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_inspect_graph_success(self, mock_get_session, graph_funcs):
        """Test successful graph inspection."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock tables
        mock_users = Mock()
        mock_users.name = 'users'
        mock_users._data = Mock(__len__=Mock(return_value=100),
                                columns=['user_id', 'name', 'age'])
        mock_users._primary_key = 'user_id'
        mock_users._time_column = 'dob'

        mock_orders = Mock()
        mock_orders.name = 'orders'
        mock_orders._data = Mock(__len__=Mock(return_value=200),
                                 columns=['order_id', 'user_id', 'date'])
        mock_orders._primary_key = 'order_id'
        mock_orders._time_column = 'date'

        mock_session.graph.tables = {
            'users': mock_users,
            'orders': mock_orders
        }

        # Mock edges
        mock_edge = Mock(src_table='orders', fkey='user_id', dst_table='users')
        mock_session.graph.edges = [mock_edge]

        result = await graph_funcs['inspect_graph']()

        assert result['success'] is True
        assert "Graph structure retrieved successfully" in result['message']
        assert 'tables' in result['data']
        assert 'links' in result['data']
        assert len(result['data']['tables']) == 2
        assert len(result['data']['links']) == 1
        assert result['data']['tables']['users']['num_rows'] == 100
        assert result['data']['tables']['orders']['columns'] == [
            'order_id', 'user_id', 'date'
        ]

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_inspect_graph_empty(self, mock_get_session, graph_funcs):
        """Test graph inspection with empty graph."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.tables = {}
        mock_session.graph.edges = []

        result = await graph_funcs['inspect_graph']()

        assert result['success'] is True
        assert len(result['data']['tables']) == 0
        assert len(result['data']['links']) == 0

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_inspect_graph_error(self, mock_get_session, graph_funcs):
        """Test graph inspection error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.tables = Mock()
        mock_session.graph.tables.values.side_effect = Exception(
            "Access error")

        result = await graph_funcs['inspect_graph']()

        assert result['success'] is False
        assert "Failed to inspect graph" in result['message']


@pytest.mark.asyncio
class TestLinkTables:
    """Test the link_tables tool."""

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_link_tables_success(self, mock_get_session, graph_funcs):
        """Test successful table linking."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.link = Mock()

        result = await graph_funcs['link_tables']('orders', 'user_id', 'users')

        assert result['success'] is True
        assert ("Successfully linked 'orders' and 'users' by 'user_id'"
                in result['message'])
        mock_session.graph.link.assert_called_once_with(
            'orders', 'user_id', 'users')

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_link_tables_error(self, mock_get_session, graph_funcs):
        """Test table linking error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.link.side_effect = Exception("Invalid link")

        result = await graph_funcs['link_tables']('orders', 'invalid_key',
                                                  'users')

        assert result['success'] is False
        assert ("Failed to link 'orders' and 'users' by 'invalid_key'"
                in result['message'])
        assert "Invalid link" in result['message']


@pytest.mark.asyncio
class TestUnlinkTables:
    """Test the unlink_tables tool."""

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_unlink_tables_success(self, mock_get_session, graph_funcs):
        """Test successful table unlinking."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.unlink = Mock()

        result = await graph_funcs['unlink_tables']('orders', 'user_id',
                                                    'users')

        assert result['success'] is True
        assert ("Successfully unlinked 'orders' and 'users' by 'user_id'"
                in result['message'])
        mock_session.graph.unlink.assert_called_once_with(
            'orders', 'user_id', 'users')

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_unlink_tables_error(self, mock_get_session, graph_funcs):
        """Test table unlinking error handling."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.unlink.side_effect = Exception("Link not found")

        result = await graph_funcs['unlink_tables']('orders', 'user_id',
                                                    'users')

        assert result['success'] is False
        assert "Failed to unlink 'orders' and 'users' by 'user_id'" in result[
            'message']
        assert "Link not found" in result['message']


@pytest.mark.asyncio
class TestGraphToolsEdgeCases:
    """Test edge cases for graph tools."""

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_infer_links_with_complex_edges(self, mock_get_session,
                                                  graph_funcs):
        """Test link inference with complex edge structures."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock complex edge structure
        mock_edge1 = Mock(src_table='orders',
                          fkey='user_id',
                          dst_table='users')
        mock_edge2 = Mock(src_table='order_items',
                          fkey='order_id',
                          dst_table='orders')
        mock_edge3 = Mock(src_table='order_items',
                          fkey='item_id',
                          dst_table='items')

        mock_session.graph.edges = [mock_edge1, mock_edge2, mock_edge3]
        mock_session.graph.infer_links = Mock()

        result = await graph_funcs['infer_links']()

        assert result['success'] is True
        mock_session.graph.infer_links.assert_called_once_with(verbose=False)

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_inspect_graph_with_time_columns(self, mock_get_session,
                                                   graph_funcs):
        """Test graph inspection with various time column configurations."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock table with no time column
        mock_table = Mock()
        mock_table.name = 'static_table'
        mock_table._data = Mock(__len__=Mock(return_value=50),
                                columns=['id', 'name'])
        mock_table._primary_key = 'id'
        mock_table._time_column = None

        mock_session.graph.tables = {'static_table': mock_table}
        mock_session.graph.edges = []

        result = await graph_funcs['inspect_graph']()

        assert result['success'] is True
        assert result['data']['tables']['static_table']['time_column'] is None

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_link_tables_same_table(self, mock_get_session, graph_funcs):
        """Test linking a table to itself (should fail)."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        mock_session.graph.link.side_effect = Exception(
            "Cannot link table to itself")

        result = await graph_funcs['link_tables']('users', 'id', 'users')

        assert result['success'] is False
        assert "Failed to link 'users' and 'users' by 'id'" in result[
            'message']

    @patch('kumo_rfm_mcp.tools.graph_tools.SessionManager.get_default_session')
    async def test_link_primary_key_to_primary_key(self, mock_get_session,
                                                   graph_funcs):
        """Linking two primary keys across two
        copies of the same table should fail."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Two logical copies of the same table schema
        users = Mock()
        users.name = 'users'
        users._primary_key = 'user_id'
        users._time_column = None
        users._data = Mock(__len__=Mock(return_value=10),
                           columns=['user_id', 'name'])

        users_clone = Mock()
        users_clone.name = 'users_clone'
        users_clone._primary_key = 'user_id'
        users_clone._time_column = None
        users_clone._data = Mock(__len__=Mock(return_value=10),
                                 columns=['user_id', 'name'])

        mock_session.graph.tables = {
            'users': users,
            'users_clone': users_clone,
        }

        # Backend rejects PK->PK links
        mock_session.graph.link.side_effect = Exception(
            "Cannot link two primary keys")

        result = await graph_funcs['link_tables']('users', 'user_id',
                                                  'users_clone')

        assert result['success'] is False
        assert ("Failed to link 'users' and 'users_clone' by 'user_id'"
                in result['message'])
