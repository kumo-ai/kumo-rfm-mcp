import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_session():
    """Mock session with graph."""
    session = Mock()
    session.graph = Mock()
    return session


@pytest.fixture
def table_funcs():
    """Extract table tool functions."""
    from kumo_rfm_mcp.tools.table_tools import register_table_tools

    mock_mcp = Mock()
    captured_funcs = []

    def capture_tool(func):
        captured_funcs.append(func)
        return Mock()

    mock_mcp.tool.side_effect = lambda: capture_tool
    register_table_tools(mock_mcp)

    return {
        'add_table': captured_funcs[0],
        'remove_table': captured_funcs[1],
        'inspect_table': captured_funcs[2],
        'list_tables': captured_funcs[3]
    }


class TestAddTable:
    """Test add_table function."""

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_add_csv_success(self, mock_get_session, mock_session, table_funcs,
                             mock_kumo_api, valid_api_key):
        """Test successful CSV table addition."""
        mock_get_session.return_value = mock_session

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv',
                                         delete=False) as f:
            f.write("id,name\n1,test\n2,test2")
            f.flush()

            result = table_funcs['add_table'](f.name, 'test_table')

            assert result['success'] is True
            assert 'test_table' in result['message']
            mock_session.graph.add_table.assert_called_once()

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_add_table_twice(self, mock_get_session, mock_session, table_funcs,
                             mock_kumo_api, valid_api_key):
        """Test adding the same table twice fails."""
        mock_get_session.return_value = mock_session
        mock_session.graph.add_table.side_effect = [
            None, Exception("Table already exists")
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv',
                                         delete=False) as f:
            f.write("id,name\n1,test")
            f.flush()

            # First call succeeds
            result1 = table_funcs['add_table'](f.name, 'test_table')
            assert result1['success'] is True

            # Second call fails
            result2 = table_funcs['add_table'](f.name, 'test_table')
            assert result2['success'] is False
            assert "Table already exists" in result2['message']

    @patch('pandas.read_csv')
    def test_add_malformed_table(self, mock_read_csv, table_funcs):
        """Test adding malformed CSV fails gracefully."""
        mock_read_csv.side_effect = pd.errors.ParserError("Malformed CSV")

        result = table_funcs['add_table']('tests/data/malformed.csv',
                                          'bad_table')

        assert result['success'] is False
        assert "Malformed CSV" in result['message']

    def test_unsupported_format(self, table_funcs):
        """Test unsupported file format."""
        result = table_funcs['add_table']('test.txt', 'test_table')

        assert result['success'] is False
        assert "Only '*.csv' or '*.parquet' files are supported" in result[
            'message']


class TestRemoveTable:
    """Test remove_table function."""

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_remove_success(self, mock_get_session, mock_session, table_funcs,
                            mock_kumo_api, valid_api_key):
        """Test successful table removal."""
        mock_get_session.return_value = mock_session

        result = table_funcs['remove_table']('test_table')

        assert result['success'] is True
        assert 'test_table' in result['message']
        mock_session.graph.remove_table.assert_called_once_with('test_table')

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_remove_table_twice(self, mock_get_session, mock_session,
                                table_funcs, mock_kumo_api, valid_api_key):
        """Test removing the same table twice fails."""
        mock_get_session.return_value = mock_session
        mock_session.graph.remove_table.side_effect = [
            None, Exception("Table not found")
        ]

        # First removal succeeds
        result1 = table_funcs['remove_table']('test_table')
        assert result1['success'] is True

        # Second removal fails
        result2 = table_funcs['remove_table']('test_table')
        assert result2['success'] is False
        assert "Table not found" in result2['message']


class TestInspectTable:
    """Test inspect_table function."""

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_inspect_existing_table(self, mock_get_session, mock_session,
                                    table_funcs, mock_kumo_api, valid_api_key):
        """Test inspecting existing table."""
        mock_get_session.return_value = mock_session

        # Simple mock - just what the function needs
        mock_session.graph.tables = {'test_table': Mock()}
        mock_table = Mock()
        mock_table._data = Mock(__len__=Mock(return_value=100),
                                columns=['id', 'name', 'age'])
        mock_table._data.iloc = Mock()
        mock_table._data.iloc.__getitem__ = Mock(return_value=Mock(
            to_dict=Mock(return_value=[{
                'id': 1,
                'name': 'test'
            }])))
        mock_table._primary_key = 'id'
        mock_table._time_column = 'age'
        mock_session.graph.__getitem__ = Mock(return_value=mock_table)

        result = table_funcs['inspect_table']('test_table', 5)

        assert result['success'] is True
        assert 'test_table' in result['message']

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_inspect_nonexistent_table(self, mock_get_session, mock_session,
                                       table_funcs, mock_kumo_api,
                                       valid_api_key):
        """Test inspecting non-existent table."""
        mock_get_session.return_value = mock_session
        mock_session.graph.tables = {}

        result = table_funcs['inspect_table']('nonexistent_table', 5)

        assert result['success'] is False
        assert "Table with name 'nonexistent_table' not found" in result[
            'message']


class TestListTables:
    """Test list_tables function."""

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_list_empty_tables(self, mock_get_session, mock_session,
                               table_funcs, mock_kumo_api, valid_api_key):
        """Test listing tables when graph is empty."""
        mock_get_session.return_value = mock_session
        mock_session.graph.tables = {}

        result = table_funcs['list_tables']()

        assert result['success'] is True
        assert "Listed 0 tables successfully" in result['message']
        assert len(result['data']) == 0

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_list_multiple_tables(self, mock_get_session, mock_session,
                                  table_funcs, mock_kumo_api, valid_api_key):
        """Test listing multiple tables."""
        mock_get_session.return_value = mock_session

        # Simple mock - just what the function needs
        mock_session.graph.tables = {
            'users':
            Mock(name='users',
                 _data=Mock(__len__=Mock(return_value=100),
                            columns=['user_id', 'name']),
                 _primary_key='user_id',
                 _time_column='dob'),
            'orders':
            Mock(name='orders',
                 _data=Mock(__len__=Mock(return_value=200),
                            columns=['order_id', 'user_id']),
                 _primary_key='order_id',
                 _time_column=None)
        }

        result = table_funcs['list_tables']()

        assert result['success'] is True
        assert "Listed 2 tables successfully" in result['message']
        assert len(result['data']) == 2


class TestTableToolsEdgeCases:
    """Test edge cases for table tools."""

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_remove_nonexistent_table(self, mock_get_session, mock_session,
                                      table_funcs, mock_kumo_api,
                                      valid_api_key):
        """Test removing non-existent table."""
        mock_get_session.return_value = mock_session
        mock_session.graph.remove_table.side_effect = KeyError(
            "Table not found")

        result = table_funcs['remove_table']('nonexistent_table')

        assert result['success'] is False
        assert "Table not found" in result['message']

    @patch('kumo_rfm_mcp.tools.table_tools.SessionManager.get_default_session')
    def test_add_empty_csv(self, mock_get_session, mock_session, table_funcs,
                           mock_kumo_api, valid_api_key):
        """Test adding empty CSV file fails gracefully."""
        mock_get_session.return_value = mock_session

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv',
                                         delete=False) as f:
            f.write("id,name\n")  # Only header, no data
            f.flush()

            result = table_funcs['add_table'](f.name, 'empty_table')

            assert result['success'] is False
            assert "Data frame must have at least one row" in result['message']
