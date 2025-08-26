import os
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def mock_kumo_api():
    """Mock kumoai.experimental.rfm module."""
    with patch('kumo_rfm_mcp.session.rfm') as mock_rfm:
        mock_rfm.init.return_value = None
        mock_rfm.LocalGraph.return_value = Mock()
        mock_rfm.KumoRFM.return_value = Mock()
        yield mock_rfm


@pytest.fixture
def clean_env():
    """Clean environment for testing."""
    with patch.dict(os.environ, {}, clear=True):
        yield


@pytest.fixture
def valid_api_key():
    """Environment with valid API key."""
    with patch.dict(os.environ, {'KUMO_API_KEY': 'test-key'}):
        yield


@pytest.fixture
def reset_session_manager():
    """Reset SessionManager state between tests."""
    from kumo_rfm_mcp.session import SessionManager
    original_default = SessionManager._default
    yield
    SessionManager._default = original_default
