from unittest.mock import patch

import pytest

from kumo_rfm_mcp.session import Session, SessionManager


class TestSession:
    """Test Session class."""

    def test_creation(self):
        """Test session creation."""
        session = Session("test")
        assert session.name == "test"
        assert not session.initialized
        assert session.model is None

    def test_repr(self):
        """Test string representation."""
        session = Session("test")
        assert repr(session) == "Session(name=test)"

    def test_initialize_success(self, mock_kumo_api, valid_api_key):
        """Test successful initialization."""
        session = Session("test")
        result = session.initialize()

        assert result is session
        assert session.initialized
        mock_kumo_api.init.assert_called_once()

    def test_initialize_missing_api_key(self, mock_kumo_api, clean_env):
        """Test initialization fails without API key."""
        session = Session("test")

        with pytest.raises(ValueError,
                           match="Missing required environment variable "
                           "KUMO_API_KEY"):
            session.initialize()

        assert not session.initialized
        mock_kumo_api.init.assert_not_called()

    def test_initialize_idempotent(self, mock_kumo_api, valid_api_key):
        """Test initialization is idempotent."""
        session = Session("test")

        session.initialize()
        session.initialize()  # Second call

        mock_kumo_api.init.assert_called_once()

    def test_initialize_kumo_error(self, mock_kumo_api, valid_api_key):
        """Test initialization handles kumo errors."""
        session = Session("test")
        mock_kumo_api.init.side_effect = Exception("Kumo error")

        with pytest.raises(Exception, match="Kumo error"):
            session.initialize()

        assert not session.initialized


class TestSessionManager:
    """Test SessionManager class."""

    def test_get_default_session(self, mock_kumo_api, valid_api_key,
                                 reset_session_manager):
        """Test getting default session."""
        session = SessionManager.get_default_session()

        assert isinstance(session, Session)
        assert session.name == "default"
        assert session.initialized

    def test_singleton_behavior(self, mock_kumo_api, valid_api_key,
                                reset_session_manager):
        """Test singleton behavior."""
        session1 = SessionManager.get_default_session()
        session2 = SessionManager.get_default_session()

        assert session1 is session2

    def test_initializes_on_first_call(self, mock_kumo_api, valid_api_key,
                                       reset_session_manager):
        """Test session is initialized on first call."""
        with patch.object(
                SessionManager._default,
                'initialize',
                wraps=SessionManager._default.initialize) as mock_init:
            SessionManager.get_default_session()
            mock_init.assert_called_once()
