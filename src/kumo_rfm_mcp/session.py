"""Session management for KumoRFM MCP server."""

import logging
import os
from typing import Dict, Optional, Any
from pydantic.v1 import BaseModel, ConfigDict
import kumoai as kumo
from kumoai.experimental.rfm import KumoRFM, LocalGraph, LocalTable

logger = logging.getLogger("kumo-rfm-mcp.session")


class SessionData(BaseModel):
    """Pydantic model for session data with validation."""
    
    # session tracking
    session_id: str = "default"
    initialized: bool = False
    
    # kumo objects
    graph: LocalGraph = LocalGraph() # TODO(blaz): currently can't initialize empty graph
    rfm: KumoRFM = KumoRFM()

    # credentials
    api_url: Optional[str] = None
    api_key: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'session_id': self.session_id,
            'initialized': self.initialized,
            'api_url': self.api_url,
            'api_key': self.api_key[:8] + "..." if self.api_key else None,
        }


class SessionState:
    """Manages multiple KumoRFM sessions."""
    
    _session_data: SessionData = SessionData(session_id="default")
    
    @classmethod
    def get_session(cls) -> SessionData:
        """TODO(blaz): we can support multiple sessions in the future"""
        return cls._session_data
    
    @classmethod
    def initialize(cls) -> None:
        """Auto-initialize default session from environment variables."""
        api_key = os.getenv("KUMO_API_KEY")
        api_url = os.getenv("KUMO_API_URL")
        if not api_url:
            api_url = "https://kumorfm.ai/api"
            logger.warning("KUMO_API_URL not set, "
                           "using default url: https://kumorfm.ai/api")
        
        session = cls.get_session()
        
        if api_key:
            try:
                session.api_key = api_key
                session.api_url = api_url
                session.initialized = True
                kumo.init(api_key=api_key, api_url=api_url)
                logger.info("KumoRFM auto-initialized from environment")
            except Exception as e:
                logger.error(f"Auto-initialization failed: {e}")
                session.initialized = False
        else:
            logger.error("KUMO_API_KEY not set - "
                         "server running without authentication")
            session.initialized = False
    