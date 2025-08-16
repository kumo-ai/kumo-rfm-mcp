import logging
import os
from dataclasses import dataclass
from typing import Optional

from kumoai.experimental import rfm
from typing_extensions import Self

logger = logging.getLogger('kumo-rfm-mcp.session')


@dataclass(init=False, repr=False)
class Session:
    name: str
    initialized: bool = False

    graph: rfm.LocalGraph = rfm.LocalGraph(tables=[])
    model: Optional[rfm.KumoRFM] = None

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name={self.name})'

    def initialize(self) -> Self:
        r"""Initialize a session from environment variables."""
        if not self.initialized:
            logger.info(f"Initializing KumoRFM session: {self.name}")

            # Check for required API key
            api_key = os.getenv('KUMO_API_KEY')
            if not api_key:
                error_msg = (
                    "Missing required environment variable KUMO_API_KEY. "
                    "Please set your KumoAI API key: "
                    "export KUMO_API_KEY='your-api-key'")
                logger.error(error_msg)
                raise ValueError(error_msg)

            rfm.init()
            self.initialized = True
            logger.info("KumoAI session initialized successfully")

        return self


class SessionManager:
    _default: Session = Session(name='default')

    @classmethod
    def get_default_session(cls) -> Session:
        r"""Returns the default session."""
        return cls._default.initialize()
