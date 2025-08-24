import logging
import os
from dataclasses import dataclass, field

from kumoai.experimental import rfm
from typing_extensions import Self

logger = logging.getLogger('kumo-rfm-mcp.session')


@dataclass(init=False, repr=False)
class Session:
    name: str
    initialized: bool = False
    graph: rfm.LocalGraph = field(default_factory=lambda: rfm.LocalGraph([]))
    model: rfm.KumoRFM | None = None

    def __init__(self, name: str) -> None:
        self.name = name
        self.initialized = False
        self.graph = rfm.LocalGraph([])
        self.model = None

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name={self.name})'

    def initialize(self) -> Self:
        r"""Initialize a session from environment variables."""
        if not self.initialized:
            logger.info(f"Initializing KumoRFM session: {self.name}")

            api_key = os.getenv('KUMO_API_KEY')
            if not api_key:
                raise ValueError("Missing required environment variable "
                                 "'KUMO_API_KEY'. Please set your API key via "
                                 "`export KUMO_API_KEY='your-api-key'`.")

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
