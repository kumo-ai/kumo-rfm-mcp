import logging
import os
from dataclasses import dataclass, field

from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm
from typing_extensions import Self

from .data_models import TableSource

logger = logging.getLogger('kumo-rfm-mcp.session')


@dataclass(init=False, repr=False)
class Session:
    name: str
    _initialized: bool = False
    _graph: rfm.LocalGraph = field(default_factory=lambda: rfm.LocalGraph([]))
    _model: rfm.KumoRFM | None = None

    def __init__(self, name: str) -> None:
        self.name = name
        self._is_initialized = False
        self._graph = rfm.LocalGraph([])
        self._model = None

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    @property
    def graph(self) -> rfm.LocalGraph:
        return self._graph

    @property
    def model(self) -> rfm.KumoRFM | None:
        if self._model is None:
            return self._model
        self.initialize()
        return self._model

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name={self.name})'

    def initialize(self) -> Self:
        r"""Initialize a session from environment variables."""
        if not self.is_initialized:
            logger.info(f"Initializing '{self.name}' KumoRFM session")

            api_key = os.getenv('KUMO_API_KEY')
            if not api_key:
                raise ToolError("Missing required environment variable "
                                "'KUMO_API_KEY'. Please set your API key via "
                                "`export KUMO_API_KEY='your-api-key'`.")

            rfm.init()
            self._is_initialized = True

        return self


class SessionManager:
    _default: Session = Session(name='default')

    @classmethod
    def get_default_session(cls) -> Session:
        r"""Returns the default session."""
        return cls._default
