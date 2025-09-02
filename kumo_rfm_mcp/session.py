import os
from dataclasses import dataclass, field

from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm
from typing_extensions import Self


@dataclass(init=False, repr=False)
class Session:
    name: str
    _graph: rfm.LocalGraph = field(default_factory=lambda: rfm.LocalGraph([]))
    _model: rfm.KumoRFM | None = None

    def __init__(self, name: str) -> None:
        self.name = name
        self._graph = rfm.LocalGraph([])
        self._model = None

    @property
    def is_initialized(self) -> bool:
        from kumoai import global_state
        return global_state.initialized

    @property
    def graph(self) -> rfm.LocalGraph:
        return self._graph

    @property
    def model(self) -> rfm.KumoRFM:
        if self._model is None:
            raise ToolError("Graph is not yet materialized")
        self.initialize()
        return self._model

    def clear(self) -> Self:
        """Clear the current session."""
        self._graph = rfm.LocalGraph([])
        self._model = None
        return self

    def initialize(self) -> Self:
        """Initialize a session from environment variables."""
        if not self.is_initialized:
            if os.getenv('KUMO_API_KEY') is None:
                raise ToolError("Missing required environment variable "
                                "'KUMO_API_KEY'. Please set your API key via "
                                "`export KUMO_API_KEY='your-api-key'` or "
                                "call the 'authenticate' tool to "
                                "automatically generate an API key.")

            rfm.init()

        return self

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name={self.name})'


class SessionManager:
    _default: Session = Session(name='default')

    @classmethod
    def get_default_session(cls) -> Session:
        r"""Returns the default session."""
        return cls._default
