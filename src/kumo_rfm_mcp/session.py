from dataclasses import dataclass
from typing import Optional

from kumoai.experimental import rfm
from typing_extensions import Self


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
            # import os
            # print('api key', os.getenv('KUMO_API_KEY'))
            # logging.info(os.getenv('KUMO_API_KEY'))
            rfm.init()
            self.initialized = True
        return self


class SessionManager:
    _default: Session = Session(name='default')

    @classmethod
    def get_default_session(cls) -> Session:
        r"""Returns the default session."""
        return cls._default.initialize()
