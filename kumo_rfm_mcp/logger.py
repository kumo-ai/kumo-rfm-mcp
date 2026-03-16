import time
from typing import Any

from kumoai.utils.progress_logger import PlainProgressLogger
from typing_extensions import Self


class MCPProgressLogger(PlainProgressLogger):
    """A progress logger safe for MCP stdio transport.

    The base :class:`ProgressLogger` writes OSC escape sequences to
    ``sys.stdout`` in ``__enter__``/``__exit__``, which corrupts the
    JSON-RPC stream when running over stdio transport. This subclass
    overrides the context manager to preserve timing and log collection
    while skipping all stdout writes.
    """
    def __enter__(self) -> Self:
        self._depth += 1
        if self._depth == 1:
            self.start_time = time.perf_counter()
        # Skip on_enter (verbose=False) and skip stdout escape sequences.
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._depth -= 1
        if self._depth == 0:
            self.end_time = time.perf_counter()
        # Skip on_exit (verbose=False) and skip stdout escape sequences.
