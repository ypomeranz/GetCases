"""Small Qt worker helpers for running blocking work off the UI thread."""

from __future__ import annotations

import inspect
import traceback
from typing import Any, Callable

from PySide6.QtCore import QObject, QRunnable, Signal, Slot


class WorkerSignals(QObject):
    finished = Signal(object)
    error = Signal(str)
    status = Signal(str)


class Worker(QRunnable):
    """Run a callable on Qt's thread pool and emit result/error signals."""

    def __init__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @Slot()
    def run(self) -> None:
        try:
            kwargs = dict(self.kwargs)
            try:
                accepts_status = "status" in inspect.signature(self.fn).parameters
            except (TypeError, ValueError):
                accepts_status = False
            if accepts_status and "status" not in kwargs:
                kwargs["status"] = self.signals.status.emit
            result = self.fn(*self.args, **kwargs)
        except Exception:
            self.signals.error.emit(traceback.format_exc())
            return
        self.signals.finished.emit(result)
