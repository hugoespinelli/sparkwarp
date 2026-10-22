"""Runs the JVM server and stops it when the pytest run that owns it is gone.

Started as `python -m sparkwarp.jvm_watchdog <owner pid> <command...>` in its own
process group, so the owner stops both with one signal; if the owner dies without
stopping them (a crash, SIGKILL), the watchdog notices and stops the server itself.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import FrameType

from sparkwarp.config import (
    JVM_STOP_TIMEOUT_SECONDS,
    PROC_STATUS,
    WATCHDOG_POLL_SECONDS,
    ZOMBIE_STATE,
)


class Lifeline:
    """Keeps the server alive exactly as long as its owner process."""

    def __init__(self, owner: int, command: list[str]) -> None:
        self._owner = owner
        self._command = command
        self._child: subprocess.Popen[bytes] | None = None

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self._on_terminate)
        self._child = subprocess.Popen(self._command)
        while self._child.poll() is None:
            if not self._owner_alive():
                self._stop_child()
                break
            time.sleep(WATCHDOG_POLL_SECONDS)
        return self._child.returncode or 0

    def _owner_alive(self) -> bool:
        """The owner exists and is no zombie; another user's process is a reused pid."""
        try:
            os.kill(self._owner, 0)
        except (ProcessLookupError, PermissionError):
            return False
        status = Path(PROC_STATUS.format(pid=self._owner))
        return not (status.exists() and ZOMBIE_STATE in status.read_text())

    def _on_terminate(self, signum: int, frame: FrameType | None) -> None:
        self._stop_child()
        sys.exit(0)

    def _stop_child(self) -> None:
        child = self._child
        if child is None or child.poll() is not None:
            return
        child.terminate()
        try:
            child.wait(JVM_STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            child.kill()


if __name__ == "__main__":
    sys.exit(Lifeline(int(sys.argv[1]), sys.argv[2:]).run())
