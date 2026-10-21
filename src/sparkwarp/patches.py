"""One reversible patch of one class attribute, the base of every sparkwarp shim."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar


class Absent:
    """Marks a class attribute that did not exist before the patch."""


ABSENT = Absent()


class ClassPatch(ABC):
    """Replaces `TARGET.ATTRIBUTE` and puts back exactly what was there before.

    Reads the class's own `__dict__`, so an attribute the class did not define itself
    is deleted on uninstall rather than copied down from a base class or __getattr__.
    """

    TARGET: ClassVar[type]
    ATTRIBUTE: ClassVar[str]

    def __init__(self) -> None:
        self._previous: object = ABSENT
        self._installed = False

    @property
    def installed(self) -> bool:
        return self._installed

    @property
    def previous(self) -> object:
        return self._previous

    @property
    def original(self) -> object:
        """What the class holds without this patch: the saved or the current value."""
        return (
            self._previous if self._installed else vars(self.TARGET).get(self.ATTRIBUTE)
        )

    @property
    def applies(self) -> bool:
        """Whether this PySpark defines the attribute; most patches always apply."""
        return True

    def install(self) -> None:
        if self._installed:
            return
        self._previous = vars(self.TARGET).get(self.ATTRIBUTE, ABSENT)
        setattr(self.TARGET, self.ATTRIBUTE, self._replacement(self._previous))
        self._installed = True

    def uninstall(self) -> None:
        if not self._installed:
            return
        if self._previous is ABSENT:
            delattr(self.TARGET, self.ATTRIBUTE)
        else:
            setattr(self.TARGET, self.ATTRIBUTE, self._previous)
        self._previous = ABSENT
        self._installed = False

    @abstractmethod
    def _replacement(self, previous: object) -> object:
        """The object that stands in for the previous attribute."""
