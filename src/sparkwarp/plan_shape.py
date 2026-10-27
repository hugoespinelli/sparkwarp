"""What a Spark Connect plan looks like to the router: its depth, and its dependencies.

A plan is self-contained when it carries its data (inline rows) or reads built-in file
formats from absolute paths; then any Spark Connect server runs it the same way. A plan
that refers to server-side state (a temp view, a table, SQL text, a function registered
by name, a checkpoint, uploaded rows, another data source) runs only where that state
lives, and so does inline Python code: the JVM's Python worker would lack the test's
import path and patches.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pyspark.sql.connect.proto as proto
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

from sparkwarp.config import (
    COMMAND,
    DATA_SOURCE,
    EXTRACT_VALUE,
    FUNCTION,
    LITERAL,
    NAMED_TABLE,
    PLAN_EXPRESSION,
    PLAN_RELATION,
    PORTABLE_FORMATS,
    READ,
    ROOT,
    SAIL_ONLY_EXPRESSIONS,
    SAIL_ONLY_RELATIONS,
    WRITE_OPERATION,
    WRITE_TO_PATH,
)


class PlanShape:
    """One walk over a plan: its expression depth, and whether it is portable."""

    def __init__(
        self, plan: proto.Plan, registered: frozenset[str] = frozenset()
    ) -> None:
        self.expression_depth = 0
        self.looks_up_by_column = False
        self._registered = frozenset(name.lower() for name in registered)
        self._needs_sail = False
        self._portable_kind = self._is_portable_kind(plan)
        self._walk(plan)

    @property
    def self_contained(self) -> bool:
        return self._portable_kind and not self._needs_sail

    @staticmethod
    def _is_portable_kind(plan: proto.Plan) -> bool:
        """A relation to run, or a write to a portable path; no other command."""
        kind = plan.WhichOneof("op_type")
        if kind == ROOT:
            return True
        is_write = (
            kind == COMMAND
            and plan.command.WhichOneof("command_type") == WRITE_OPERATION
        )
        if not is_write:
            return False
        write = plan.command.write_operation
        writes_to_path = write.WhichOneof("save_type") == WRITE_TO_PATH
        return writes_to_path and FileTarget(write.source, [write.path]).portable

    def _walk(self, top: Message) -> None:
        pending: list[tuple[Message, int]] = [(top, 0)]
        while pending:
            message, depth = pending.pop()
            is_expression = message.DESCRIPTOR.name == PLAN_EXPRESSION
            depth += is_expression
            if is_expression:
                self._note_expression(message)
            self.expression_depth = max(self.expression_depth, depth)
            if message.DESCRIPTOR.name == PLAN_RELATION:
                self._note_relation(message)
            pending.extend((child, depth) for child in MessageTree.children(message))

    def _note_expression(self, expression: Message) -> None:
        kind = expression.WhichOneof("expr_type")
        if kind in SAIL_ONLY_EXPRESSIONS or self._calls_a_registered(expression):
            self._needs_sail = True
        if kind == EXTRACT_VALUE:
            extraction = expression.unresolved_extract_value.extraction
            from_a_column = extraction.WhichOneof("expr_type") != LITERAL
            self.looks_up_by_column = self.looks_up_by_column or from_a_column

    def _calls_a_registered(self, expression: Message) -> bool:
        """A function called by name that is a UDF, or one registered on Sail."""
        if expression.WhichOneof("expr_type") != FUNCTION:
            return False
        function = expression.unresolved_function
        named = function.function_name.lower() in self._registered
        return bool(function.is_user_defined_function) or named

    def _note_relation(self, relation: Message) -> None:
        kind = relation.WhichOneof("rel_type")
        read_kind = relation.read.WhichOneof("read_type") if kind == READ else None
        source = relation.read.data_source
        reads_other_files = read_kind == DATA_SOURCE and not (
            FileTarget(source.format, list(source.paths)).portable
        )
        reads_a_table = read_kind == NAMED_TABLE
        if kind in SAIL_ONLY_RELATIONS or reads_a_table or reads_other_files:
            self._needs_sail = True


class MessageTree:
    """Iterative walks over a protobuf tree: deep plans would overflow recursion."""

    @staticmethod
    def children(message: Message) -> list[Message]:
        children: list[Message] = []
        for field, value in message.ListFields():
            if field.type != FieldDescriptor.TYPE_MESSAGE:
                continue
            if not FieldShape.repeated(field):
                children.append(value)
            elif not field.message_type.GetOptions().map_entry:
                children.extend(value)
        return children

    @staticmethod
    def walk(top: Message) -> Iterator[Message]:
        """Every message under `top`, itself included, children read after the yield."""
        pending = [top]
        while pending:
            message = pending.pop()
            yield message
            pending.extend(MessageTree.children(message))

    @staticmethod
    def relations_below(relation: Message) -> list[Message]:
        """The nearest relations under `relation`, also those inside its expressions."""
        found: list[Message] = []
        pending = MessageTree.children(relation)
        while pending:
            message = pending.pop()
            if message.DESCRIPTOR.name == PLAN_RELATION:
                found.append(message)
            else:
                pending.extend(MessageTree.children(message))
        return found


class FileTarget:
    """Files any Spark reads alike: a built-in format at absolute paths."""

    def __init__(self, file_format: str, paths: list[str]) -> None:
        self._format = file_format.lower()
        self._paths = paths

    @property
    def portable(self) -> bool:
        built_in = self._format in PORTABLE_FORMATS
        absolute = bool(self._paths) and all(os.path.isabs(p) for p in self._paths)
        return built_in and absolute


class FieldShape:
    """Whether a field repeats, on protobuf 5 (`label`) and 6+ (`is_repeated`) alike."""

    @staticmethod
    def repeated(field: FieldDescriptor) -> bool:
        is_repeated = getattr(field, "is_repeated", None)
        if is_repeated is not None:
            return bool(is_repeated)
        return bool(field.label == FieldDescriptor.LABEL_REPEATED)
