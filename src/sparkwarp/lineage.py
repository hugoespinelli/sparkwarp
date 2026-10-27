"""Cuts the lineage a loop keeps re-sending: the part of a plan that already ran.

PySpark sends a DataFrame's whole history with every action, and Sail plans it from
scratch, so a loop that re-derives a DataFrame pays again for every earlier round.
Before an action runs, the DataFrame under it is checkpointed on Sail and the action
reads the stored rows; when a later plan contains that DataFrame again, the subtree is
swapped for the checkpoint's cached relation.
"""

from __future__ import annotations

import hashlib
import os
from contextlib import suppress
from dataclasses import dataclass, field
from threading import RLock

import pyspark.sql.connect.proto as proto
from google.protobuf.message import Message
from pyspark.errors import PySparkException
from pyspark.sql.connect.client.core import SparkConnectClient
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.plan import CachedRemoteRelation

from sparkwarp.config import (
    ACTION_WRAPPERS,
    AGGREGATE,
    CACHED_REMOTE_RELATION,
    CHECKPOINT_RESULT,
    COLUMN_REFERENCES,
    COMMAND,
    COMMAND_TYPE,
    COUNT_FUNCTION,
    CUT_BUDGET_NODES,
    CUT_ENV_VAR,
    CUT_MAX_STORED,
    EXPR_TYPE,
    FINGERPRINT_BYTES,
    FUNCTION,
    LITERAL,
    ON_FLAG_VALUES,
    OP_TYPE,
    PLAN_EXPRESSION,
    PLAN_ID_FIELD,
    PLAN_RELATION,
    PROJECT,
    PURE_EXPRESSION_KINDS,
    PURE_FUNCTIONS,
    REL_TYPE,
    ROOT,
    SQL_COMMAND,
    SQL_COMMENT,
    SQL_CONF_KEYWORDS,
    STORABLE_RELATIONS,
    TRIAL_RELATION_ID,
)
from sparkwarp.plan_shape import MessageTree


class CutSwitch:
    """`SPARKWARP_CUT=1` turns the cut on; read per plan so a conftest can flip it."""

    @staticmethod
    def on() -> bool:
        return os.environ.get(CUT_ENV_VAR, "").strip().lower() in ON_FLAG_VALUES


class ActionWrappers:
    """Peels off what an action or a plain column selection adds over a DataFrame."""

    @staticmethod
    def strip(relation: Message) -> Message:
        while ActionWrappers._wraps(relation):
            relation = getattr(relation, relation.WhichOneof(REL_TYPE)).input
        return relation

    @staticmethod
    def read_partly(relation: Message) -> bool:
        """`first`, `show`, `limit` and `tail` read a few rows, not the whole frame."""
        while ActionWrappers._wraps(relation):
            kind = relation.WhichOneof(REL_TYPE)
            if kind in ACTION_WRAPPERS:
                return True
            relation = getattr(relation, kind).input
        return False

    @staticmethod
    def _wraps(relation: Message) -> bool:
        kind = relation.WhichOneof(REL_TYPE)
        return (
            kind in ACTION_WRAPPERS
            or CountAggregate.wraps(relation)
            or ColumnSelection.wraps(relation)
        )


class CountAggregate:
    """The global `count(1)` aggregate that `df.count()` sends."""

    @staticmethod
    def wraps(relation: Message) -> bool:
        if relation.WhichOneof(REL_TYPE) != AGGREGATE:
            return False
        aggregate = relation.aggregate
        plain_group_by = (
            aggregate.group_type == proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY
        )
        nothing_grouped = not aggregate.grouping_expressions
        measures = aggregate.aggregate_expressions
        only_a_count = len(measures) == 1 and CountAggregate._counts_a_literal(
            measures[0]
        )
        return plain_group_by and nothing_grouped and only_a_count

    @staticmethod
    def _counts_a_literal(expression: Message) -> bool:
        if expression.WhichOneof(EXPR_TYPE) != FUNCTION:
            return False
        function = expression.unresolved_function
        one_literal = len(function.arguments) == 1 and (
            function.arguments[0].WhichOneof(EXPR_TYPE) == LITERAL
        )
        is_count = function.function_name == COUNT_FUNCTION
        return is_count and not function.is_distinct and one_literal


class ColumnSelection:
    """A `select` of plain columns: cheap to plan, so the DataFrame under it is kept."""

    @staticmethod
    def wraps(relation: Message) -> bool:
        if relation.WhichOneof(REL_TYPE) != PROJECT:
            return False
        expressions = relation.project.expressions
        return bool(expressions) and all(
            expression.WhichOneof(EXPR_TYPE) in COLUMN_REFERENCES
            for expression in expressions
        )


class Fingerprint:
    """A hash of a relation subtree's bytes, serialized in C.

    PySpark stamps a plan id once per DataFrame object, so a DataFrame reused in a later
    round keeps its bytes; an equal subtree rebuilt from new objects does not.
    """

    @staticmethod
    def of(relation: Message) -> bytes:
        data = relation.SerializeToString(deterministic=True)
        return hashlib.blake2b(data, digest_size=FINGERPRINT_BYTES).digest()


class Pure:
    """A subtree a stored result may stand in for: the same rows on every run.

    An allowlist: every relation, expression kind and function must be a known pure
    built-in. Anything else (server state, user code, time, order, alias names) is
    never stored, so an unknown case costs speed, never correctness.
    """

    def __init__(self, own_ids: frozenset[str], registered: frozenset[str]) -> None:
        self._own_ids = own_ids
        self._registered = registered

    def check(self, top: Message) -> bool:
        return all(self._allowed(message) for message in MessageTree.walk(top))

    def _allowed(self, message: Message) -> bool:
        kind = message.DESCRIPTOR.name
        if kind == PLAN_RELATION:
            return self._relation_allowed(message)
        if kind == PLAN_EXPRESSION:
            return self._expression_allowed(message)
        return True

    def _relation_allowed(self, relation: Message) -> bool:
        kind = relation.WhichOneof(REL_TYPE)
        if kind == CACHED_REMOTE_RELATION:
            return relation.cached_remote_relation.relation_id in self._own_ids
        return kind in STORABLE_RELATIONS

    def _expression_allowed(self, expression: Message) -> bool:
        kind = expression.WhichOneof(EXPR_TYPE)
        if kind != FUNCTION:
            return kind in PURE_EXPRESSION_KINDS
        function = expression.unresolved_function
        name = function.function_name.lower()
        user_code = function.is_user_defined_function or name in self._registered
        return name in PURE_FUNCTIONS and not user_code


class SqlConf:
    """`spark.sql("SET ...")` changes conf without a config call; watch it too."""

    @staticmethod
    def changes(plan: Message) -> bool:
        is_sql = PlanKind.of(plan) == COMMAND and (
            plan.command.WhichOneof(COMMAND_TYPE) == SQL_COMMAND
        )
        if not is_sql:
            return False
        query = plan.command.sql_command.input.sql.query
        code = [
            line
            for line in query.splitlines()
            if not line.strip().startswith(SQL_COMMENT)
        ]
        words = " ".join(code).split()
        return bool(words) and words[0].upper() in SQL_CONF_KEYWORDS


class Budget:
    """Plans with more expressions than the budget are worth cutting."""

    @staticmethod
    def exceeded_by(top: Message, budget: int = CUT_BUDGET_NODES) -> bool:
        count = 0
        for message in MessageTree.walk(top):
            count += message.DESCRIPTOR.name == PLAN_EXPRESSION
            if count > budget:
                return True
        return False


class PlanKind:
    """Which oneof a plan carries: a relation to run, a command, a compressed plan."""

    @staticmethod
    def of(plan: Message) -> str | None:
        kind: str | None = plan.WhichOneof(OP_TYPE)
        return kind


class CheckpointHolder:
    """The DataFrame PySpark builds for a checkpoint result, over a cached relation."""

    @staticmethod
    def relation_id(holder: ConnectDataFrame) -> str | None:
        cached = holder._plan
        if not isinstance(cached, CachedRemoteRelation):
            return None
        relation_id: str | None = cached._relation_id
        return relation_id


@dataclass(frozen=True)
class CachedRoot:
    """A checkpoint on Sail, and what the subtree it replaces looked like.

    `holder` is kept so PySpark does not send a release while the id is in use; Sail
    frees a checkpoint only when its session ends. `size` filters candidates cheaply,
    and `inner_ids` are the plan ids that disappear when the subtree is swapped out.
    """

    relation_id: str
    holder: ConnectDataFrame
    size: int
    inner_ids: frozenset[int]


class CachedRoots:
    """Fingerprint to checkpoint, up to a fixed number per session.

    Sail keeps a checkpoint until its session ends whatever the client releases, so
    evicting an entry would free nothing; the cap bounds what is held instead.
    """

    def __init__(self) -> None:
        self._roots: dict[bytes, CachedRoot] = {}

    def __len__(self) -> int:
        return len(self._roots)

    @property
    def relation_ids(self) -> frozenset[str]:
        return frozenset(root.relation_id for root in self._roots.values())

    @property
    def sizes(self) -> frozenset[int]:
        return frozenset(root.size for root in self._roots.values())

    def by_relation_id(self, relation_id: str) -> CachedRoot | None:
        return next(
            (r for r in self._roots.values() if r.relation_id == relation_id), None
        )

    def lookup(self, fingerprint: bytes) -> CachedRoot | None:
        return self._roots.get(fingerprint)

    def holds(self, fingerprint: bytes) -> bool:
        return fingerprint in self._roots

    def remember(self, fingerprint: bytes, root: CachedRoot) -> None:
        self._roots[fingerprint] = root

    def drain(self) -> None:
        self._roots.clear()


class ColumnReferences:
    """Column references such as `df["x"]` carry the plan id of their DataFrame."""

    @staticmethod
    def dangle_in(top: Message, hidden: frozenset[int]) -> bool:
        """Some reference names a relation a swap removed, or one not in the plan.

        A reference into a removed subtree counts even if the id appears elsewhere too,
        because Spark resolves it against both places and may call it ambiguous.
        """
        referenced: set[int] = set()
        present: set[int] = set()
        for message in MessageTree.walk(top):
            kind = message.DESCRIPTOR.name
            if kind == PLAN_RELATION and message.common.HasField(PLAN_ID_FIELD):
                present.add(message.common.plan_id)
            if kind == PLAN_EXPRESSION:
                referenced.update(ColumnReferences._plan_id(message))
        return bool(referenced - present) or bool(referenced & hidden)

    @staticmethod
    def relation_ids_inside(relation: Message, roots: CachedRoots) -> frozenset[int]:
        """Plan ids under `relation` (not its own), through swapped-in checkpoints."""
        found: set[int] = set()
        for message in MessageTree.walk(relation):
            if message.DESCRIPTOR.name != PLAN_RELATION or message is relation:
                continue
            if message.common.HasField(PLAN_ID_FIELD):
                found.add(message.common.plan_id)
            if message.WhichOneof(REL_TYPE) == CACHED_REMOTE_RELATION:
                root = roots.by_relation_id(message.cached_remote_relation.relation_id)
                found.update(root.inner_ids if root is not None else ())
        return frozenset(found)

    @staticmethod
    def _plan_id(expression: Message) -> list[int]:
        kind = expression.WhichOneof(EXPR_TYPE)
        if kind not in COLUMN_REFERENCES:
            return []
        reference = getattr(expression, kind)
        return [reference.plan_id] if reference.HasField(PLAN_ID_FIELD) else []


class Splice:
    """Swaps every subtree the cache knows, top-down, for its cached relation.

    Top-down matters: the newest known round must match before an older one inside it.
    A subtree is fingerprinted only when its serialized size, computed in C, equals a
    stored one, and smaller subtrees are not searched at all.
    """

    def __init__(self, roots: CachedRoots) -> None:
        self._roots = roots
        self._sizes = roots.sizes
        self._smallest = min(self._sizes, default=0)
        self.hits = 0
        self.hidden: set[int] = set()

    def into(self, top: Message) -> Splice:
        pending = [top]
        while pending:
            relation = pending.pop()
            size = relation.ByteSize()
            if size < self._smallest:
                continue
            root = self._match(relation, size)
            if root is not None:
                Splice.replace(relation, root.relation_id)
                self.hits += 1
                self.hidden.update(root.inner_ids)
            else:
                pending.extend(MessageTree.relations_below(relation))
        return self

    def _match(self, relation: Message, size: int) -> CachedRoot | None:
        if size not in self._sizes:
            return None
        return self._roots.lookup(Fingerprint.of(relation))

    @staticmethod
    def replace(relation: Message, relation_id: str) -> None:
        common = proto.RelationCommon()
        common.CopyFrom(relation.common)
        relation.Clear()
        relation.common.CopyFrom(common)
        relation.cached_remote_relation.relation_id = relation_id


@dataclass(frozen=True)
class PendingRoot:
    """A DataFrame to store before the action runs, so it is computed only once."""

    fingerprint: bytes
    relation: Message
    size: int
    inner_ids: frozenset[int]


@dataclass(frozen=True)
class CutPlan:
    """What one request becomes: the plan to send, and what to store first."""

    sent: proto.Plan
    hits: int
    pending: PendingRoot | None = None
    stored: bool = False

    @classmethod
    def untouched(cls, plan: proto.Plan) -> CutPlan:
        return cls(plan, 0)

    def request_for(
        self, request: proto.ExecutePlanRequest
    ) -> proto.ExecutePlanRequest:
        changed = self.hits > 0 or self.stored
        if not changed:
            return request
        cut = proto.ExecutePlanRequest()
        cut.CopyFrom(request)
        cut.plan.CopyFrom(self.sent)
        return cut


class PlanCutter:
    """Prepares one plan for one Sail session's stored DataFrames."""

    def __init__(self, cuts: SessionCuts, registered: frozenset[str]) -> None:
        self._cuts = cuts
        self._registered = registered

    def prepare(self, plan: proto.Plan) -> CutPlan:
        is_root = PlanKind.of(plan) == ROOT
        if not is_root or not Budget.exceeded_by(plan.root):
            return CutPlan.untouched(plan)
        sent = proto.Plan()
        sent.CopyFrom(plan)
        candidate = ActionWrappers.strip(sent.root)
        worth_storing = Budget.exceeded_by(candidate) and not (
            ActionWrappers.read_partly(sent.root)
        )
        fingerprint = Fingerprint.of(candidate)
        size = candidate.ByteSize()
        roots = self._cuts.roots
        splice = Splice(roots).into(candidate)
        hidden = frozenset(splice.hidden)
        if splice.hits and ColumnReferences.dangle_in(sent.root, hidden):
            return CutPlan.untouched(plan)
        if worth_storing and self._storable(fingerprint, candidate, sent):
            inner = ColumnReferences.relation_ids_inside(candidate, roots)
            pending = PendingRoot(fingerprint, candidate, size, inner | hidden)
            return CutPlan(sent, splice.hits, pending)
        return CutPlan(sent, splice.hits)

    def _storable(
        self, fingerprint: bytes, candidate: Message, sent: proto.Plan
    ) -> bool:
        cuts = self._cuts
        known = cuts.roots.holds(fingerprint) or fingerprint in cuts.failed
        if known or cuts.full:
            return False
        pure = Pure(cuts.roots.relation_ids, self._registered).check(candidate)
        return pure and not TrialSwap.dangles(candidate, sent, cuts.roots)


class TrialSwap:
    """Whether swapping a candidate out would leave a column reference dangling."""

    @staticmethod
    def dangles(candidate: Message, sent: proto.Plan, roots: CachedRoots) -> bool:
        hidden = ColumnReferences.relation_ids_inside(candidate, roots)
        kept = proto.Relation()
        kept.CopyFrom(candidate)
        Splice.replace(candidate, TRIAL_RELATION_ID)
        try:
            return ColumnReferences.dangle_in(sent.root, hidden)
        finally:
            candidate.CopyFrom(kept)


class SailCheckpoints:
    """Checkpoints a relation on the Sail session that sent it."""

    @staticmethod
    def checkpoint(
        client: SparkConnectClient, pending: PendingRoot
    ) -> CachedRoot | None:
        relation = pending.relation
        command = proto.Command()
        command.checkpoint_command.relation.CopyFrom(relation)
        command.checkpoint_command.local = True
        command.checkpoint_command.eager = True
        with suppress(PySparkException):
            _, properties, *_ = client.execute_command(command)
            holder = properties.get(CHECKPOINT_RESULT)
            if isinstance(holder, ConnectDataFrame):
                relation_id = CheckpointHolder.relation_id(holder)
                if relation_id is not None:
                    return CachedRoot(
                        relation_id, holder, pending.size, pending.inner_ids
                    )
        return None


@dataclass
class SessionCuts:
    """One Sail client's stored DataFrames; holding the client keeps its id unique."""

    client: SparkConnectClient
    roots: CachedRoots = field(default_factory=CachedRoots)
    failed: set[bytes] = field(default_factory=set)
    made: int = 0

    @property
    def full(self) -> bool:
        """The session made its share of checkpoints; forgetting them frees nothing."""
        return self.made >= CUT_MAX_STORED

    def store(self, cut: CutPlan) -> CutPlan:
        """Checkpoints the pending DataFrame, then lets the action read the stored rows.

        A failed checkpoint (the query itself fails, say) sends the plan without that
        DataFrame stored, so the user's own error surfaces; it is not tried again.
        """
        pending = cut.pending
        if pending is None:
            return cut
        self.made += 1
        root = SailCheckpoints.checkpoint(self.client, pending)
        if root is None:
            self.failed.add(pending.fingerprint)
            return CutPlan(cut.sent, cut.hits)
        self.roots.remember(pending.fingerprint, root)
        Splice.replace(pending.relation, root.relation_id)
        return CutPlan(cut.sent, cut.hits, stored=True)


class LineageCuts:
    """The cut for every Sail session of the process; owned by the plan router."""

    def __init__(self) -> None:
        self._sessions: dict[int, SessionCuts] = {}
        self._lock = RLock()
        self.plans_cut = 0
        self.last_sent_bytes = 0

    @property
    def cached_roots(self) -> int:
        with self._lock:
            return sum(len(cuts.roots) for cuts in self._sessions.values())

    def prepare(
        self,
        client: SparkConnectClient,
        plan: proto.Plan,
        registered: frozenset[str] = frozenset(),
    ) -> CutPlan:
        if SqlConf.changes(plan):
            self.forget(client)
        if not CutSwitch.on():
            return CutPlan.untouched(plan)
        with self._lock:
            cuts = self._session(client)
            cut = cuts.store(PlanCutter(cuts, registered).prepare(plan))
            self.plans_cut += int(cut.hits > 0)
            self.last_sent_bytes = cut.sent.ByteSize()
        return cut

    def forget(self, client: SparkConnectClient) -> None:
        """A conf write can change results, so the session's stored DataFrames go.

        They stay on Sail until the session ends, so the count of those made does not
        go down.
        """
        with self._lock:
            cuts = self._sessions.get(id(client))
            if cuts is not None:
                cuts.roots.drain()
                cuts.failed.clear()

    def release_all(self) -> None:
        """Forgets every stored DataFrame; Sail frees them when its sessions end."""
        with self._lock:
            for cuts in self._sessions.values():
                cuts.roots.drain()
            self._sessions.clear()

    def _session(self, client: SparkConnectClient) -> SessionCuts:
        key = id(client)
        if key not in self._sessions:
            self._sessions[key] = SessionCuts(client)
        return self._sessions[key]
