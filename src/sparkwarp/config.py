"""Names and defaults shared across sparkwarp. Other modules hold no such literals."""

from __future__ import annotations

from enum import Enum


class Ownership(str, Enum):
    """Who started the running engine, and so who may stop it."""

    LIBRARY = "library"
    PLUGIN = "plugin"


ACTIVATION_ENV_VAR = "SPARKWARP"
ORIGINAL_ENV_VAR = "SPARKWARP_ORIGINAL_ENV"
CLI_FLAG = "--sparkwarp"
CLI_FLAG_DEST = "sparkwarp"
CLI_GROUP = "sparkwarp"
CLI_FLAG_HELP = "Run PySpark tests on Sail (no JVM)."
PLUGIN_NAME = "sparkwarp-session"
WARNING_STACKLEVEL = 3
ON_FLAG_VALUES = frozenset({"1", "true", "yes", "on"})

CONNECT_MODE_ENV_VAR = "SPARK_CONNECT_MODE_ENABLED"
REMOTE_ENV_VAR = "SPARK_REMOTE"
MASTER_ENV_VAR = "MASTER"
CONNECT_MODE_ON = "1"
CONNECT_URL_SCHEME = "sc://"

SPARK_MASTER_KEY = "spark.master"
SPARK_REMOTE_KEY = "spark.remote"
CLUSTER_URL_KEYS = frozenset({SPARK_MASTER_KEY, SPARK_REMOTE_KEY})
TIMEZONE_KEY = "spark.sql.session.timeZone"
ANSI_KEY = "spark.sql.ansi.enabled"
ANSI_ON = "true"
DEFAULT_TIMEZONE = "UTC"
ENGINE_LABEL = "sail"

LOCALHOST = "127.0.0.1"
ANY_FREE_PORT = 0
CHECKPOINT_ENV_VAR = "SAIL_EXECUTION__CHECKPOINT__PATH"
CHECKPOINT_DIR_PREFIX = "sparkwarp-checkpoint-"

STUB_DEFAULT_PARALLELISM = 1
STUB_ATTRIBUTE_PARAMETER = "attr_name"
STUB_PREFIX = "sparkContext."
CLOSE_WITHOUT_ORIGINAL_ERROR = "cannot close the session with {original!r}"
RDD_FEATURE = "rdd"
FEATURE_PARAMETER = "feature"

SPARK_ONLY_MARKER = "spark_only"
SPARK_ONLY_MARKER_HELP = (
    "spark_only: the test needs real Spark (DeltaTable API, RDD, streaming). "
    "sparkwarp skips it on Sail; it runs when sparkwarp is off (SPARKWARP=0)."
)
SPARK_ONLY_SKIP_REASON = (
    "spark_only: needs real Spark; runs in the SPARKWARP=0 parity lane"
)
HINT_SECTION_TITLE = "sparkwarp"
UNSUPPORTED_HINT = (
    "This test hit a feature that Spark Connect or Sail does not support. "
    "KNOWN_GAPS.md in the sparkwarp source distribution lists the known gaps. If the "
    "test needs real Spark, mark it @pytest.mark.spark_only: sparkwarp then skips it, "
    "and it runs when SPARKWARP=0."
)
PROTOBUF_DEPTH_SIGNAL = "MaxDepth"
JVM_ONLY_CONDITION = "JVM_ATTRIBUTE_NOT_SUPPORTED"
NOT_IMPLEMENTED_CONDITION = "NOT_IMPLEMENTED"
UNSUPPORTED_CONDITIONS = frozenset({NOT_IMPLEMENTED_CONDITION, JVM_ONLY_CONDITION})
UNSUPPORTED_ERROR_CLASSES = frozenset({"UnsupportedOperationException"})
PLUGIN_OWNS_STOP_WARNING = (
    "sparkwarp.stop() was ignored: the pytest plugin owns this session and stops "
    "it when the run ends."
)
