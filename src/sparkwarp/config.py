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

SPARK_ONLY_MARKER_HELP = (
    "spark_only: the test needs real Spark (Delta, RDD, streaming). "
    "Routing to a JVM arrives in a later release; the marker is registered only."
)
PLUGIN_OWNS_STOP_WARNING = (
    "sparkwarp.stop() was ignored: the pytest plugin owns this session and stops "
    "it when the run ends."
)
