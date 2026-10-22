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
STACK_SIZE_ENV_VAR = "SAIL_RUNTIME__STACK_SIZE"
SAIL_STACK_SIZE_BYTES = 32 * 1024 * 1024
UPB_BACKEND = "upb"

JVM_ENV_VAR = "SPARKWARP_JVM"
JVM_DEPTH_ENV_VAR = "SPARKWARP_JVM_DEPTH"
JVM_DIR_ENV_VAR = "SPARKWARP_JVM_DIR"
JVM_DIR_PREFIX = "sparkwarp-jvm-"
DEFAULT_JVM_DEPTH = 64
OFF_FLAG_VALUES = frozenset({"0", "false", "no", "off"})
SERVER_STATE_FILE = "server.json"
SERVER_LOCK_FILE = "server.lock"
SERVER_LOG_FILE = "server.log"
COUNTS_FILE_PATTERN = "counts-{pid}.json"
COUNTS_FILE_GLOB = "counts-*.json"
ROUTING_SUMMARY = (
    "sparkwarp: {total} {noun} ran on the JVM ({deep_plans} too deep for Sail, "
    "{sail_gaps} Sail gaps)"
)
QUERY_NOUNS = ("query", "queries")
OWNER_FILE = "owner.pid"
JVM_START_TIMEOUT_SECONDS = 180
JVM_BUSY_WAIT_SECONDS = 30
JVM_STOP_TIMEOUT_SECONDS = 30
LOCK_POLL_SECONDS = 0.1
WATCHDOG_POLL_SECONDS = 1.0
PORT_PROBE_SECONDS = 1.0
WATCHDOG_MODULE = "sparkwarp.jvm_watchdog"
CONNECT_SERVER_CLASS = "org.apache.spark.sql.connect.service.SparkConnectServer"
CONNECT_BIND_ADDRESS = "127.0.0.1"
CONNECT_MASTER = "local[*]"
CONNECT_SERVER_CONF = {
    "spark.connect.grpc.binding.address": CONNECT_BIND_ADDRESS,
    "spark.ui.enabled": "false",
    "spark.log.level": "WARN",
    "spark.sql.session.timeZone": "UTC",
    "spark.connect.session.manager.defaultSessionTimeout": "7d",
}
CONNECT_PORT_KEY = "spark.connect.grpc.binding.port"
JVM_DEFAULT_TIMEZONE_OPTION = "-Duser.timezone=UTC"
SPARK_SUBMIT = ("bin", "spark-submit")
SPARK_INTERNAL_RESOURCE = "spark-internal"
SPARK_HOME_ENV_VAR = "SPARK_HOME"
PYSPARK_PYTHON_ENV_VAR = "PYSPARK_PYTHON"
JAVA_HOME_ENV_VAR = "JAVA_HOME"
JAVA_EXECUTABLE = "java"
JAVA_MINIMUM_VERSION = 17
JAVA_VERSION_PATTERN = r'version "(\d+)(?:\.(\d+))?'
JAVA_LEGACY_MAJOR = 1
JVM_START_FAILED = "the JVM Spark Connect server did not start ({reason}); see {log}"
START_EXITED = "it exited"
START_TIMED_OUT = "it timed out"
JVM_LOST = "the JVM Spark Connect server stopped answering ({reason})"
FAILURE_FILE = "failed.txt"
LOCK_TIMEOUT = "another sparkwarp process held {path} for over {seconds} s"
JAVA_BIN_DIR = "bin"
WINDOWS_PLATFORM = "win32"
ATOMIC_SUFFIX = ".tmp"
FAILED_LOG_NAME = "sparkwarp-jvm-failed-{pid}.log"
LOCK_WAIT_SECONDS = 360
PROC_STATUS = "/proc/{pid}/status"
PROC_CMDLINE = "/proc/{pid}/cmdline"
ZOMBIE_STATE = "State:\tZ"
PLAN_RELATION = "Relation"
PLAN_EXPRESSION = "Expression"
SERVER_STATE_RELATIONS = frozenset(
    {
        "sql",
        "catalog",
        "cached_remote_relation",
        "cached_local_relation",
        "chunked_cached_local_relation",
        "extension",
        "ml_relation",
    }
)
CONF_SET = "set"
CONF_WRITES = frozenset({CONF_SET, "unset"})
COMMAND = "command"
ROOT = "root"
READ = "read"
NAMED_TABLE = "named_table"
DATA_SOURCE = "data_source"
WRITE_OPERATION = "write_operation"
WRITE_TO_PATH = "path"
EXTRACT_VALUE = "unresolved_extract_value"
LITERAL = "literal"
FUNCTION = "unresolved_function"
REGISTER_FUNCTION = "register_function"
NO_JVM_SERVER = "sparkwarp routed a plan to the JVM before its engine started"
DEEP_COLUMN_FEATURE = (
    "On Sail, a column nested {depth} levels deep (SPARKWARP_JVM_DEPTH is {limit}; "
    "{why}. Install Java 17+ so sparkwarp runs it on the JVM, mark the test "
    "spark_only, or set SPARKWARP_JVM_DEPTH=0 to run it slowly on Sail)"
)
NO_JVM_REASON = "the JVM server is off or Java 17+ was not found"
SERVER_STATE_EXPRESSIONS = frozenset(
    {"expression_string", "call_function", "extension"}
)
INLINE_CODE_RELATIONS = frozenset(
    {
        "map_partitions",
        "group_map",
        "co_group_map",
        "apply_in_pandas_with_state",
        "common_inline_user_defined_table_function",
        "common_inline_user_defined_data_source",
    }
)
INLINE_CODE_EXPRESSIONS = frozenset({"common_inline_user_defined_function"})
SAIL_ONLY_RELATIONS = SERVER_STATE_RELATIONS | INLINE_CODE_RELATIONS
SAIL_ONLY_EXPRESSIONS = SERVER_STATE_EXPRESSIONS | INLINE_CODE_EXPRESSIONS
PORTABLE_FORMATS = frozenset({"", "parquet", "csv", "json", "orc", "text"})
SAIL_GAP_SIGNALS = (
    "extraction must be a literal",
    "does not satisfy order requirements",
    "function: input_file_name",
    "Incompatible inputs for Union",
)
LOOKUP_CAST_SIGNAL = "Cannot cast string"

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
JVM_ONLY_CONDITION = "JVM_ATTRIBUTE_NOT_SUPPORTED"
NOT_IMPLEMENTED_CONDITION = "NOT_IMPLEMENTED"
UNSUPPORTED_CONDITIONS = frozenset({NOT_IMPLEMENTED_CONDITION, JVM_ONLY_CONDITION})
UNSUPPORTED_ERROR_CLASSES = frozenset({"UnsupportedOperationException"})
PLUGIN_OWNS_STOP_WARNING = (
    "sparkwarp.stop() was ignored: the pytest plugin owns this session and stops "
    "it when the run ends."
)
