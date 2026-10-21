"""Unit tests for the corpus harness's guards; no Spark engine is needed."""

from pyspark.errors import PySparkTypeError
from pyspark.errors.exceptions.connect import AnalysisException

from corpus.harness import Outcome


def test_client_side_errors_are_flagged() -> None:
    client_error = PySparkTypeError(message="argument has the wrong type")
    assert Outcome.of_error(client_error).from_client
    assert Outcome.of_error(TypeError("bad argument")).from_client


def test_engine_errors_are_not_flagged() -> None:
    assert not Outcome.of_error(AnalysisException("unresolved column")).from_client


def test_the_client_flag_does_not_take_part_in_comparison() -> None:
    engine = Outcome(error="X", from_client=False)
    client = Outcome(error="X", from_client=True)
    assert engine.matches(client)


def test_exact_comparison_keeps_int_and_float_apart() -> None:
    assert not Outcome(rows=[1]).matches(Outcome(rows=[1.0]))
