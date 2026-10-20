import pytest

from sparkwarp.config import ACTIVATION_ENV_VAR
from sparkwarp.pytest_plugin import Activation


def test_nothing_set_means_inactive() -> None:
    assert not Activation(False, {}).active


def test_cli_flag_activates() -> None:
    assert Activation(True, {}).active


def test_cli_flag_beats_an_explicit_off() -> None:
    assert Activation(True, {ACTIVATION_ENV_VAR: "0"}).active


@pytest.mark.parametrize("value", ["0", "false", "No", "OFF", "", "  ", "spark"])
def test_off_or_unknown_values_stay_inactive(value: str) -> None:
    assert not Activation(False, {ACTIVATION_ENV_VAR: value}).active


@pytest.mark.parametrize("value", ["1", "true", "YES", " on "])
def test_on_values_activate(value: str) -> None:
    assert Activation(False, {ACTIVATION_ENV_VAR: value}).active
