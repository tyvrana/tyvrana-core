import pytest
from pydantic import ValidationError

from tyvrana_core import CoreConfig


def test_defaults_are_local_and_bounded() -> None:
    config = CoreConfig()
    assert config.host == "127.0.0.1"
    assert 0 < config.port < 65536
    assert config.registration_timeout > 0
    assert config.operation_timeout > 0
    assert config.max_message_size == 1_048_576
    assert CoreConfig(port=0).port == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host", ""),
        ("host", "  "),
        ("port", -1),
        ("port", 65536),
        ("port", "8765"),
        ("port", True),
        ("registration_timeout", 0),
        ("operation_timeout", -1),
        ("send_timeout", float("inf")),
        ("close_timeout", float("nan")),
        ("max_message_size", 0),
        ("max_artifact_size", 0),
        ("max_artifact_storage", -1),
        ("max_artifact_entries", True),
        ("max_artifact_transfers", "4"),
        ("max_inline_image_bytes", 0),
        ("unknown", 1),
    ],
)
def test_invalid_configuration_is_rejected(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        CoreConfig.model_validate({field: value})


def test_configuration_is_frozen() -> None:
    config = CoreConfig()
    with pytest.raises(ValidationError, match="frozen"):
        config.port = 1234  # type: ignore[misc]
