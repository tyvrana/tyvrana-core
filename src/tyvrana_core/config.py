"""Typed settings for a local adapter listener."""

import os
import sys
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


def state_directory() -> str:
    if sys.platform == "win32":
        return str(
            Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData/Local"))
            / "Tyvrana"
        )
    if sys.platform == "darwin":
        return str(Path.home() / "Library/Application Support/Tyvrana")
    return str(
        Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share")) / "tyvrana"
    )


class CoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    state_directory: str = Field(default_factory=state_directory, min_length=1)

    host: str = Field(default="127.0.0.1", min_length=1, pattern=r"\S")
    port: int = Field(default=8765, ge=0, le=65535)
    registration_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    operation_timeout: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    send_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    close_timeout: float = Field(default=2.0, gt=0, allow_inf_nan=False)
    max_message_size: int = Field(default=4 * 1024 * 1024, gt=0)
    max_artifact_size: int = Field(default=128 * 1024 * 1024, gt=0)
    max_artifact_storage: int = Field(default=512 * 1024 * 1024, gt=0)
    max_artifact_entries: int = Field(default=128, gt=0)
    max_artifact_transfers: int = Field(default=4, gt=0)
    max_inline_image_bytes: int = Field(default=4 * 1024 * 1024, gt=0)
