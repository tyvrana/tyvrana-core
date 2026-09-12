"""Typed settings for a local adapter listener."""

from pydantic import BaseModel, ConfigDict, Field


class CoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    host: str = Field(default="127.0.0.1", min_length=1, pattern=r"\S")
    port: int = Field(default=8765, ge=0, le=65535)
    registration_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    operation_timeout: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    send_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    close_timeout: float = Field(default=2.0, gt=0, allow_inf_nan=False)
    max_message_size: int = Field(default=1_048_576, gt=0)
    max_artifact_size: int = Field(default=16 * 1024 * 1024, gt=0)
    max_artifact_storage: int = Field(default=64 * 1024 * 1024, gt=0)
    max_artifact_entries: int = Field(default=128, gt=0)
    max_artifact_transfers: int = Field(default=4, gt=0)
    max_inline_image_bytes: int = Field(default=4 * 1024 * 1024, gt=0)
