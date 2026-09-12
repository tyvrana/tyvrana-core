import hashlib
from uuid import uuid4

from tyvrana_protocol import (
    MAX_ARTIFACT_CHUNK_SIZE,
    ArtifactAccepted,
    ArtifactBegin,
    ArtifactComplete,
    ArtifactDescriptor,
    ArtifactReady,
    encode_artifact_chunk,
)

from .helpers import FakeAdapter


def begin_message(
    data: bytes,
    request_id: str = "request",
    *,
    media_type: str = "application/octet-stream",
) -> ArtifactBegin:
    return ArtifactBegin(
        type="artifact.begin",
        transfer_id=uuid4().hex,
        request_id=request_id,
        descriptor=ArtifactDescriptor(
            artifact_id=uuid4().hex,
            name="artifact",
            media_type=media_type,
            byte_size=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        ),
    )


async def begin(fake: FakeAdapter, message: ArtifactBegin) -> None:
    await fake.send(message)
    assert await fake.receive() == ArtifactReady(
        type="artifact.ready", transfer_id=message.transfer_id
    )


async def upload(fake: FakeAdapter, message: ArtifactBegin, data: bytes) -> None:
    await begin(fake, message)
    for offset in range(0, len(data), MAX_ARTIFACT_CHUNK_SIZE):
        await fake.websocket.send(
            encode_artifact_chunk(
                message.transfer_id,
                offset,
                data[offset : offset + MAX_ARTIFACT_CHUNK_SIZE],
            )
        )
    await fake.send(
        ArtifactComplete(type="artifact.complete", transfer_id=message.transfer_id)
    )
    assert await fake.receive() == ArtifactAccepted(
        type="artifact.accepted", transfer_id=message.transfer_id
    )
