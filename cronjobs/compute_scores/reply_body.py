"""Read an indexer's reply in full, but never more of it than a set limit.

Each indexer registers its own URL, so the server behind it decides what it sends back. Read with
no limit, 1 indexer's huge or never-ending reply could run the job out of memory.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import aiohttp


class ReplyTooLargeError(ValueError):
    """An indexer's reply was bigger than the limit set for it.

    A ValueError, like a body that is not JSON, because asking again would get the same reply.
    """


async def read_reply_body(resp: "aiohttp.ClientResponse", limit: int) -> bytes:
    """Return the whole body of a reply; raise ReplyTooLargeError once it passes `limit` bytes."""
    declared = resp.content_length
    if declared is not None and declared > limit:
        raise ReplyTooLargeError(
            f"reply declares {declared:,} bytes, over the {limit:,} byte limit"
        )

    pieces: list[bytes] = []
    received = 0
    while True:
        # A read returns as soon as any of the body has arrived, so keep reading until it ends.
        # Asking for at most 1 byte past the limit means no more than that is ever held.
        piece = await resp.content.read(limit + 1 - received)
        if not piece:
            return b"".join(pieces)
        received += len(piece)
        if received > limit:
            raise ReplyTooLargeError(f"reply is over the {limit:,} byte limit")
        pieces.append(piece)
