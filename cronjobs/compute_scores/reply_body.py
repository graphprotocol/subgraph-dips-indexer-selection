"""Read an indexer's reply in full, but never more of it than a set limit.

Each indexer registers its own URL, so the server behind it decides what it sends back. Read with
no limit, 1 indexer's huge or never-ending reply could run the job out of memory.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import aiohttp

# aiohttp raises its own buffer size, and how much of a compressed reply it decompresses at once,
# to match the largest read asked of it. Reads this small leave both at aiohttp's usual size, so
# a small compressed reply cannot fill several times the limit in memory before reading pauses.
_READ_SIZE = 64 * 1024


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
        # Never asking past 1 byte over the limit keeps what is collected here within it.
        piece = await resp.content.read(min(_READ_SIZE, limit + 1 - received))
        if not piece:
            return b"".join(pieces)
        received += len(piece)
        if received > limit:
            raise ReplyTooLargeError(f"reply is over the {limit:,} byte limit")
        pieces.append(piece)
