"""The context an item carries between stages, in the `weirparent` header:

    weirparent: 00;pipeline;key;born_at_ms;flags;last_stage

Text fields are percent-encoded, so keys may contain `;`. A newer version may append fields,
which this version ignores (as W3C traceparent does).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from urllib.parse import quote, unquote

HEADER = "weirparent"
VERSION = "00"
SAMPLED = 0x01

Headers = Mapping[str, str | bytes] | Iterable[tuple[str, str | bytes]]


@dataclass(frozen=True)
class Context:
    pipeline: str
    key: str
    event_time: float  # birth time, unix seconds: every age counts from here
    sampled: bool = False  # reserved for per-key lineage
    last_stage: str = ""  # the stage that emitted it
    invalid: bool = False  # the header was there but could not be read; never sent

    def header(self) -> str:
        flags = SAMPLED if self.sampled else 0
        fields = [VERSION, _enc(self.pipeline), _enc(self.key), str(int(self.event_time * 1000)), f"{flags:02x}"]
        return ";".join([*fields, _enc(self.last_stage)])

    @classmethod
    def from_header(cls, value: str | bytes | None) -> Context | None:
        """None if the value is missing or malformed."""
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode(errors="replace")
        parts = value.split(";")
        version = parts[0]
        if len(version) != 2 or version == "ff" or not _is_hex(version):
            return None
        if len(parts) < 6 or (version == VERSION and len(parts) != 6):
            return None
        _, pipeline, key, born_at_ms, flags, last_stage = parts[:6]
        if not born_at_ms.isdigit() or len(flags) != 2 or not _is_hex(flags):
            return None
        sampled = bool(int(flags, 16) & SAMPLED)
        return cls(unquote(pipeline), unquote(key), int(born_at_ms) / 1000, sampled, unquote(last_stage))


def extract(headers: Headers | None) -> Context | None:
    """The context from a dict or Kafka-style [(key, value)] headers. None without the header;
    an `invalid` context if the header is there but malformed, so the stage can count it."""
    if not headers:
        return None
    pairs = headers.items() if isinstance(headers, Mapping) else headers
    for key, value in pairs:
        if key == HEADER:
            return Context.from_header(value) or Context("", "", 0.0, invalid=True)
    return None


def inject(ctx: Context) -> list[tuple[str, str]]:
    """Kafka-style headers carrying the context."""
    return [(HEADER, ctx.header())]


def _enc(text: str) -> str:
    return quote(text, safe="")


def _is_hex(text: str) -> bool:
    return all(c in "0123456789abcdef" for c in text)
