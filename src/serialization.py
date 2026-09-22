"""内部状态的显式类型编码，不执行持久化数据中的代码。"""

from collections import deque
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import cache
import json
from typing import Any

from .timeline import BusinessState, CartPosition, InventoryPosition, ScheduledFact

TYPES = {
    cls.__name__: cls
    for cls in (BusinessState, CartPosition, InventoryPosition, ScheduledFact)
}


SCALAR_TYPES = frozenset((str, int, float, bool, type(None)))


@cache
def _field_names(cls: type) -> tuple[str, ...]:
    return tuple(field.name for field in fields(cls))


def encode(value: Any) -> Any:
    # 业务行中绝大多数值是普通标量，跳过特殊类型和容器判断。
    if type(value) in SCALAR_TYPES:
        return value
    if isinstance(value, dict):
        if all(isinstance(k, str) for k in value):
            return {
                k: v if type(v) in SCALAR_TYPES else encode(v) for k, v in value.items()
            }
        return {"$map": [[encode(k), encode(v)] for k, v in value.items()]}
    if isinstance(value, list):
        return [v if type(v) in SCALAR_TYPES else encode(v) for v in value]
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, (set, tuple, deque)):
        return {"$" + type(value).__name__: [encode(v) for v in value]}
    if is_dataclass(value):
        return {
            "$type": type(value).__name__,
            "fields": {
                name: encode(getattr(value, name)) for name in _field_names(type(value))
            },
        }
    return value


def decode(value: Any) -> Any:
    if isinstance(value, list):
        return [decode(v) for v in value]
    if not isinstance(value, dict):
        return value
    for tag, constructor in (
        ("$datetime", datetime.fromisoformat),
        ("$date", date.fromisoformat),
        ("$decimal", Decimal),
    ):
        if tag in value:
            return constructor(value[tag])
    if "$type" in value:
        return TYPES[value["$type"]](
            **{k: decode(v) for k, v in value["fields"].items()}
        )
    if "$map" in value:
        return {decode(k): decode(v) for k, v in value["$map"]}
    for tag, constructor in (("$set", set), ("$tuple", tuple), ("$deque", deque)):
        if tag in value:
            return constructor(decode(v) for v in value[tag])
    return {k: decode(v) for k, v in value.items()}


def dumps(value) -> bytes:
    return json.dumps(encode(value), ensure_ascii=False, separators=(",", ":")).encode()


def loads(value: bytes) -> Any:
    return decode(json.loads(value))
