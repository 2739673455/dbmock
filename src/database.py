"""Doris数据库管理能力"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .settings import DorisConfig

logger = logging.getLogger(__name__)


class DorisStreamLoader:
    """通过严格模式Stream Load批量写入Doris"""

    def __init__(self, config: DorisConfig, batch_id: str) -> None:
        self._config = config
        self._batch_id = re.sub(r"[^A-Za-z0-9_-]", "_", batch_id)
        self._sequences: defaultdict[str, int] = defaultdict(int)
        self._client = httpx.Client(timeout=600, trust_env=False)
        self._lock = threading.Lock()

    @staticmethod
    def _identifier(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Doris标识符无效: {value}")
        return value

    @classmethod
    def _parquet_bytes(cls, rows: Sequence[Mapping[str, Any]]) -> bytes:
        normalized = []
        for row in rows:
            nested = {
                key: json.dumps(
                    value, ensure_ascii=False, default=str, separators=(",", ":")
                )
                for key, value in row.items()
                if isinstance(value, (dict, list))
            }
            # 普通业务行直接交给 Arrow，只有嵌套字段需要 JSON 化和行复制。
            normalized.append(dict(row) | nested if nested else row)
        table = pa.Table.from_pylist(normalized)
        sink = pa.BufferOutputStream()
        pq.write_table(
            table,
            sink,
            compression="snappy",
            use_dictionary=True,
            write_statistics=False,
        )
        return sink.getvalue().to_pybytes()

    def load(self, table_name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        database = self._identifier(self._config.database)
        table = self._identifier(table_name)
        with self._lock:
            self._sequences[table] += 1
            sequence = self._sequences[table]
        label = f"{self._batch_id}_{table}_{sequence}"
        body = self._parquet_bytes(rows)
        headers = {
            "Expect": "100-continue",
            "format": "parquet",
            "strict_mode": "true",
            "max_filter_ratio": "0",
            "timezone": "Asia/Shanghai",
            "label": label,
            "timeout": "600",
        }
        endpoint = (
            f"http://{self._config.host}:{self._config.http_port}"
            f"/api/{database}/{table}/_stream_load"
        )
        response = self._client.put(
            endpoint,
            auth=(self._config.user, self._config.password),
            headers=headers,
            content=body,
        )
        if response.is_redirect:
            location = response.headers.get("location")
            if not location:
                raise RuntimeError(f"Doris Stream Load重定向缺少地址: {table}")
            redirect_url = httpx.URL(location).copy_with(
                username=None,
                password=None,
            )
            response = self._client.put(
                redirect_url,
                auth=(self._config.user, self._config.password),
                headers=headers,
                content=body,
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Doris Stream Load响应不是对象: {table}")
        status = str(payload.get("Status", ""))
        total = int(payload.get("NumberTotalRows", 0))
        loaded = int(payload.get("NumberLoadedRows", 0))
        filtered = int(payload.get("NumberFilteredRows", 0))
        if status != "Success" or total != len(rows) or loaded != len(rows) or filtered:
            raise RuntimeError(
                "Doris Stream Load失败 "
                f"table={table} status={status} total={total} "
                f"loaded={loaded} filtered={filtered} "
                f"message={payload.get('Message')}"
            )

    def close(self) -> None:
        self._client.close()
