"""PostgreSQL 中的月度状态文档与跨进程生成互斥。"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import threading
import time
import zlib
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from . import serialization
from .settings import DATA_DIR, ROOT_DIR, now

logger = logging.getLogger(__name__)
STATUS_FILE = ROOT_DIR / ".generation-status.json"


def write_status(ctx, dataset) -> None:
    """导出便于查看的进度；数据库仍是追加与恢复的唯一依据。"""
    status = {
        "updated_at": now().isoformat(timespec="seconds"),
        "timezone": "Asia/Shanghai",
        "database": f"{ctx.db.host}:{ctx.db.port}/{ctx.db.database}",
        "start_month": dataset["start"].strftime("%Y-%m") if dataset else None,
        "cutoff": dataset["cutoff"].isoformat()
        if dataset and dataset["cutoff"]
        else None,
        "completed_month": dataset["completed"].strftime("%Y-%m")
        if dataset and dataset["completed"]
        else None,
        "active_month": dataset["active"].strftime("%Y-%m")
        if dataset and dataset["active"]
        else None,
        "smoke": dataset["smoke"] if dataset else None,
        "needs_recovery": dataset["dirty"] if dataset else False,
    }
    temporary = STATUS_FILE.with_suffix(".json.tmp")
    try:
        temporary.write_text(
            json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(STATUS_FILE)
    except OSError:
        logger.warning("本地进度文件更新失败，以数据库进度为准", exc_info=True)


REQUIRED_SEEDS = {
    "manifest.json",
    "brands.json",
    "categories.json",
    "geo_regions.json",
    "logistics_companies.json",
    "payment_types.json",
    "shops.json",
    "spus.jsonl",
    "skus.jsonl",
    "lineage.jsonl",
}


def seed_fingerprints(directory: Path = DATA_DIR) -> dict[str, str]:
    result = {}
    for path in sorted(directory.iterdir()):
        if (
            not path.is_file()
            or path.suffix not in {".json", ".jsonl"}
            or path.name == "quality_report.json"
        ):
            continue
        digest = hashlib.sha256()
        with path.open("rb") as source:
            first = source.read(1024 * 1024)
            if first.startswith(b"version https://git-lfs.github.com/spec/v1"):
                raise ValueError(f"种子文件尚未拉取，请执行 git lfs pull: {path.name}")
            digest.update(first)
            while block := source.read(1024 * 1024):
                digest.update(block)
        result[path.name] = digest.hexdigest()
    if missing := REQUIRED_SEEDS - result.keys():
        raise ValueError(f"缺少种子文件: {sorted(missing)}")
    return result


def check_seeds(expected: dict, actual: dict) -> None:
    changed = sorted(
        name
        for name in expected.keys() | actual.keys()
        if expected.get(name) != actual.get(name)
    )
    if changed:
        raise ValueError(f"种子数据发生变化，拒绝生成: {', '.join(changed)}")


class StateStore:
    def __init__(self, ctx, guard=lambda: None):
        self.ctx = ctx
        self.guard = guard

    def get(self, key: str) -> Any:
        with self.ctx.engine.connect() as conn:
            rows = (
                conn.execute(
                    text(
                        "SELECT chunk_no, chunk_count, revision, digest, payload FROM dbmock_documents WHERE document_key = :key ORDER BY chunk_no"
                    ),
                    {"key": key},
                )
                .mappings()
                .all()
            )
        if not rows:
            return None
        head = rows[0]
        rows = [row for row in rows if row["revision"] == head["revision"]]
        if (
            head["chunk_no"] != 0
            or len(rows) != head["chunk_count"]
            or [row["chunk_no"] for row in rows] != list(range(head["chunk_count"]))
        ):
            raise ValueError(f"内部状态文档不完整: {key}")
        compressed = base64.b64decode("".join(row["payload"] for row in rows))
        if hashlib.sha256(compressed).hexdigest() != head["digest"]:
            raise ValueError(f"内部状态文档校验失败: {key}")
        return serialization.loads(zlib.decompress(compressed))

    def wait_for_loads(self) -> None:
        self.ctx.lock.drain()

    def put(self, key: str, value) -> dict[str, int | float]:
        """保存状态文档，返回大小、分块数和各阶段耗时。"""
        started = time.perf_counter()
        converted = serialization.encode(value)
        converted_at = time.perf_counter()
        serialized = json.dumps(
            converted, ensure_ascii=False, separators=(",", ":")
        ).encode()
        json_encoded_at = time.perf_counter()
        del converted
        serialized_at = time.perf_counter()
        compressed = zlib.compress(serialized, level=3)
        del serialized
        compressed_at = time.perf_counter()
        payload = base64.b64encode(compressed).decode()
        chunks = [payload[i : i + 60000] for i in range(0, len(payload), 60000)]
        revision = uuid4().hex
        digest = hashlib.sha256(compressed).hexdigest()
        rows = [
            {
                "document_key": key,
                "chunk_no": index,
                "chunk_count": len(chunks),
                "revision": revision,
                "digest": digest,
                "payload": chunk,
            }
            for index, chunk in enumerate(chunks)
        ]
        encoded_at = time.perf_counter()
        self.guard()
        # Replace every chunk (including stale tail chunks) in one transaction.
        with self.ctx.write_transaction() as conn:
            conn.execute(
                text("DELETE FROM dbmock_documents WHERE document_key = :key"),
                {"key": key},
            )
            self.ctx.loader.load_in(conn, "dbmock_documents", rows)
        written_at = time.perf_counter()
        if key == "dataset":
            write_status(self.ctx, value)

        return {
            "compressed_bytes": len(compressed),
            "chunks": len(chunks),
            "serialize_seconds": serialized_at - started,
            "type_convert_seconds": converted_at - started,
            "json_encode_seconds": json_encoded_at - converted_at,
            "serialize_release_seconds": serialized_at - json_encoded_at,
            "compress_seconds": compressed_at - serialized_at,
            "encode_seconds": encoded_at - compressed_at,
            "write_seconds": written_at - encoded_at,
            "cleanup_seconds": time.perf_counter() - written_at,
        }


class GenerationLock:
    """One coordinator per database; fence every independent writer transaction.

    Session lock (DBMK,1) identifies the live coordinator. Shared transaction
    locks (DBMK,2) cover every write through COMMIT; the exclusive barrier drains
    even an orphan backend whose client didn't receive the commit response.
    """

    _OWNER = text("""
        SELECT EXISTS (
          SELECT 1 FROM pg_catalog.pg_locks l
          JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid
          WHERE l.locktype = 'advisory' AND l.granted AND l.mode = 'ExclusiveLock'
            AND l.database = (SELECT oid FROM pg_catalog.pg_database
                               WHERE datname = current_database())
            AND l.classid = 1145195851 AND l.objid = 1 AND l.objsubid = 2
            AND a.pid = :pid AND a.backend_start = :started
        )
    """)

    def __init__(self, engine):
        self.engine = engine
        self.conn: Connection | None = None
        self.owner: dict = {}
        self._mutex = threading.Lock()

    def __enter__(self):
        conn = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        # Session advisory locks must never go back to the general pool.
        conn.detach()
        self.conn = conn
        try:
            if not conn.scalar(text("SELECT pg_try_advisory_lock(1145195851, 1)")):
                raise ValueError("该数据库已有生成或校验任务运行，拒绝并发执行")
            self.owner = dict(
                conn.execute(
                    text("""
                SELECT pid, backend_start AS started FROM pg_catalog.pg_stat_activity
                WHERE pid = pg_backend_pid()
            """)
                )
                .mappings()
                .one()
            )
            self.drain()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def _verify(self, conn):
        if not self.owner or not conn.scalar(self._OWNER, self.owner):
            raise RuntimeError("生成锁连接已失效，停止写入；下次运行将回滚未完成月份")

    def check(self):
        with self._mutex:
            if self.conn is None:
                raise RuntimeError("生成任务未持锁")
            self._verify(self.conn)

    def drain(self):
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '60s'"))
            conn.execute(text("SELECT pg_advisory_xact_lock(1145195851, 2)"))
            self._verify(conn)

    @contextmanager
    def transaction(self):
        with self.engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock_shared(1145195851, 2)"))
            self._verify(conn)
            yield conn

    def __exit__(self, *exc):
        with self._mutex:
            self.owner = {}
            if self.conn is not None:
                self.conn.close()
                self.conn = None
