"""Doris 中的月度状态文档与跨进程生成互斥。"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import threading
import time
import zlib
from pathlib import Path
from uuid import uuid4
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

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

    def wait_for_loads(self, timeout: float = 60) -> None:
        """回滚前等待可能已提交但尚未可见的装载，超时则保留待恢复状态。"""
        deadline = time.monotonic() + timeout
        while True:
            self.guard()
            with self.ctx.engine.connect() as conn:
                databases = conn.execute(text("SHOW PROC '/transactions'")).mappings()
                database = next(
                    (row for row in databases if row["DbName"] == self.ctx.db.database),
                    None,
                )
                if database is None:
                    raise RuntimeError("无法确认目标数据库的事务状态，拒绝回滚")
                if int(database["RunningTransactionNum"]) == 0:
                    return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "仍有未结束的装载事务，暂停回滚；事务结束后重新运行生成命令"
                )
            logger.warning("等待目标数据库的装载事务结束后再回滚")
            time.sleep(1)

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
        # 一个 Stream Load 事务提交整份文档；旧版本多余分块由 revision 排除。
        self.ctx.loader.load("dbmock_documents", rows)
        written_at = time.perf_counter()
        if key == "dataset":
            write_status(self.ctx, value)
        try:
            self.guard()
            with self.ctx.engine.connect() as conn:
                conn.execute(
                    text(
                        "DELETE FROM dbmock_documents WHERE document_key = :key AND revision != :revision"
                    ),
                    {"key": key, "revision": revision},
                )
        except Exception:
            logger.warning(
                "旧状态分块清理失败，已提交的新状态仍有效 document=%s",
                key,
                exc_info=True,
            )

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
    """原子建表抢占；连接失效后以保留的重命名墓碑防止并发回收旧锁。"""

    def __init__(self, engine):
        self.engine = engine
        self.conn: Connection
        self.owner: dict = {}
        self._mutex = threading.Lock()
        self._stop = threading.Event()
        self._lost = None
        self._thread = None

    def _comment(self):
        value = self.conn.execute(
            text(
                "SELECT TABLE_COMMENT FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dbmock_run_lock'"
            )
        ).scalar_one_or_none()
        return json.loads(value) if value else None

    def _sessions(self):
        return (
            self.conn.execute(
                text(
                    "SELECT Id, Host, LoginTime, FE FROM information_schema.processlist"
                )
            )
            .mappings()
            .all()
        )

    @staticmethod
    def _identity(row):
        return {key: str(row.get(key, "")) for key in ("Id", "Host", "LoginTime", "FE")}

    def __enter__(self):
        self.conn = self.engine.connect()
        try:
            current = next(
                row
                for row in self.conn.execute(text("SHOW FULL PROCESSLIST")).mappings()
                if row["CurrentConnected"] == "Yes"
            )
            self.owner = self._identity(current) | {"token": uuid4().hex}
            for attempt in range(5):
                try:
                    self.conn.execute(
                        text(
                            "CREATE TABLE dbmock_run_lock (lock_id INT NOT NULL) ENGINE=OLAP DUPLICATE KEY(lock_id) COMMENT :owner DISTRIBUTED BY HASH(lock_id) BUCKETS 1 PROPERTIES ('replication_num'='1')"
                        ),
                        {"owner": json.dumps(self.owner)},
                    )
                    break
                except DBAPIError:
                    previous = self._comment()
                    if previous is None:
                        raise
                    identity = {
                        key: previous[key] for key in ("Id", "Host", "LoginTime", "FE")
                    }
                    if any(self._identity(row) == identity for row in self._sessions()):
                        raise ValueError(
                            "该数据库已有生成或校验任务运行，拒绝并发执行"
                        ) from None
                    token = previous["token"]
                    if len(token) != 32 or any(
                        c not in "0123456789abcdef" for c in token
                    ):
                        raise ValueError("生成锁标识损坏，拒绝自动回收") from None
                    try:
                        # 不删除墓碑，延迟到达的竞争者无法把新锁改名为同一个旧锁墓碑。
                        self.conn.execute(
                            text(
                                f"ALTER TABLE dbmock_run_lock RENAME dbmock_stale_lock_{token}"
                            )
                        )
                        logger.warning("已回收中断任务的生成锁")
                    except DBAPIError:
                        # 其他任务已回收；重新检查当前持有者。
                        continue
            else:
                raise RuntimeError("生成锁回收失败，请检查数据库权限和并发任务")
            self._thread = threading.Thread(target=self._heartbeat, daemon=True)
            self._thread.start()
            return self
        except BaseException:
            self.conn.close()
            raise

    def _heartbeat(self):
        while not self._stop.wait(10):
            try:
                self.check()
            except Exception as exc:
                self._lost = exc
                return

    def check(self):
        if self._lost:
            raise RuntimeError(
                "生成锁连接已失效，停止写入；下次运行将回滚未完成月份"
            ) from self._lost
        with self._mutex:
            try:
                self.conn.execute(text("SELECT 1"))
                if self._comment() != self.owner:
                    raise RuntimeError("生成锁所有者发生变化")
            except Exception as exc:
                self._lost = exc
                raise

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread:
            self._thread.join()
        try:
            if self._lost is None:
                with self._mutex:
                    if self._comment() == self.owner:
                        self.conn.execute(text("DROP TABLE dbmock_run_lock"))
        finally:
            self.conn.close()
