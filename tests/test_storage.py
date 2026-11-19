import base64
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import psycopg
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.state_store import GenerationLock, StateStore
from tests.pg_fixture import DisposableDatabase


class StorageTest(DisposableDatabase):
    def test_patch_explicit_null_and_atomic_failure(self):
        ctx = self.ctx
        table = "dbmock_documents"
        row = dict(
            document_key="x",
            chunk_no=0,
            chunk_count=1,
            revision="a",
            digest="x",
            payload="value",
        )
        ctx.loader.load(table, [row])
        ctx.loader.load(table, [dict(document_key="x", chunk_no=0, payload="patched")])
        with self.assertRaises((psycopg.IntegrityError, IntegrityError)):
            ctx.loader.load(
                table,
                [dict(document_key=k, chunk_no=0, payload=None) for k in ("x", "new")],
            )
        with ctx.engine.connect() as conn:
            self.assertEqual(
                conn.execute(
                    text("SELECT document_key, payload, revision FROM dbmock_documents")
                ).one(),
                ("x", "patched", "a"),
            )

    def test_atomic_document_shrink(self):
        store = StateStore(self.ctx, self.ctx.lock.check)
        value = base64.b64encode(random.Random(42).randbytes(200000)).decode()
        self.assertGreater(store.put("checkpoint", value)["chunks"], 1)
        self.assertEqual(store.get("checkpoint"), value)
        store.put("checkpoint", "small")
        self.assertEqual(store.get("checkpoint"), "small")
        with self.ctx.engine.connect() as conn:
            self.assertEqual(
                conn.scalar(text("SELECT count(*) FROM dbmock_documents")), 1
            )

    def test_lock_and_lost_coordinator(self):
        ctx = self.ctx
        with self.assertRaisesRegex(ValueError, "拒绝并发"):
            with GenerationLock(ctx.engine):
                self.fail("another coordinator acquired the lock")
        stale = ctx.lock
        entered, release = Event(), Event()

        def old_worker():
            with stale.transaction():
                entered.set()
                if not release.wait(10):
                    raise TimeoutError("test writer not released")

        def new_coordinator():
            with GenerationLock(ctx.engine):
                with self.assertRaises(RuntimeError):
                    with stale.transaction():
                        self.fail("stale coordinator accepted")
                return True

        with ThreadPoolExecutor(2) as executor:
            worker = executor.submit(old_worker)
            self.assertTrue(entered.wait(5))
            try:
                # Simulate a process losing its coordinator while COPY is in flight.
                with ctx.engine.begin() as conn:
                    self.assertTrue(
                        conn.scalar(
                            text("SELECT pg_terminate_backend(:pid)"),
                            {"pid": stale.owner["pid"]},
                        )
                    )
                next_run = executor.submit(new_coordinator)
                from time import monotonic, sleep

                deadline = monotonic() + 5
                with ctx.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as conn:
                    while not conn.scalar(
                        text("""
                        SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
                            AND classid=1145195851 AND objid=2 AND NOT granted
                            AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))
                    """)
                    ):
                        if monotonic() >= deadline:
                            self.fail("replacement did not wait for old writer")
                        sleep(0.02)
            finally:
                release.set()
            worker.result(5)
            self.assertTrue(next_run.result(5))
