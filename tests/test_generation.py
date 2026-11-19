from datetime import datetime
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sqlalchemy import text

from main import _parse_args
from src import generation
from src.settings import DATA_DIR, RunContext
from src.state_store import StateStore
from tests.pg_fixture import DisposableDatabase


class GenerationTest(DisposableDatabase):
    def test_initial_append_partial_replay_and_rollback(self):
        with (DATA_DIR / "manifest.json").open("rb") as stream:
            if stream.read(48).startswith(b"version https://git-lfs.github.com"):
                self.skipTest("requires materialized Git LFS fixtures")
        # Keep the optional quality-report output away from a real checkout.
        seeds = Path(self.enterContext(TemporaryDirectory()))
        for path in DATA_DIR.iterdir():
            if path.name != "quality_report.json" and path.is_file():
                copyfile(path, seeds / path.name)
        self.enterContext(patch("src.settings.DATA_DIR", seeds))
        ctx = self.ctx
        store = StateStore(ctx, ctx.lock.check)

        def run(*args):
            generation.run(ctx, _parse_args(args), ctx.lock.check)

        ctx.as_of_time = datetime(2026, 2, 5, 12)
        run("--start-month", "2026-01", "--end-month", "2026-01", "--smoke")
        run("--validate-only")
        january = store.get("dataset")
        self.assertEqual(january["completed"].isoformat(), "2026-01-01")
        self.assertEqual(len(january["counts"]), 45)

        # Fail after business rows have committed, before month validation.
        with patch.object(
            generation.quality,
            "validate_database",
            side_effect=RuntimeError("injected"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                run("--end-month", "2026-02")
        restored = store.get("dataset")
        self.assertFalse(restored["dirty"])
        self.assertEqual(restored["counts"], january["counts"])
        with ctx.engine.connect() as conn:
            self.assertEqual(
                conn.scalar(
                    text(
                        "SELECT COUNT(*) FROM dwd_traffic_page_view_di WHERE biz_date >= '2026-02-01'"
                    )
                ),
                0,
            )
        # Emulate death between committing the data and starting the rollback:
        # the next coordinator must see dirty state and redo the whole month.
        with (
            patch.object(
                generation.quality,
                "validate_database",
                side_effect=RuntimeError("interrupted"),
            ),
            patch.object(
                generation, "rollback", side_effect=RuntimeError("restart needed")
            ),
            self.assertRaisesRegex(RuntimeError, "restart needed"),
        ):
            run("--end-month", "2026-02")
        self.assertTrue(store.get("dataset")["dirty"])
        ctx.lock.__exit__(None, None, None)
        ctx = RunContext(self.ctx.db, as_of_time=self.ctx.as_of_time)
        self.addCleanup(ctx.close)
        self.enterContext(ctx.lock)
        store = StateStore(ctx, ctx.lock.check)
        run("--end-month", "2026-02")
        partial = store.get("dataset")
        self.assertEqual(partial["cutoff"], ctx.as_of_time)
        run("--end-month", "2026-02")
        self.assertEqual(store.get("dataset"), partial)
        ctx.as_of_time = datetime(2026, 3, 1, 12)
        run("--end-month", "2026-02")
        run("--validate-only")
        with self.assertRaisesRegex(ValueError, "禁止重复"):
            run("--end-month", "2026-02")
        final = store.get("dataset")
        self.assertGreater(
            final["counts"]["dwd_traffic_page_view_di"],
            partial["counts"]["dwd_traffic_page_view_di"],
        )
