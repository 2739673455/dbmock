"""Inventory snapshots and PostgreSQL versions of the cross-table quality checks."""

from datetime import date, datetime, time, timedelta
from decimal import Decimal

from sqlalchemy import text

from src.batches.snapshots import materialize_period
from src.quality import CHECKS
from src.support import date_key
from src.timeline import MonthPeriod
from tests.pg_fixture import DisposableDatabase


class InventorySnapshotTest(DisposableDatabase):
    def test_quality_checks_execute_on_postgresql(self):
        cutoff = datetime(2024, 2, 4, 12)
        with self.ctx.engine.connect() as conn:
            for name, query in CHECKS.items():
                with self.subTest(name=name):
                    self.assertEqual(
                        conn.execute(
                            text(query), {"lifecycle_cutoff": cutoff}
                        ).scalar_one()
                        or 0,
                        0,
                    )

    def test_opening_stock_daily_close_nullable_cost_and_replay(self):
        opening_day = date(2024, 1, 31)
        days = [opening_day + timedelta(days=i) for i in range(5)]
        self.ctx.loader.load(
            "dim_date",
            [
                {
                    "date_key": date_key(day),
                    "full_date": day,
                    "calendar_year": day.year,
                    "calendar_quarter": 1,
                    "calendar_month": day.month,
                    "year_month_code": day.strftime("%Y-%m"),
                    "week_of_year": day.isocalendar().week,
                    "day_of_month": day.day,
                    "day_of_week": day.isoweekday(),
                    "day_name_cn": "星期" + "一二三四五六日"[day.weekday()],
                    "fiscal_year": day.year,
                    "fiscal_quarter": 1,
                }
                for day in days
            ],
        )

        stock_events = [
            (days[0], 1, (0, 0, 0), (20, 0, 0), Decimal("2.50"), "INITIAL_STOCK"),
            (days[2], 9, (20, 0, 0), (16, 2, 0), Decimal("2.50"), "SALE_RESERVE"),
            (days[2], 10, (16, 2, 0), (15, 4, 1), Decimal("3.00"), "SALE_RESERVE"),
            (days[3], 9, (15, 4, 1), (11, 4, 2), None, "SALE_RESERVE"),
        ]
        rows = []
        for idx, (day, hour, before, after, cost, change_type) in enumerate(
            stock_events, 1
        ):
            timestamp = datetime.combine(day, time(hour))
            rows.append(
                {
                    "inventory_change_id": idx,
                    "change_no": f"IC{idx}",
                    "event_date_key": date_key(day),
                    "warehouse_sk": 1,
                    "warehouse_id": 1,
                    "sku_sk": 1,
                    "sku_id": 1,
                    "spu_sk": 1,
                    "spu_id": 1,
                    "shop_sk": 1,
                    "shop_id": 1,
                    "change_type": change_type,
                    "biz_type": "PURCHASE_RECEIPT" if idx == 1 else "ORDER",
                    "biz_id": str(idx),
                    "before_on_hand_qty": before[0],
                    "before_reserved_qty": before[1],
                    "before_in_transit_qty": before[2],
                    "after_on_hand_qty": after[0],
                    "after_reserved_qty": after[1],
                    "after_in_transit_qty": after[2],
                    "unit_cost": cost,
                    "event_time": timestamp,
                    "biz_date": day,
                    "source_record_id": f"fixture:{idx}",
                    "load_batch_id": f"fixture-{day:%Y-%m}",
                    "dw_load_time": timestamp + timedelta(minutes=10),
                }
            )
        self.ctx.loader.load("dwd_inventory_change_di", rows)

        self.ctx.validation_cutoff = datetime.combine(days[-1], time(12))
        self.assertEqual(
            materialize_period(
                self.ctx, MonthPeriod("2024-01", days[0], days[0]), "fixture-2024-01"
            ),
            1,
        )
        self.assertEqual(
            materialize_period(
                self.ctx, MonthPeriod("2024-02", days[1], days[-1]), "fixture-2024-02"
            ),
            4,
        )

        # Recompute the previously published part of the current day in place.
        self.ctx.validation_cutoff = datetime.combine(days[-1], time(17))
        self.assertEqual(
            materialize_period(
                self.ctx, MonthPeriod("2024-02", days[-1], days[-1]), "replay-2024-02"
            ),
            1,
        )
        with self.ctx.engine.connect() as conn:
            snapshots = conn.execute(
                text(
                    """
                    SELECT biz_date, on_hand_qty, reserved_qty, available_qty,
                           in_transit_qty, unit_cost, inventory_cost_amount,
                           snapshot_time, dw_load_time, load_batch_id
                    FROM dwd_inventory_daily_snapshot_df
                    ORDER BY biz_date
                    """
                )
            ).all()
            self.assertEqual([row.biz_date for row in snapshots], days)
            self.assertEqual(
                [tuple(row[1:7]) for row in snapshots],
                [
                    (20, 0, 20, 0, Decimal("2.50"), Decimal("50.00")),
                    (20, 0, 20, 0, Decimal("2.50"), Decimal("50.00")),
                    (15, 4, 11, 1, Decimal("3.00"), Decimal("45.00")),
                    (11, 4, 7, 2, Decimal("3.00"), Decimal("33.00")),
                    (11, 4, 7, 2, Decimal("3.00"), Decimal("33.00")),
                ],
            )
            self.assertEqual(
                snapshots[-1][7:],
                (
                    self.ctx.validation_cutoff,
                    self.ctx.validation_cutoff,
                    "replay-2024-02",
                ),
            )
            for subject in ("库存事件首尾衔接", "库存快照覆盖范围", "库存期末快照"):
                self.assertEqual(conn.execute(text(CHECKS[subject])).scalar_one(), 0)
