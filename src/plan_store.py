"""月度计划中的维度索引与按业务时间发布的行记录。"""

from __future__ import annotations

import copy
import re
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .support import END_OF_TIME, start_of_day


# 与建表脚本共用键定义，计划中显式分配代理键，避免补充执行重新分配。
def table_keys() -> dict[str, tuple[str, ...]]:
    ddl = (
        Path(__file__).resolve().parents[1] / "scripts/sql/ecommerce.sql"
    ).read_text()
    return {
        name: tuple(re.findall(r"`(\w+)`", keys))
        for name, keys in re.findall(
            r"CREATE TABLE (\w+)\s*\(.*?UNIQUE KEY\s*\(([^)]+)\)", ddl, re.S
        )
        if not name.startswith("dbmock_")
    }


FACT_TIME_COLUMNS = {
    "dwd_interaction_cart_event_di": "event_time",
    "dwd_interaction_favor_event_di": "event_time",
    "dwd_inventory_change_di": "event_time",
    "dwd_inventory_daily_snapshot_df": "snapshot_time",
    "dwd_marketing_user_coupon_event_di": "event_time",
    "dwd_product_sku_price_change_di": "price_effective_time",
    "dwd_service_comment_detail_di": "comment_time",
    "dwd_trade_delivery_di": "delivery_create_time",
    "dwd_trade_delivery_item_di": "delivery_create_time",
    "dwd_trade_delivery_status_event_di": "event_time",
    "dwd_trade_order_detail_activity_di": "order_create_time",
    "dwd_trade_order_detail_coupon_di": "coupon_use_time",
    "dwd_trade_order_detail_di": "order_create_time",
    "dwd_trade_order_status_event_di": "event_time",
    "dwd_trade_pay_detail_di": "pay_request_time",
    "dwd_trade_pay_order_detail_di": "pay_request_time",
    "dwd_trade_pay_status_event_di": "event_time",
    "dwd_trade_refund_detail_di": "apply_time",
    "dwd_trade_refund_pay_detail_di": "refund_pay_request_time",
    "dwd_trade_refund_pay_status_event_di": "event_time",
    "dwd_trade_refund_status_event_di": "event_time",
    "dwd_traffic_page_view_di": "event_time",
    "dwd_traffic_search_click_di": "event_time",
    "dwd_traffic_search_di": "event_time",
    "dwd_traffic_session_di": "session_start_time",
}


class PlanStore:
    def __init__(self, month_start: datetime, dimensions: dict[str, list[dict]]):
        self.month_start = month_start
        self.keys = table_keys()
        self.dimensions: dict[str, dict[tuple, dict]] = defaultdict(dict)
        for table, rows in copy.deepcopy(dimensions).items():
            for row in rows:
                self.dimensions[table][self.key(table, row)] = row
        self.next_ids = {
            table: max((int(key[0]) for key in rows), default=0) + 1
            for table, rows in self.dimensions.items()
        }
        self.journal: dict[str, list[dict]] = defaultdict(list)
        self._lock = threading.Lock()

    def key(self, table: str, row: dict) -> tuple:
        return tuple(row[name] for name in self.keys[table])

    def rows(self, table: str) -> list[dict[str, Any]]:
        return list(self.dimensions[table].values())

    def snapshot(self) -> dict[str, list[dict]]:
        return {
            table: copy.deepcopy(list(rows.values()))
            for table, rows in self.dimensions.items()
        }

    def load(self, table: str, rows) -> None:
        with self._lock:
            for source in rows:
                row = dict(source)
                if not table.startswith("dwd_"):
                    key_name = self.keys[table][0]
                    if key_name not in row:
                        row[key_name] = self.next_ids.get(table, 1)
                        self.next_ids[table] = row[key_name] + 1
                    self.dimensions[table][self.key(table, row)] = row
                self.journal[table].append(row)

    def available_at(self, table: str, row: dict) -> datetime:
        if table in FACT_TIME_COLUMNS:
            return row[FACT_TIME_COLUMNS[table]]
        if table == "dim_date":
            return start_of_day(row["full_date"])
        if table in {"bridge_promotion_scope", "bridge_coupon_scope"}:
            parent, key = (
                ("dim_promotion_rule_version", "promotion_version_sk")
                if table == "bridge_promotion_scope"
                else ("dim_coupon_template_version", "coupon_template_version_sk")
            )
            rule = self.dimensions[parent][(row[key],)]
            return max(self.month_start, rule["rule_effective_start_time"])
        if (
            row.get("is_current") == 0
            and row.get("effective_end_time", END_OF_TIME) != END_OF_TIME
        ):
            return row["effective_end_time"]
        return max(
            self.month_start,
            row.get(
                "effective_start_time",
                row.get("rule_effective_start_time", self.month_start),
            ),
        )

    def visible(self, cutoff: datetime) -> dict[str, list[dict]]:
        """获得截至当前时间的目标行，包括尚未结束会话的当前聚合值。"""
        result: dict[str, list[dict]] = {}
        for table, rows in self.journal.items():
            result[table] = [
                dict(row) for row in rows if self.available_at(table, row) <= cutoff
            ]
        views: dict[str, list[dict]] = defaultdict(list)
        searches: dict[str, int] = defaultdict(int)
        for row in result.get("dwd_traffic_page_view_di", []):
            views[row["session_id"]].append(row)
        for row in result.get("dwd_traffic_search_di", []):
            searches[row["session_id"]] += 1
        for row in result.get("dwd_traffic_session_di", []):
            if row["session_end_time"] > cutoff:
                pages = sorted(
                    views[row["session_id"]],
                    key=lambda page: (page["event_time"], page["page_view_id"]),
                )
                row["session_end_time"] = cutoff
                row["session_duration_sec"] = int(
                    (cutoff - row["session_start_time"]).total_seconds()
                )
                row["page_view_count"] = len(pages)
                row["search_count"] = searches[row["session_id"]]
                row["is_bounce"] = int(len(pages) == 1)
                if pages:
                    row["exit_page_sk"] = pages[-1]["page_sk"]
                    row["exit_page_id"] = pages[-1]["page_id"]
        for rows in result.values():
            for row in rows:
                for field in ("dw_load_time", "dw_update_time"):
                    if isinstance(row.get(field), datetime):
                        row[field] = min(row[field], cutoff)
        return result
