"""初始化具有真实活动窗口和适用范围的营销规则"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal

from ..plan_store import PlanStore

from ..settings import RunContext
from ..support import (
    END_OF_TIME,
    UNKNOWN_ID,
    UNKNOWN_SK,
    TableWriter,
    stable_hash,
    start_of_day,
)

logger = logging.getLogger(__name__)


def _rule_audit(rule: dict, ctx: RunContext) -> dict:
    return {
        "rule_hash": stable_hash(rule),
        "source_update_time": None,
        "load_batch_id": ctx.initial_batch_id,
    }


def _campaign_window(
    ctx: RunContext,
    index: int,
    total: int,
    rule_kind: str,
) -> tuple[datetime, datetime]:
    span_days = max(1, (ctx.gen.end_date - ctx.gen.start_date).days)
    offset = min(span_days, span_days * index // max(total, 1))
    anchor = ctx.gen.start_date + timedelta(days=offset)
    special_days = (
        anchor.replace(month=6, day=18),
        anchor.replace(month=11, day=11),
        anchor.replace(month=12, day=12),
    )
    valid_specials = [
        day for day in special_days if ctx.gen.start_date <= day <= ctx.gen.end_date
    ]
    if index % 4 == 0 and valid_specials:
        anchor = min(valid_specials, key=lambda day: abs((day - anchor).days))
    if rule_kind == "promotion":
        duration = 10 + index % 19
    elif rule_kind == "coupon":
        duration = 7 + index % 15
    else:
        raise ValueError(f"不支持的营销规则类型: {rule_kind}")
    start = start_of_day(max(ctx.gen.start_date, anchor - timedelta(days=1)))
    end_date = anchor + timedelta(days=duration)
    return start, start_of_day(end_date)


def _promotion_rows(ctx: RunContext):
    unknown = {
        "promotion_version_sk": UNKNOWN_SK,
        "promotion_id": UNKNOWN_ID,
        "rule_version_no": 1,
        "promotion_name": "未知促销",
        "promotion_type": "未知",
        "promotion_scene": "未知",
        "promotion_priority": 1,
        "activity_start_time": datetime(1900, 1, 1),
        "activity_end_time": END_OF_TIME,
        "rule_effective_start_time": datetime(1900, 1, 1),
        "rule_effective_end_time": END_OF_TIME,
        "rule_description": "未知促销规则",
        "threshold_amount": None,
        "discount_amount": None,
        "discount_rate": None,
        "max_discount_amount": None,
        "sponsor_type": "未知",
        "sponsor_business_id": None,
        "promotion_status": "已发布",
    }
    yield unknown | _rule_audit(unknown, ctx)
    types = ("满减", "折扣", "直降")
    for index in range(ctx.gen.promotion_count):
        activity_start, activity_end = _campaign_window(
            ctx,
            index,
            ctx.gen.promotion_count,
            "promotion",
        )
        promotion_type = types[index % len(types)]
        threshold = Decimal((index % 6 + 1) * 50)
        discount = Decimal((index % 5 + 1) * 10)
        rule = {
            "promotion_id": ctx.gen.start_date.toordinal() * 100_000 + index + 1,
            "rule_version_no": 1,
            "promotion_name": f"平台{promotion_type}活动{index + 1:03d}",
            "promotion_type": promotion_type,
            "promotion_scene": "主题活动" if index % 4 == 0 else "日常营销",
            "promotion_priority": index % 10 + 1,
            "activity_start_time": activity_start,
            "activity_end_time": activity_end,
            "rule_effective_start_time": activity_start,
            "rule_effective_end_time": END_OF_TIME,
            "rule_description": f"满 {threshold} 元享活动优惠",
            "threshold_amount": threshold,
            "discount_amount": discount if promotion_type != "折扣" else None,
            "discount_rate": Decimal("0.880000") if promotion_type == "折扣" else None,
            "max_discount_amount": Decimal("100.00")
            if promotion_type == "折扣"
            else None,
            "sponsor_type": "平台",
            "sponsor_business_id": "PLATFORM",
            "promotion_status": "已发布",
        }
        yield rule | _rule_audit(rule, ctx)


def _coupon_rows(ctx: RunContext):
    unknown = {
        "coupon_template_version_sk": UNKNOWN_SK,
        "coupon_template_id": UNKNOWN_ID,
        "rule_version_no": 1,
        "coupon_name": "未知优惠券",
        "coupon_type": "未知",
        "threshold_amount": None,
        "discount_amount": None,
        "discount_rate": None,
        "max_discount_amount": None,
        "issue_start_time": datetime(1900, 1, 1),
        "issue_end_time": END_OF_TIME,
        "use_start_time": datetime(1900, 1, 1),
        "use_end_time": END_OF_TIME,
        "rule_effective_start_time": datetime(1900, 1, 1),
        "rule_effective_end_time": END_OF_TIME,
        "total_issue_limit": None,
        "per_user_limit": None,
        "coupon_status": "已发布",
    }
    yield unknown | _rule_audit(unknown, ctx)
    for index in range(ctx.gen.coupon_count):
        use_start, use_end = _campaign_window(
            ctx,
            index,
            ctx.gen.coupon_count,
            "coupon",
        )
        issue_start = max(
            start_of_day(ctx.gen.start_date),
            use_start - timedelta(days=3),
        )
        is_discount = index % 4 == 3
        threshold = Decimal((index % 6 + 1) * 50)
        discount = Decimal((index % 4 + 1) * 10)
        rule = {
            "coupon_template_id": ctx.gen.start_date.toordinal() * 100_000 + index + 1,
            "rule_version_no": 1,
            "coupon_name": f"限时{'折扣' if is_discount else '满减'}券{index + 1:03d}",
            "coupon_type": "折扣券" if is_discount else "满减券",
            "threshold_amount": threshold,
            "discount_amount": None if is_discount else discount,
            "discount_rate": Decimal("0.920000") if is_discount else None,
            "max_discount_amount": Decimal("50.00") if is_discount else None,
            "issue_start_time": issue_start,
            "issue_end_time": use_end,
            "use_start_time": use_start,
            "use_end_time": use_end,
            "rule_effective_start_time": issue_start,
            "rule_effective_end_time": END_OF_TIME,
            "total_issue_limit": 5_000 + index % 10 * 1_000,
            "per_user_limit": 1 if index % 3 else 2,
            "coupon_status": "已发布",
        }
        yield rule | _rule_audit(rule, ctx)


def run(ctx: RunContext, store: PlanStore) -> None:
    writer = TableWriter(
        store, ctx.gen.batch_size, ctx.gen.dataset_start, ctx.data_end_time
    )
    first = not store.rows("dim_promotion_rule_version")
    for row in _promotion_rows(ctx):
        if first or row["promotion_id"] != UNKNOWN_ID:
            writer.add("dim_promotion_rule_version", row)
    for row in _coupon_rows(ctx):
        if first or row["coupon_template_id"] != UNKNOWN_ID:
            writer.add("dim_coupon_template_version", row)
    writer.flush_all()
    categories = [
        row
        for row in store.rows("dim_category_info_zip")
        if row["is_current"] and row["is_leaf"] and row["category_id"] != UNKNOWN_ID
    ]
    shops = [
        row
        for row in store.rows("dim_shop_info_zip")
        if row["is_current"] and row["shop_id"] != UNKNOWN_ID
    ]
    for kind, parent, scope_table, id_key, sk_key in (
        (
            "promotion",
            "dim_promotion_rule_version",
            "bridge_promotion_scope",
            "promotion_id",
            "promotion_version_sk",
        ),
        (
            "coupon",
            "dim_coupon_template_version",
            "bridge_coupon_scope",
            "coupon_template_id",
            "coupon_template_version_sk",
        ),
    ):
        rules = [
            row
            for row in store.rows(parent)
            if row[id_key] != UNKNOWN_ID
            and row["load_batch_id"] == ctx.initial_batch_id
        ]
        for index, rule in enumerate(rules):
            scope_type, business_id = _scope(index, categories, shops)
            writer.add(
                scope_table,
                {
                    sk_key: rule[sk_key],
                    id_key: rule[id_key],
                    "scope_type": scope_type,
                    "scope_business_id": business_id,
                    "is_excluded": 0,
                    "load_batch_id": ctx.initial_batch_id,
                },
            )
    writer.flush_all()


def _scope(
    index: int,
    categories: list[dict],
    shops: list[dict],
) -> tuple[str, str]:
    if index % 5 in {0, 3}:
        return "ALL", "*"
    if index % 5 == 1:
        return "SHOP", str(shops[index % len(shops)]["shop_id"])
    return "CATEGORY", str(categories[index % len(categories)]["category_id"])
