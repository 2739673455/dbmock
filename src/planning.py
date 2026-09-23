"""生成完整月度计划，发布截止时间不参与业务随机数和计划计算。"""

from __future__ import annotations

import copy
import logging
from time import perf_counter
from datetime import timedelta

from .batches import behavior, commerce, dimensions, marketing, products
from .plan_store import PlanStore
from .reference import load_reference_data
from .settings import RunContext
from .support import TableWriter, start_of_day
from .timeline import BusinessState, day_targets, month_periods


logger = logging.getLogger(__name__)


def build_plan(
    ctx: RunContext, before_dimensions: dict, before_state: BusinessState
) -> dict:
    started = perf_counter()
    month_label = ctx.gen.start_date.strftime("%Y-%m")
    logger.info("month=%s 月度数据生成开始", month_label)
    store = PlanStore(start_of_day(ctx.gen.start_date), before_dimensions)
    state = copy.deepcopy(before_state)
    dimensions.run(ctx, store)
    stage_started = perf_counter()
    selections = products.run_dimensions(ctx, store)
    logger.info(
        "month=%s 商品数据生成完成 selected_spus=%s elapsed=%.2fs",
        month_label,
        len(selections),
        perf_counter() - stage_started,
    )
    stage_started = perf_counter()
    marketing.run(ctx, store)
    logger.info(
        "month=%s 营销数据生成完成 promotions=%s coupons=%s elapsed=%.2fs",
        month_label,
        ctx.gen.promotion_count,
        ctx.gen.coupon_count,
        perf_counter() - stage_started,
    )
    refs = load_reference_data(ctx, store)
    period = month_periods(ctx.gen.start_date, ctx.gen.end_date)[0]
    targets = [
        day_targets(total, period, ctx.gen.start_date, kind)
        for total, kind in (
            (ctx.gen.page_view_count, "traffic"),
            (ctx.gen.search_count, "search"),
            (ctx.gen.order_detail_count, "order"),
        )
    ]
    writer = TableWriter(
        store, ctx.gen.batch_size, ctx.gen.dataset_start, ctx.data_end_time
    )
    batch_id = ctx.period_batch_id(period.key)
    day = period.start_date
    while day <= period.end_date:
        products.generate_price_events(ctx, refs, writer, day, batch_id)
        intents = behavior.generate_day(
            ctx,
            refs,
            state,
            writer,
            day,
            batch_id,
            *(target[day] for target in targets),
        )
        commerce.generate_day(ctx, refs, state, writer, day, batch_id, intents)
        day += timedelta(days=1)
    logger.info(
        "month=%s 业务事件生成完成 pending_facts=%s elapsed=%.2fs",
        month_label,
        len(state.pending_facts),
        perf_counter() - started,
    )
    dimensions.generate_user_lifecycle_updates(
        ctx, refs, state, writer, ctx.data_end_time, batch_id
    )
    writer.flush_all()
    logger.info(
        "month=%s 月度数据生成完成 rows=%s elapsed=%.2fs",
        month_label,
        sum(len(rows) for rows in store.journal.values()),
        perf_counter() - started,
    )
    # 只需保留未来库存事件的序号；已经结束的月份不会再分配事件 ID。
    state.inventory_event_sequences = {
        day: count
        for day, count in state.inventory_event_sequences.items()
        if day > ctx.gen.end_date
    }
    return {
        "before_dimensions": before_dimensions,
        "journal": dict(store.journal),
        "state": state,
        "selections": selections,
    }


def restore_plan(month, plan: dict) -> PlanStore:
    """重建用于发布的只读维度索引，业务行由 visible() 复制后再修改。"""
    store = PlanStore(start_of_day(month), {})
    for table, rows in plan["before_dimensions"].items():
        store.dimensions[table].update((store.key(table, row), row) for row in rows)
    for table, rows in plan["journal"].items():
        if not table.startswith("dwd_"):
            # 顺序应用同一主键的变更，得到月末维度及营销规则索引。
            store.dimensions[table].update((store.key(table, row), row) for row in rows)
    store.journal.update(plan["journal"])
    return store
