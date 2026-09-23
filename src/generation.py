"""按月计划、增量发布、校验提交与整月回滚。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from datetime import datetime, time

from sqlalchemy import text

from . import quality, support
from .batches import snapshots
from .planning import build_plan, restore_plan
from .plan_store import table_keys
from .settings import GenerateConfig, month_end, next_month, read_config, sample_scales
from .state_store import StateStore, check_seeds, seed_fingerprints, write_status
from .timeline import BusinessState, MonthPeriod

logger = logging.getLogger(__name__)


def keyed(rows, keys):
    return {tuple(row[key] for key in keys): row for row in rows}


def expected_counts(plan, visible, cutoff):
    result = dict(plan["before_counts"])
    keys = table_keys()
    for table in keys:
        rows = keyed(visible.get(table, []), keys[table])
        if table.startswith("dwd_"):
            result[table] = result.get(table, 0) + len(rows)
        else:
            existing = keyed(plan["before_dimensions"].get(table, []), keys[table])
            existing.update(rows)
            result[table] = len(existing)
    skus = keyed(
        plan["before_dimensions"].get("dim_sku_info_zip", []), keys["dim_sku_info_zip"]
    )
    skus.update(keyed(visible.get("dim_sku_info_zip", []), keys["dim_sku_info_zip"]))
    result["dwd_inventory_daily_snapshot_df"] = plan["before_counts"].get(
        "dwd_inventory_daily_snapshot_df", 0
    ) + sum(
        (cutoff.date() - max(plan["month"], row["effective_start_time"].date())).days
        + 1
        for row in skus.values()
        if row["sku_id"] != support.UNKNOWN_ID
    )
    return result


def _load_rows(ctx, guard, table, rows):
    # 相同列集合一起装载；缺失可选字段交给表默认值处理。
    groups = {}
    for row in rows:
        groups.setdefault(tuple(sorted(row)), []).append(row)
    for group in groups.values():
        for index in range(0, len(group), ctx.gen.batch_size):
            guard()
            ctx.loader.load(table, group[index : index + ctx.gen.batch_size])


def publish(ctx, guard, plan, old_cutoff, cutoff):
    started = perf_counter()
    month_label = plan["month"].strftime("%Y-%m")
    row_count = 0
    store = restore_plan(plan["month"], plan)
    visible = store.visible(cutoff)
    previous = store.visible(old_cutoff) if old_cutoff is not None else {}
    with ThreadPoolExecutor(
        max_workers=ctx.gen.stream_load_workers, thread_name_prefix="dbmock-publish"
    ) as executor:
        futures = []
        for table, keys in table_keys().items():
            old = keyed(previous.get(table, []), keys)
            current = keyed(visible.get(table, []), keys)
            changed = [row for key, row in current.items() if old.get(key) != row]
            if changed:
                row_count += len(changed)
                futures.append(executor.submit(_load_rows, ctx, guard, table, changed))
        # 离开线程池前等待所有写入结束，任何失败都交由整月回滚处理。
        for future in futures:
            future.result()
    logger.info(
        "month=%s 月度数据写入完成 rows=%s elapsed=%.2fs",
        month_label,
        row_count,
        perf_counter() - started,
    )
    ctx.validation_cutoff = cutoff
    start = old_cutoff.date() if old_cutoff is not None else plan["month"]
    period = MonthPeriod(f"{plan['month']:%Y-%m}", start, cutoff.date())
    guard()
    snapshots.materialize_period(ctx, period, ctx.period_batch_id(period.key))
    return expected_counts(plan, visible, cutoff)


def rollback(ctx, store, dataset, plan):
    month = plan["month"]
    logger.warning("month=%s 开始整月回滚", month.strftime("%Y-%m"))
    dataset["dirty"] = True
    store.put("dataset", dataset)
    store.wait_for_loads()
    for table in table_keys():
        store.guard()
        with ctx.engine.connect() as conn:
            if table.startswith("dwd_"):
                conn.execute(
                    text(
                        f"DELETE FROM `{table}` WHERE biz_date >= :start AND biz_date <= :end"
                    ),
                    {"start": month, "end": month_end(month)},
                )
            else:
                conn.execute(text(f"TRUNCATE TABLE `{table}`"))
        if not table.startswith("dwd_"):
            _load_rows(
                ctx, store.guard, table, plan["before_dimensions"].get(table, [])
            )
    dataset["cutoff"] = plan["before_cutoff"]
    dataset["counts"] = plan["before_counts"]
    dataset["dirty"] = False
    store.put("dataset", dataset)
    logger.warning(
        "month=%s 整月回滚完成；配置和上架计划已保留", month.strftime("%Y-%m")
    )


def _configure(ctx, dataset, plan):
    ctx.gen = GenerateConfig.from_plan(
        plan["month"],
        dataset["start"],
        plan["config"],
        plan["scales"],
        dataset["smoke"],
    )


def run(ctx, args, guard):
    store = StateStore(ctx, guard)
    reflected = support.reflect_tables(ctx.engine)
    if "dbmock_documents" not in reflected:
        raise ValueError("缺少月度生成管理表，请初始化数据库；旧版数据不支持直接追加")
    tables = {name: table for name, table in reflected.items() if name in table_keys()}
    if set(tables) != set(table_keys()):
        raise ValueError("业务表缺失，请初始化数据库")
    dataset: dict = store.get("dataset")
    write_status(ctx, dataset)
    if args.validate_only:
        if dataset is None or dataset["cutoff"] is None:
            raise ValueError("没有已提交的生成进度可供校验")
        if dataset["dirty"]:
            raise ValueError("存在中断月份，请先运行生成命令完成回滚")
        month = dataset["cutoff"].date().replace(day=1)
        plan = store.get(f"plan:{month:%Y-%m}")
        _configure(ctx, dataset, plan)
        ctx.expected_counts = dataset["counts"]
        ctx.validation_cutoff = dataset["cutoff"]
        quality.validate_database(ctx, tables)
        return
    current_month = ctx.as_of_time.date().replace(day=1)
    if args.end_month > current_month:
        raise ValueError("截止月份不能超过当前月份")
    fingerprints = seed_fingerprints()
    if dataset is None:
        if args.start_month is None:
            raise ValueError("首次生成必须提供 --start-month")
        if args.start_month > args.end_month:
            raise ValueError("开始月份不能晚于截止月份")
        with ctx.engine.connect() as conn:
            support.assert_empty(conn, tables)
        dataset = {
            "start": args.start_month,
            "smoke": bool(args.smoke),
            "seeds": fingerprints,
            "completed": None,
            "active": None,
            "cutoff": None,
            "dirty": False,
            "counts": {},
            "checkpoint": None,
        }
    else:
        check_seeds(dataset["seeds"], fingerprints)
        if args.smoke is not None and args.smoke != dataset["smoke"]:
            raise ValueError("同一数据集不能切换 smoke 模式")
        if args.start_month is not None:
            if dataset["cutoff"] is not None or args.start_month != dataset["start"]:
                raise ValueError("已有生成进度，追加时只传 --end-month")
        if dataset["completed"] is not None and args.end_month <= dataset["completed"]:
            raise ValueError("截止月份已完整生成，禁止重复生成")
    month = dataset["active"] or (
        next_month(dataset["completed"]) if dataset["completed"] else dataset["start"]
    )
    if args.end_month < month:
        raise ValueError("截止月份早于待生成月份")
    while month <= args.end_month:
        month_started = perf_counter()
        logger.info(
            "month=%s 月份生成开始 mode=%s",
            month.strftime("%Y-%m"),
            "smoke" if dataset["smoke"] else "full",
        )
        guard()
        check_seeds(dataset["seeds"], seed_fingerprints())
        cutoff = min(ctx.as_of_time, datetime.combine(month_end(month), time.max))
        complete = cutoff == datetime.combine(month_end(month), time.max)
        key = f"plan:{month:%Y-%m}"
        plan = store.get(key)
        new_plan = plan is None
        if new_plan:
            config = read_config()
            initial = (
                store.get(f"plan:{dataset['start']:%Y-%m}")
                if month != dataset["start"]
                else None
            )
            scales = sample_scales(
                config, month, dataset["smoke"], initial["scales"] if initial else None
            )
            logger.info(
                "month=%s 月度规模抽样完成 scales=%s", month.strftime("%Y-%m"), scales
            )
            plan = {
                "month": month,
                "config": config,
                "scales": scales,
                "before_checkpoint": dataset["checkpoint"],
                "before_counts": dict(dataset["counts"]),
                "before_cutoff": dataset["cutoff"],
            }
        _configure(ctx, dataset, plan)
        if "journal" not in plan:
            if plan["before_checkpoint"]:
                restore_started = perf_counter()
                baseline = store.get(plan["before_checkpoint"])
                if baseline is None:
                    raise ValueError("缺少月初检查点，无法恢复或重新生成该月数据")
                logger.info(
                    "month=%s 上月状态恢复完成 checkpoint=%s elapsed=%.2fs",
                    month.strftime("%Y-%m"),
                    plan["before_checkpoint"],
                    perf_counter() - restore_started,
                )
            else:
                baseline = None
                logger.info("month=%s 首次生成，无上月检查点", month.strftime("%Y-%m"))
            before_dimensions = baseline["dimensions"] if baseline else {}
            before_state = baseline["state"] if baseline else BusinessState()
            plan["before_dimensions"] = before_dimensions
            if dataset["dirty"]:
                rollback(ctx, store, dataset, plan)
            if not new_plan:
                logger.info(
                    "month=%s 按冻结配置重新生成数据 scales=%s",
                    month.strftime("%Y-%m"),
                    plan["scales"],
                )
            generated = build_plan(ctx, before_dimensions, before_state)
            if not new_plan and generated["selections"] != plan["selections"]:
                raise ValueError("重新生成的商品上架安排与冻结记录不一致，拒绝写入")
            plan.update(generated)
            # 先完成生成约束检查，再冻结配置；完整月份不保存整月明细。
            if new_plan or not complete:
                save_started = perf_counter()
                store.put("dataset", dataset)
                dataset_seconds = perf_counter() - save_started
                if complete:
                    store.put(
                        key,
                        {
                            name: plan[name]
                            for name in (
                                "month",
                                "config",
                                "scales",
                                "selections",
                                "before_checkpoint",
                                "before_counts",
                                "before_cutoff",
                            )
                        },
                    )
                    logger.info(
                        "month=%s 月度恢复信息就绪 elapsed=%.2fs",
                        month.strftime("%Y-%m"),
                        perf_counter() - save_started,
                    )
                else:
                    saved = store.put(key, plan)
                    logger.info(
                        "month=%s 月度数据已暂存 compressed_bytes=%s chunks=%s "
                        "dataset_save=%.2fs serialize=%.2fs type_convert=%.2fs "
                        "json_encode=%.2fs serialize_release=%.2fs compress=%.2fs "
                        "encode=%.2fs write=%.2fs cleanup=%.2fs elapsed=%.2fs",
                        month.strftime("%Y-%m"),
                        saved["compressed_bytes"],
                        saved["chunks"],
                        dataset_seconds,
                        saved["serialize_seconds"],
                        saved["type_convert_seconds"],
                        saved["json_encode_seconds"],
                        saved["serialize_release_seconds"],
                        saved["compress_seconds"],
                        saved["encode_seconds"],
                        saved["write_seconds"],
                        saved["cleanup_seconds"],
                        perf_counter() - save_started,
                    )
        else:
            logger.info(
                "month=%s 沿用已暂存月度数据 scales=%s",
                month.strftime("%Y-%m"),
                plan["scales"],
            )
            if dataset["dirty"]:
                rollback(ctx, store, dataset, plan)
        old_cutoff = (
            dataset["cutoff"]
            if dataset["cutoff"] and dataset["cutoff"].date().replace(day=1) == month
            else None
        )
        if old_cutoff is not None and cutoff <= old_cutoff:
            logger.info(
                "month=%s 截止时刻未推进，没有新增数据",
                month.strftime("%Y-%m"),
            )
            return
        seed_started = perf_counter()
        check_seeds(dataset["seeds"], seed_fingerprints())
        logger.info(
            "month=%s 种子数据校验通过 elapsed=%.2fs",
            month.strftime("%Y-%m"),
            perf_counter() - seed_started,
        )
        dataset["active"] = month
        dataset["dirty"] = True
        store.put("dataset", dataset)
        try:
            ctx.expected_counts = publish(ctx, guard, plan, old_cutoff, cutoff)
            quality.validate_database(ctx, tables)
            checkpoint_key = f"checkpoint:{month:%Y-%m}"
            if complete:
                store.put(
                    checkpoint_key,
                    {
                        "dimensions": restore_plan(month, plan).snapshot(),
                        "state": plan["state"],
                    },
                )
            committed: dict = dict(
                dataset,
                counts=ctx.expected_counts,
                cutoff=cutoff,
                dirty=False,
                active=None if complete else month,
                completed=month if complete else dataset["completed"],
                checkpoint=checkpoint_key if complete else dataset["checkpoint"],
            )
            store.put("dataset", committed)
        except BaseException:
            # 网络异常可能发生在进度已经提交之后；先确认提交结果，不能误删已完成月份。
            store.wait_for_loads()
            latest = store.get("dataset")
            if latest and not latest["dirty"] and latest["cutoff"] == cutoff:
                logger.exception(
                    "month=%s 进度已经提交，保留已生成数据", month.strftime("%Y-%m")
                )
                raise
            logger.exception(
                "month=%s 月份生成失败，准备整月回滚", month.strftime("%Y-%m")
            )
            rollback(ctx, store, dataset, plan)
            raise
        dataset = committed
        logger.info(
            "month=%s 月份进度已提交 cutoff=%s complete=%s elapsed=%.2fs",
            month.strftime("%Y-%m"),
            cutoff,
            complete,
            perf_counter() - month_started,
        )
        if complete:
            # 已完成月份只保留配置和上架名单，完整业务行无需在管理表中重复保留。
            store.put(
                key,
                {
                    name: plan[name]
                    for name in ("month", "config", "scales", "selections")
                },
            )
        month = next_month(month)
