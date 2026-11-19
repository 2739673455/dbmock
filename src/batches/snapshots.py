"""由库存变更事件批量生成月内库存日快照"""

from __future__ import annotations

from time import perf_counter

import logging

from sqlalchemy import text

from ..settings import RunContext
from ..timeline import MonthPeriod

logger = logging.getLogger(__name__)


def materialize_period(
    ctx: RunContext,
    period: MonthPeriod,
    batch_id: str,
) -> int:
    """在 PostgreSQL 内按月集合计算库存日快照"""
    started = perf_counter()
    parameters = {
        "period_start": period.start_date,
        "period_end": period.end_date,
        "as_of_time": ctx.validation_cutoff or ctx.as_of_time,
        "batch_id": batch_id,
    }
    with ctx.write_transaction() as conn:
        conn.execute(text(_INSERT_SQL), parameters)
        count = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM dwd_inventory_daily_snapshot_df
                    WHERE biz_date BETWEEN :period_start AND :period_end
                    """
                ),
                parameters,
            ).scalar_one()
        )
    logger.info(
        "month=%s 库存快照写入完成 rows=%s elapsed=%.2fs",
        period.key,
        count,
        perf_counter() - started,
    )
    return count


_INSERT_SQL = """
INSERT INTO dwd_inventory_daily_snapshot_df (
    snapshot_date_key,
    warehouse_sk,
    sku_sk,
    warehouse_id,
    sku_id,
    spu_sk,
    spu_id,
    shop_sk,
    shop_id,
    on_hand_qty,
    reserved_qty,
    available_qty,
    in_transit_qty,
    unit_cost,
    inventory_cost_amount,
    currency_code,
    snapshot_time,
    biz_date,
    source_record_id,
    load_batch_id,
    dw_load_time
)
WITH
sku_base AS (
    SELECT warehouse_sk,
           warehouse_id,
           sku_sk,
           sku_id,
           spu_sk,
           spu_id,
           shop_sk,
           shop_id,
           MIN(biz_date) AS listing_date
    FROM dwd_inventory_change_di
    WHERE change_type = 'INITIAL_STOCK'
      AND biz_date <= :period_end
    GROUP BY warehouse_sk,
             warehouse_id,
             sku_sk,
             sku_id,
             spu_sk,
             spu_id,
             shop_sk,
             shop_id
),
daily_ending AS (
    SELECT DISTINCT ON (warehouse_id, sku_id, biz_date)
           warehouse_id,
           sku_id,
           biz_date,
           after_on_hand_qty AS on_hand_qty,
           after_reserved_qty AS reserved_qty,
           after_in_transit_qty AS in_transit_qty
    FROM dwd_inventory_change_di
    WHERE biz_date BETWEEN :period_start AND :period_end
    ORDER BY warehouse_id, sku_id, biz_date, inventory_change_id DESC
),
opening AS (
    SELECT DISTINCT ON (warehouse_id, sku_id)
           warehouse_id,
           sku_id,
           after_on_hand_qty AS on_hand_qty,
           after_reserved_qty AS reserved_qty,
           after_in_transit_qty AS in_transit_qty
    FROM dwd_inventory_change_di
    WHERE biz_date < :period_start
    ORDER BY warehouse_id, sku_id, inventory_change_id DESC
),
-- MAX_BY 在成本为 NULL 时忽略该事件；数量由同组最新事件决定。
daily_cost AS (
    SELECT DISTINCT ON (warehouse_id, sku_id, biz_date)
           warehouse_id, sku_id, biz_date, unit_cost
    FROM dwd_inventory_change_di
    WHERE unit_cost IS NOT NULL AND biz_date BETWEEN :period_start AND :period_end
    ORDER BY warehouse_id, sku_id, biz_date, inventory_change_id DESC
),
opening_cost AS (
    SELECT DISTINCT ON (warehouse_id, sku_id)
           warehouse_id, sku_id, unit_cost
    FROM dwd_inventory_change_di
    WHERE unit_cost IS NOT NULL AND biz_date < :period_start
    ORDER BY warehouse_id, sku_id, inventory_change_id DESC
),
calendar AS (
    SELECT b.*,
           d.full_date,
           d.date_key,
           GREATEST(b.listing_date, CAST(:period_start AS DATE)) AS first_date
    FROM sku_base b
    JOIN dim_date d
      ON d.full_date BETWEEN GREATEST(
             b.listing_date,
             CAST(:period_start AS DATE)
         ) AND CAST(:period_end AS DATE)
),
seeded AS (
    SELECT c.*,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.on_hand_qty
               WHEN c.full_date = c.first_date THEN o.on_hand_qty
           END AS on_hand_seed,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.reserved_qty
               WHEN c.full_date = c.first_date THEN o.reserved_qty
           END AS reserved_seed,
           CASE
               WHEN e.biz_date IS NOT NULL THEN ec.unit_cost
               WHEN c.full_date = c.first_date THEN oc.unit_cost
           END AS unit_cost_seed,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.in_transit_qty
               WHEN c.full_date = c.first_date THEN o.in_transit_qty
           END AS in_transit_seed
    FROM calendar c
    LEFT JOIN daily_ending e
      ON e.warehouse_id = c.warehouse_id
     AND e.sku_id = c.sku_id
     AND e.biz_date = c.full_date
    LEFT JOIN opening o
      ON o.warehouse_id = c.warehouse_id
     AND o.sku_id = c.sku_id
    LEFT JOIN daily_cost ec
      ON ec.warehouse_id = c.warehouse_id
     AND ec.sku_id = c.sku_id
     AND ec.biz_date = c.full_date
    LEFT JOIN opening_cost oc
      ON oc.warehouse_id = c.warehouse_id
     AND oc.sku_id = c.sku_id
),
-- PostgreSQL LAST_VALUE 不支持 IGNORE NULLS。每个非空值开始一个新组，
-- 组内唯一非空值就是应向后填充的种子；成本独立分组以保留可空语义。
seed_groups AS (
    SELECT *,
           COUNT(on_hand_seed) OVER w AS qty_group,
           COUNT(unit_cost_seed) OVER w AS cost_group
    FROM seeded
    WINDOW w AS (
        PARTITION BY warehouse_id, sku_id
        ORDER BY full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),
computed AS (
    SELECT *,
           MAX(on_hand_seed) OVER qty AS on_hand_qty,
           MAX(reserved_seed) OVER qty AS reserved_qty,
           MAX(unit_cost_seed) OVER cost AS unit_cost,
           MAX(in_transit_seed) OVER qty AS in_transit_qty
    FROM seed_groups
    WINDOW qty AS (PARTITION BY warehouse_id, sku_id, qty_group),
           cost AS (PARTITION BY warehouse_id, sku_id, cost_group)
),
snapshot_rows AS (
    SELECT date_key AS snapshot_date_key,
           warehouse_sk,
           sku_sk,
           warehouse_id,
           sku_id,
           spu_sk,
           spu_id,
           shop_sk,
           shop_id,
           on_hand_qty,
           reserved_qty,
           on_hand_qty - reserved_qty AS available_qty,
           in_transit_qty,
           unit_cost,
           ROUND(unit_cost * on_hand_qty, 4) AS inventory_cost_amount,
           'CNY' AS currency_code,
           LEAST(
               full_date + TIME '23:59:59.999999',
               CAST(:as_of_time AS TIMESTAMP(6))
           ) AS snapshot_time,
           full_date AS biz_date,
           CONCAT(
               'inventory-snapshot:',
               CAST(warehouse_id AS TEXT),
               ':',
               CAST(sku_id AS TEXT),
               ':',
               CAST(date_key AS TEXT)
           ) AS source_record_id,
           :batch_id AS load_batch_id
    FROM computed
)
SELECT snapshot_date_key,
       warehouse_sk,
       sku_sk,
       warehouse_id,
       sku_id,
       spu_sk,
       spu_id,
       shop_sk,
       shop_id,
       on_hand_qty,
       reserved_qty,
       available_qty,
       in_transit_qty,
       unit_cost,
       inventory_cost_amount,
       currency_code,
       snapshot_time,
       biz_date,
       source_record_id,
       load_batch_id,
       LEAST(
           snapshot_time + (60 + MOD(sku_id, 240)) * INTERVAL '1 minute',
           CAST(:as_of_time AS TIMESTAMP(6))
       ) AS dw_load_time
FROM snapshot_rows
WHERE TRUE
ON CONFLICT (snapshot_date_key, warehouse_sk, sku_sk) DO UPDATE SET
    warehouse_id = EXCLUDED.warehouse_id,
    sku_id = EXCLUDED.sku_id,
    spu_sk = EXCLUDED.spu_sk,
    spu_id = EXCLUDED.spu_id,
    shop_sk = EXCLUDED.shop_sk,
    shop_id = EXCLUDED.shop_id,
    on_hand_qty = EXCLUDED.on_hand_qty,
    reserved_qty = EXCLUDED.reserved_qty,
    available_qty = EXCLUDED.available_qty,
    in_transit_qty = EXCLUDED.in_transit_qty,
    unit_cost = EXCLUDED.unit_cost,
    inventory_cost_amount = EXCLUDED.inventory_cost_amount,
    currency_code = EXCLUDED.currency_code,
    snapshot_time = EXCLUDED.snapshot_time,
    biz_date = EXCLUDED.biz_date,
    source_record_id = EXCLUDED.source_record_id,
    load_batch_id = EXCLUDED.load_batch_id,
    dw_load_time = EXCLUDED.dw_load_time
"""
