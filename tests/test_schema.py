"""PostgreSQL schema and non-destructive bootstrap checks."""

import os
from unittest.mock import patch

from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from scripts.init_db import initialize
from tests.pg_fixture import DisposableDatabase


class SchemaTest(DisposableDatabase):
    def test_preserves_tables_keys_and_column_metadata(self):
        inspector = inspect(self.ctx.engine)
        tables = inspector.get_table_names(schema="public")
        self.assertEqual(len(tables), 46)
        self.assertIn("dbmock_documents", tables)
        self.assertEqual(
            inspector.get_pk_constraint(
                "dwd_inventory_daily_snapshot_df", schema="public"
            )["constrained_columns"],
            ["snapshot_date_key", "warehouse_sk", "sku_sk"],
        )
        self.assertEqual(
            inspector.get_pk_constraint("dbmock_documents", schema="public")[
                "constrained_columns"
            ],
            ["document_key", "chunk_no"],
        )
        with self.ctx.engine.connect() as conn:
            sku_columns = {
                r.column_name: r
                for r in conn.execute(
                    text(
                        """
                        SELECT column_name, data_type, is_nullable, identity_generation,
                               datetime_precision
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'dim_sku_info_zip'
                        """
                    )
                )
            }
            amount_type = conn.execute(
                text(
                    """
                    SELECT data_type, numeric_precision, numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'dwd_trade_order_detail_di'
                      AND column_name = 'list_amount'
                    """
                )
            ).one()
        self.assertEqual(tuple(amount_type), ("numeric", 18, 2))
        self.assertEqual(sku_columns["sku_sk"].identity_generation, "BY DEFAULT")
        self.assertEqual(sku_columns["sku_sk"].is_nullable, "NO")
        self.assertEqual(sku_columns["sku_specs_json"].data_type, "json")
        self.assertEqual(sku_columns["sku_specs_json"].is_nullable, "YES")
        self.assertEqual(
            sku_columns["effective_start_time"].data_type, "timestamp without time zone"
        )
        self.assertEqual(sku_columns["effective_start_time"].datetime_precision, 6)
        self.assertEqual(
            inspector.get_table_comment("dim_date", schema="public")["text"],
            "公共日期维度",
        )
        columns = {
            c["name"]: c for c in inspector.get_columns("dim_date", schema="public")
        }
        self.assertEqual(columns["date_key"].get("comment"), "日期键，格式YYYYMMDD")

    def test_defaults_explicit_identity_and_duplicate_rejection(self):
        with self.ctx.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO dim_channel_info (
                        channel_sk, channel_code, channel_name, attribute_hash, load_batch_id,
                        dw_load_time, dw_update_time
                    ) VALUES (
                        -1, 'unknown', '未知', repeat('0', 64), 'schema-test',
                        TIMESTAMP '2026-01-01 00:00:01.123456',
                        TIMESTAMP '2026-01-01 00:00:01.123456'
                    )
                    RETURNING channel_sk, channel_status, is_deleted,
                              channel_group, dw_load_time
                    """
                )
            ).one()
            self.assertEqual(tuple(row[:4]), (-1, 1, 0, None))
            self.assertEqual(row.dw_load_time.microsecond, 123456)
            self.assertIsNone(row.dw_load_time.tzinfo)
            with self.assertRaises(IntegrityError), conn.begin_nested():
                conn.execute(
                    text(
                        """
                        INSERT INTO dim_channel_info
                        SELECT * FROM dim_channel_info WHERE channel_sk = -1
                        """
                    )
                )

            with self.assertRaises(IntegrityError), conn.begin_nested():
                conn.execute(
                    text(
                        "UPDATE dim_channel_info SET channel_name = NULL "
                        "WHERE channel_sk = -1"
                    )
                )

            generated_key = conn.execute(
                text(
                    """
                    INSERT INTO dim_channel_info (
                        channel_code, channel_name, attribute_hash, load_batch_id,
                        dw_load_time, dw_update_time
                    ) VALUES (
                        'example', '示例', repeat('0', 64), 'schema-test',
                        TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-01'
                    )
                    RETURNING channel_sk
                    """
                )
            ).scalar_one()
            self.assertGreater(generated_key, 0)

    def test_bootstrap_refuses_existing_schema_without_destroying_content(self):
        with self.ctx.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO dbmock_documents VALUES (
                        'init-safety', 0, 1, 'revision', 'digest', 'marker'
                    )
                    """
                )
            )
        config = self.ctx.db
        env = {
            "DB_HOST": config.host,
            "DB_PORT": str(config.port),
            "DB_USER": config.user,
            "DB_PASSWORD": config.password,
            "DB_NAME": config.database,
        }
        with (
            patch.dict(os.environ, env),
            self.assertRaisesRegex(ValueError, "现有数据未修改"),
        ):
            initialize()
        with self.ctx.engine.connect() as conn:
            payload = conn.execute(
                text(
                    """
                    SELECT payload FROM dbmock_documents
                    WHERE document_key = 'init-safety'
                    """
                )
            ).scalar_one()
        self.assertEqual(payload, "marker")
