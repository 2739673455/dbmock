"""PostgreSQL 15+ COPY → staging → MERGE, one transaction per batch."""

from __future__ import annotations

import json
from functools import partial
from collections.abc import Mapping, Sequence
from typing import Any

from psycopg import sql
from psycopg.types.json import Json

from .plan_store import table_keys


_json_dump = partial(json.dumps, ensure_ascii=False, default=str, separators=(",", ":"))


class PostgresLoader:
    def __init__(self, write_transaction):
        self.write_transaction = write_transaction
        self.keys = table_keys() | {"dbmock_documents": ("document_key", "chunk_no")}

    def load(self, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if rows:
            with self.write_transaction() as conn:
                self.load_in(conn, table, rows)

    def load_in(self, conn, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        """Each input has a unique plan key and one column shape. Missing ≠ NULL."""
        if not rows:
            return
        keys = self.keys[table]
        columns = tuple(rows[0])
        if not set(keys).issubset(columns):
            raise ValueError(f"Missing primary key for {table}")
        names = sql.SQL(", ").join(map(sql.Identifier, columns))
        target = sql.Identifier("public", table)
        # Transaction-local staging has types, but not target defaults or NOT NULL
        # constraints: patches to existing records need not repeat required fields.
        stage = sql.Identifier("pg_temp", "dbmock_stage")
        raw = conn.connection.driver_connection
        with raw.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE TEMP TABLE dbmock_stage ON COMMIT DROP AS "
                    "SELECT {} FROM {} WITH NO DATA"
                ).format(names, target)
            )
            with cur.copy(
                sql.SQL("COPY {} ({}) FROM STDIN").format(stage, names)
            ) as cp:
                for row in rows:
                    if row.keys() != rows[0].keys():
                        raise ValueError(f"Mixed row shapes for {table}")
                    cp.write_row(
                        tuple(
                            Json(v, dumps=_json_dump)
                            if isinstance(v, (dict, list))
                            else v
                            for v in (row[c] for c in columns)
                        )
                    )
            equal = sql.SQL(" AND ").join(
                sql.SQL("t.{0} = s.{0}").format(sql.Identifier(k)) for k in keys
            )
            updates = sql.SQL(", ").join(
                sql.SQL("{0} = s.{0}").format(sql.Identifier(c))
                for c in columns
                if c not in keys
            )
            matched = (
                sql.SQL("UPDATE SET {}").format(updates)
                if any(c not in keys for c in columns)
                else sql.SQL("DO NOTHING")
            )
            values = sql.SQL(", ").join(sql.Identifier("s", c) for c in columns)
            cur.execute(
                sql.SQL(
                    "MERGE INTO {} AS t USING {} AS s ON {} "
                    "WHEN MATCHED THEN {} "
                    "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})"
                ).format(target, stage, equal, matched, names, values)
            )
