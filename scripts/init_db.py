"""在预先创建的 PostgreSQL 数据库中初始化 dbmock 表（不删除任何数据库）。"""

import logging
import os
from pathlib import Path
from typing import LiteralString, cast

import dotenv
import psycopg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT_DIR / "scripts" / "sql" / "ecommerce.sql"


def initialize() -> None:
    dotenv.load_dotenv(ROOT_DIR / ".env")
    db_name = os.environ["DB_NAME"]
    schema = SCHEMA_FILE.read_text(encoding="utf-8")

    # 连接指定数据库。CREATE TABLE、约束和注释要么全部成功，要么一起回滚。
    with psycopg.connect(
        dbname=db_name,
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    ) as conn:
        conn.execute("SET LOCAL search_path TO public")
        existing = conn.execute(
            """
            SELECT c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
            LIMIT 1
            """
        ).fetchone()
        if existing:
            raise ValueError(
                f"数据库 {db_name} 的 public schema 已有对象 {existing[0]!r}；"
                "请使用新建的空数据库，现有数据未修改"
            )
        conn.execute(cast(LiteralString, schema))

    logger.info("PostgreSQL 数据库 %s 建表成功", db_name)


if __name__ == "__main__":
    try:
        initialize()
    except (KeyError, ValueError, psycopg.Error) as exc:
        raise SystemExit(f"初始化失败: {exc}") from None
