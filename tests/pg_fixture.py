"""Opt-in tests: isolate every case in a UUID-named database on local PostgreSQL."""

import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from scripts.init_db import initialize
from src.settings import PostgresConfig, RunContext


class DisposableDatabase(unittest.TestCase):
    def setUp(self):
        dsn = os.environ.get("DBMOCK_TEST_ADMIN_DSN")
        if not dsn:
            self.skipTest("set DBMOCK_TEST_ADMIN_DSN for disposable local PostgreSQL")
        options = conninfo_to_dict(dsn)
        host = str(options.get("host") or "")
        if not (host in {"localhost", "127.0.0.1", "::1"} or host.startswith("/")):
            self.fail("test DSN must explicitly identify a local socket/loopback host")
        dbname = f"dbmock_test_{uuid4().hex}"
        stack = self.enterContext(ExitStack())
        admin = stack.enter_context(psycopg.connect(dsn, autocommit=True))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        stack.callback(
            admin.execute, sql.SQL("DROP DATABASE {}").format(sql.Identifier(dbname))
        )
        self.env = {
            "DB_HOST": host,
            "DB_PORT": str(admin.info.port),
            "DB_NAME": dbname,
            "DB_USER": admin.info.user,
            "DB_PASSWORD": str(options.get("password") or ""),
        }
        stack.enter_context(patch.dict(os.environ, self.env))
        directory = stack.enter_context(tempfile.TemporaryDirectory())
        stack.enter_context(
            patch("src.state_store.STATUS_FILE", Path(directory) / "status.json")
        )
        initialize()
        self.ctx = RunContext(PostgresConfig())
        stack.callback(self.ctx.close)
        stack.enter_context(self.ctx.lock)
