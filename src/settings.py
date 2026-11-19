"""按月生成配置、可复现规模抽样和运行上下文。"""

from __future__ import annotations

import calendar
import math
import os
import random
import re
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import dotenv
from sqlalchemy import URL, Engine, create_engine

if TYPE_CHECKING:
    from .database import PostgresLoader
    from .state_store import GenerationLock

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
# 规模为正态分布均值；smoke 使用相同日期范围，抽样结果缩放到 5%。
# variation 可用同名规模项覆盖 default；0 表示固定规模。
GENERATION_CONFIG = {
    "scale": {
        "initial_users": 6000,  # 起始月份月初已有用户数，仅首次生成
        "new_users": 1000,  # 每月新注册用户数
        "initial_spus": 1000,  # 初始上架 SPU 数，包含关联 SKU，仅首次生成
        "new_spus": 150,  # 每月新增上架 SPU 数，受剩余种子商品数量限制
        "order_details": 5000,  # 每月订单明细行数，不是订单数
        "page_views": 200000,  # 每月页面访问事件数
        "searches": 30000,  # 每月搜索事件数
        "promotions": 2,  # 每月新增促销规则数
        "coupons": 4,  # 每月新增优惠券模板数，不是领取数量
        "warehouses": 12,  # 初始仓库数，仅首次生成，后续沿用
    },
    "variation": {"default": 0.15},  # 默认标准差为规模基准值的 15%
    "runtime": {
        "batch_size": 250000,  # 每批写入的行数上限，不参与随机抽样
        "publish_workers": 4,  # 并行写入线程数，不参与随机抽样
    },
}
SEED = 42
BUSINESS_TIMEZONE = timezone(timedelta(hours=8), "Asia/Shanghai")
dotenv.load_dotenv(ROOT_DIR / ".env")


def now() -> datetime:
    return datetime.now(tz=BUSINESS_TIMEZONE).replace(tzinfo=None)


def parse_month(value: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError("月份格式必须为 YYYY-MM")
    return date.fromisoformat(value + "-01")


def month_end(month: date) -> date:
    return month.replace(day=calendar.monthrange(month.year, month.month)[1])


def next_month(month: date) -> date:
    return month_end(month) + timedelta(days=1)


@dataclass(slots=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: os.environ["DB_HOST"])
    port: int = field(default_factory=lambda: int(os.environ["DB_PORT"]))
    user: str = field(default_factory=lambda: os.environ["DB_USER"])
    password: str = field(default_factory=lambda: os.environ["DB_PASSWORD"])
    database: str = field(default_factory=lambda: os.environ["DB_NAME"])

    @property
    def sqlalchemy_url(self) -> URL:
        return URL.create(
            "postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )


POSITIVE_SCALES = {
    "initial_users",
    "new_users",
    "initial_spus",
    "warehouses",
    "order_details",
    "page_views",
    "searches",
}


def read_config() -> dict:
    """复制当前生成配置，避免月度快照共享可变对象。"""
    return deepcopy(GENERATION_CONFIG)


def sample_scales(
    config: dict, month: date, smoke: bool, initial: dict[str, int] | None = None
) -> dict[str, int]:
    result = {}
    for name in sorted(config["scale"]):
        if initial is not None and name in {
            "initial_users",
            "initial_spus",
            "warehouses",
        }:
            result[name] = initial[name]
            continue
        mean = config["scale"][name]
        ratio = config["variation"].get(name, config["variation"].get("default", 0.15))
        sigma = mean * ratio
        rng = random.Random(f"{SEED}:{month:%Y-%m}:{name}")
        value = float(mean)
        if sigma:
            while True:
                value = rng.gauss(mean, sigma)
                if mean - 3 * sigma <= value <= mean + 3 * sigma:
                    break
        normal = max(1 if name in POSITIVE_SCALES else 0, math.floor(value + 0.5))
        # 先抽正常规模再缩放；离散且必须存在的对象至少保留一个。
        result[name] = (
            max(1 if normal else 0, math.floor(normal * 0.05 + 0.5))
            if smoke
            else normal
        )
    if result["page_views"] < max(result["order_details"] * 8, result["searches"] * 3):
        raise ValueError("访问量不足：月度访问量须至少为订单明细的 8 倍、搜索量的 3 倍")
    return result


@dataclass(slots=True)
class GenerateConfig:
    start_date: date
    end_date: date
    dataset_start: date
    batch_size: int
    publish_workers: int
    user_count: int
    initial_user_count: int
    spu_count: int
    initial_spu_count: int
    promotion_count: int
    coupon_count: int
    order_detail_count: int
    page_view_count: int
    search_count: int
    warehouse_count: int
    is_smoke: bool = False
    data_dir: Path = field(default_factory=lambda: DATA_DIR)
    seed: int = field(default=SEED, init=False)

    def master_data_path(self, filename: str) -> Path:
        return self.data_dir / filename

    @classmethod
    def from_plan(
        cls, month: date, start: date, config: dict, scales: dict, smoke: bool
    ) -> GenerateConfig:
        first = month == start
        return cls(
            month,
            month_end(month),
            start,
            **config["runtime"],
            user_count=scales["new_users"],
            initial_user_count=scales["initial_users"] if first else 0,
            spu_count=scales["new_spus"],
            initial_spu_count=scales["initial_spus"] if first else 0,
            promotion_count=scales["promotions"],
            coupon_count=scales["coupons"],
            order_detail_count=scales["order_details"],
            page_view_count=scales["page_views"],
            search_count=scales["searches"],
            warehouse_count=scales["warehouses"],
            is_smoke=smoke,
        )


@dataclass(slots=True)
class RunContext:
    db: PostgresConfig
    gen: GenerateConfig = field(init=False)
    as_of_time: datetime = field(default_factory=now)
    lock: GenerationLock = field(init=False)
    engine: Engine = field(init=False)
    loader: PostgresLoader = field(init=False)
    expected_counts: dict[str, int] = field(default_factory=dict)
    validation_cutoff: datetime | None = None

    def __post_init__(self) -> None:
        from .database import PostgresLoader
        from .state_store import GenerationLock

        self.engine = create_engine(
            self.db.sqlalchemy_url,
            pool_pre_ping=True,
            connect_args={"options": "-c search_path=public -c timezone=Asia/Shanghai"},
        )
        self.lock = GenerationLock(self.engine)
        self.loader = PostgresLoader(self.write_transaction)

    def write_transaction(self):
        return self.lock.transaction()

    @property
    def run_id(self) -> str:
        return f"dbmock-{self.gen.dataset_start:%Y%m}-" + (
            "smoke" if self.gen.is_smoke else "full"
        )

    @property
    def initial_batch_id(self) -> str:
        return f"{self.run_id}-{self.gen.start_date:%Y-%m}-INIT"

    def period_batch_id(self, period_key: str) -> str:
        return f"{self.run_id}-{period_key}"

    @property
    def data_end_time(self) -> datetime:
        return datetime.combine(self.gen.end_date, time.max)

    def close(self) -> None:
        self.engine.dispose()
