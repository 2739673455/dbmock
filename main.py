"""按月生成电商数据，支持当月补充和连续追加。"""

from __future__ import annotations

import argparse
import logging

from src.generation import run
from src.logging_setup import configure_logging
from src.settings import PostgresConfig, RunContext, parse_month

logger = logging.getLogger(__name__)


def _month(value: str):
    try:
        return parse_month(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="按月生成和追加电商数仓模拟数据")
    parser.add_argument("--start-month", type=_month, help="首次生成的开始月份 YYYY-MM")
    parser.add_argument("--end-month", type=_month, help="生成或追加的截止月份 YYYY-MM")
    parser.add_argument(
        "--smoke",
        action="store_true",
        default=None,
        help="相同时间跨度，数据规模为正常抽样结果的 5%%；追加时自动沿用",
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="按已保存的进度校验现有数据"
    )
    args = parser.parse_args(argv)
    if args.validate_only:
        if args.start_month or args.end_month or args.smoke:
            parser.error("--validate-only 不接受月份和 smoke 参数")
    elif args.end_month is None:
        parser.error("生成时必须提供 --end-month")
    return args


def main():
    args = _parse_args()
    configure_logging("validate" if args.validate_only else "generate")
    ctx = RunContext(PostgresConfig())
    try:
        with ctx.lock as lock:
            run(ctx, args, lock.check)
    except BaseException:
        logger.exception("任务失败")
        raise
    finally:
        ctx.close()


if __name__ == "__main__":
    main()
