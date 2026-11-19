"""命令行任务日志配置。"""

import logging
import os
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.console import Group
from rich.logging import RichHandler
from rich.table import Table
from rich.text import Text


class ConsoleLogHandler(RichHandler):
    MONTH_COLORS = (
        "#56B4E9",  # 天蓝
        "#E69F00",  # 橙黄
        "#66CC99",  # 薄荷绿
        "#CC79A7",  # 紫红
        "#F0E442",  # 明黄
        "#9999FF",  # 淡紫
        "#FF8877",  # 珊瑚红
        "#44CCCC",  # 青绿
        "#DD99EE",  # 浅紫红
        "#AADD66",  # 黄绿
        "#FFBB88",  # 杏色
        "#88AAEE",  # 蓝紫
    )

    def render(self, *, record, traceback, message_renderable):
        month = ""
        if isinstance(message_renderable, Text) and message_renderable.plain.startswith(
            "month="
        ):
            prefix, separator, _ = message_renderable.plain.partition(" ")
            if separator:
                month = prefix.removeprefix("month=")
                message_renderable = message_renderable[len(prefix) + 1 :]
        month_style = "bold"
        if month:
            month_date = datetime.strptime(month, "%Y-%m")
            color_index = month_date.year * 12 + month_date.month - 1
            month_style += " " + self.MONTH_COLORS[color_index % len(self.MONTH_COLORS)]
        output = Table.grid(padding=(0, 1), expand=True)
        output.add_column(style="log.time", no_wrap=True)
        output.add_column(no_wrap=True)
        output.add_column(no_wrap=True)
        output.add_column(ratio=1, overflow="fold")
        output.add_row(
            datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
            Text(record.levelname, style=f"logging.level.{record.levelname.lower()}"),
            Text(month, style=month_style),
            Group(message_renderable, traceback) if traceback else message_renderable,
        )
        return output


def configure_logging(task: str) -> Path:
    log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    log_path = log_dir / f"{task}-{timestamp}-{os.getpid()}.log"
    console_handler = ConsoleLogHandler(
        console=Console(stderr=True),
        show_path=False,
        omit_repeated_times=False,
        log_time_format="%H:%M:%S",
        markup=False,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
    )
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler],
        force=True,
    )
    return log_path
