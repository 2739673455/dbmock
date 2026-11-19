# dbmock

## 环境要求

### 拉取 Git LFS 数据文件

`data/` 中的数据文件通过 Git LFS 管理。首次使用本仓库前，请先安装 Git LFS，然后在 dbmock 仓库根目录执行：

```bash
git lfs install
git lfs pull
```

`git lfs install` 通常只需为当前用户执行一次。未正确拉取时，数据文件的内容会是以 `version https://git-lfs.github.com/spec/v1` 开头的 Git LFS 指针；执行 `git lfs pull` 后会替换为实际数据。

### 同步 Python 依赖

在 dbmock 仓库根目录执行（作为子模块使用时先进入 `dbmock/`）：

```bash
uv sync
```

### 创建本地配置

本项目需要 **PostgreSQL 15 或更高版本**（使用 `MERGE`）。请提前创建一个专用空数据库；配置中的用户需要对其 public schema 有建表权限，并可创建临时表。复制配置模板，然后在 `.env` 中填写该服务的连接信息：

```bash
cp .env.example .env
```

`.env` 配置示例：

```dotenv
DB_HOST="127.0.0.1"
DB_PORT=5432
DB_USER="dbmock"
DB_PASSWORD="replace-me"
DB_NAME="ecommerce"
```

## 快速开始

### 1. 初始化 PostgreSQL

```bash
uv run scripts/init_db.py
```

该命令只连接 `DB_NAME` 指定的已有数据库，在单个事务中创建 45 张业务表和 1 张管理表。public 中已有表、视图或序列时会退出；不会删除数据库，不会覆盖旧数据。首次创建数据库可用 `createdb -O dbmock ecommerce`（由管理员执行，角色须已存在）。

### 2. 首次生成

在 `src/settings.py` 的 `GENERATION_CONFIG` 中配置规模。首次生成需要同时指定 `--start-month` 和 `--end-month`，分别表示开始月份和截止月份：

```bash
uv run main.py --start-month 2026-01 --end-month 2026-08
```

### 3. 追加或补充当月

追加时只传 `--end-month`，不再指定开始月份。程序读取已有进度，自动补完未完成的月份，再连续生成到指定的截止月份。

```bash
uv run main.py --end-month 2026-09
```

例如，已有数据完整生成到 8 月，上面的命令会从 9 月开始生成。如果 9 月已生成过一部分，再执行同一命令会补充到本次任务启动时刻。截止月份不能超过当前月份；历史月份生成整月，当月只生成截至任务启动时刻的数据。已完整生成的月份不能重复生成，已有数据会保留。

追加时不要再次初始化数据库。需要小规模验证时，在生成命令后加 `--smoke`，日期范围不变，规模缩放到 5%。

## 数仓模型

项目在 PostgreSQL 中创建 45 张业务表，覆盖以下主题域：

- 公共维度：日期、渠道、页面、行政区划、支付方式和物流公司
- 用户域：用户拉链维度、用户标签及标签关系
- 商品域：SPU、SKU、品牌、类目、商家、店铺和价格变更
- 营销域：促销规则、优惠券模板、适用范围和用户券生命周期
- 流量域：会话、页面访问、搜索和搜索点击
- 互动域：收藏、取消收藏及购物车增删改清
- 交易域：订单明细、订单状态、活动优惠和优惠券分摊
- 支付域：支付明细、订单分摊及支付状态
- 履约域：配送单、配送商品和配送状态
- 售后域：退款、退款状态、退款支付和评价
- 库存域：库存变更流水和 SKU 日末库存快照

表名遵循以下约定：

- `dim_*_zip`：SCD2 拉链维度
- `dim_*`：普通维度或版本维度
- `bridge_*`：多对多关系桥接表
- `dwd_*_di`：明细事件事实表
- `dwd_*_df`：全量日快照表

完整 DDL 位于 `scripts/sql/ecommerce.sql`

## 校验和恢复

`uv run main.py --validate-only` 按数据库中已提交的截止时间校验；本地进度文件不是恢复依据。程序以 COPY 写临时表，再按原逻辑键 MERGE；只更新传入字段，显式 NULL 与省略字段不同。正常补充未结束的月份只发布新增和变更行；如果检测到上次写入中断，则先回滚到该月开始前，再按已冻结的计划恢复。同一数据库同时只允许一个造数或校验任务；普通查询不需要拿生成锁。

此版本只改 dbmock，不迁移已有 Doris 表或管理状态。请在新库从相同种子重新生成，不要将原数据目录当作 PostgreSQL 的数据目录。生成器显式分配代理键；不要向造数目标混入外部写入。真实内存占用随数据规模、批量大小、并发和服务配置变化。

## 运行数据库集成测试

在专用本地测试实例上提供允许创建数据库的账号。测试逐例创建随机命名的数据库，退出时只删除该例创建的库；未设置变量时跳过。端到端生成测试另需 `git lfs pull`。

```bash
DBMOCK_TEST_ADMIN_DSN="host=127.0.0.1 port=5432 dbname=postgres user=dbmock_test" \
  uv run python -m unittest discover -v
```
