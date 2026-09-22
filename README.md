# dbmock

## 环境要求

### 拉取 Git LFS 数据文件

`data/` 中的数据文件通过 Git LFS 管理。首次使用本仓库前，请先安装 Git LFS，然后在 dbmock 仓库根目录执行：

```bash
git lfs install
git lfs pull
```

`git lfs install` 通常只需为当前用户执行一次。未正确拉取时，数据文件的内容会是以 `version https://git-lfs.github.com/spec/v1` 开头的 Git LFS 指针；执行 `git lfs pull` 后会替换为实际数据。

### 启动依赖服务

本项目需要可访问的 Apache Doris 服务。使用已有服务时，直接在 `.env` 中填写连接信息。

在 dbmock 仓库根目录安装依赖并创建本地配置（作为子模块使用时先进入 `dbmock/`）：

```bash
cp .env.example .env
uv sync
```

`.env` 保存 Doris 连接信息：

```dotenv
DB_HOST="127.0.0.1"
DB_PORT=9030
DB_HTTP_PORT=8040
DB_USER="atguigu"
DB_PASSWORD="123123"
DB_NAME="ecommerce"
```

## 快速开始

### 1. 初始化 Doris

```bash
uv run scripts/init_db.py
```

该命令会删除并重建 `.env` 中 `DB_NAME` 指定的数据库，然后执行全部业务表和内部管理表 DDL。

### 2. 首次生成

在 `src/settings.py` 的 `GENERATION_CONFIG` 中配置规模，然后指定起止月份：

```bash
uv run main.py --start-month 2026-01 --end-month 2026-08
```

### 3. 追加或补充当月

```bash
uv run main.py --end-month 2026-09
```

追加时不要再次初始化数据库。需要小规模验证时，在生成命令后加 `--smoke`，日期范围不变，规模缩放到 5%。

本地进度保存在 `.generation-status.json`，包含起始月份、最新已提交截止时间 `cutoff`、已完成月份、当前月份、smoke 模式和待恢复标记 `needs_recovery`。生成进度提交及回滚时自动更新；时间使用北京时间。若运行中断且 `needs_recovery` 为 `true`，需再次执行生成命令完成恢复。该文件仅供查看，追加以数据库记录为准。

## 数仓模型

项目在 Doris 中创建 45 张业务表，覆盖以下主题域：

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
