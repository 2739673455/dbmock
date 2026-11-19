MONTH_ARGS = $(if $(start),--start-month $(start)) $(if $(end),--end-month $(end))

.PHONY: help init-db generate smoke clean

help:
	@echo "make init-db                            - 在已有 PostgreSQL 空库中创建表"
	@echo "make generate start=2026-01 end=2026-08 - 首次生成"
	@echo "make generate end=2026-09               - 追加数据"
	@echo "make smoke start=2026-01 end=2026-08    - 首次生成，使用 5% 数据规模"
	@echo "make smoke end=2026-09                  - 追加 smoke 数据"
	@echo "make clean                              - 清理临时文件"

init-db:
	uv run scripts/init_db.py

generate:
	uv run main.py $(MONTH_ARGS)

smoke:
	uv run main.py $(MONTH_ARGS) --smoke

clean:
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete
