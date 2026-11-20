"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
集成了跨模块管道总线 (pipeline_bus) 的状态快照，供运营概览消费。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def replace_rows(self, module: str, rows: list[dict[str, Any]]) -> None:
        """整表替换：演练重置等场景用它把模块恢复到种子状态。"""
        self._tables[module] = [dict(row) for row in rows]

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        from app.pipeline import pipeline_bus
        pipe = pipeline_bus.snapshot()
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "管道事件入队", "value": pipe["pipeline_metrics"]["total_input"]},
            {"label": "管道事件消费", "value": pipe["pipeline_metrics"]["total_output"]},
            {"label": "审计记录数", "value": pipe["audit_count"]},
        ]
        return {"cards": cards, "modules": modules, "pipeline": pipe}


store = Store()
