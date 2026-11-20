"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import dispatch_board, resource

# 固定路径的分发看板路由必须排在通用资源路由之前。
ROUTERS = [dispatch_board, resource]
