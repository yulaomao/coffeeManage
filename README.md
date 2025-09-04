# 智能自助咖啡机 — 管理后台（设备为中心）

一个最小可运行的 Flask + Redis 项目骨架，按“设备为中心”设计，内置菜单管理（分类/商品/发布/导入导出）API 与基础审计、速率限制与调度占位。

## 快速开始（Windows PowerShell）

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
# 可选：启动本地 Redis（需要你已安装 Redis for Windows 或使用 Docker）
# docker run -p 6379:6379 --name redis -d redis:7
$env:REDIS_URL = "redis://localhost:6379/0"
python run.py
```

访问：
- 健康检查：GET http://localhost:5000/healthz
- 拉起菜单：GET http://localhost:5000/api/v1/devices/dev-1/menu （请求头 X-Role: admin）

## 主要功能
- 设备为中心的键空间（cm:dev:{id}:*），菜单 CRUD、发布与可售集合维护
- 审计流：cm:stream:audit（XADD）
- 速率限制：菜单写操作基于 Redis INCR 固定窗口
- 调度器：APScheduler 启动，含命令回收占位任务

## 重要路径
- `app/__init__.py` 应用工厂
- `app/blueprints/api_v1.py` REST API（v1 摘要子集）
- `app/services/menu.py` 菜单服务
- `app/services/devices.py` 设备服务
- `app/utils/` 配置、扩展、RBAC、键工具与限流
- `app/tasks/jobs.py` 定时任务注册

## 扩展建议
- 完整实现订单、告警、指令批次与 SSE
- RBAC 接入真实用户与会话、CSRF
- 菜单可售计算加入配方启用与物料校验
- 导出任务与批次下发重试
