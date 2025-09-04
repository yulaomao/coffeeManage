from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from ..utils.extensions import redis_cli
from ..services.commands import CommandService


def register_jobs(sched: BackgroundScheduler, app):
    # 命令回收占位任务（每分钟）
    def recycle_inflight():
        try:
            CommandService.recycle_inflight()
        except Exception:
            # best-effort
            pass

    sched.add_job(recycle_inflight, 'interval', minutes=1, id='recycle_inflight', max_instances=1, coalesce=True)
