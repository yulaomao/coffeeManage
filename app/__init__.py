import os
from flask import Flask
from .utils.config import load_config
from .utils.extensions import redis_cli, scheduler, cors
from .blueprints.api_v1 import api_v1_bp
from .blueprints.ui import ui_bp


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    cfg = load_config()
    app.config.update(cfg)

    # Init extensions
    cors.init_app(app, resources={r"/api/*": {"origins": app.config.get("CORS_ORIGINS", "*")}})
    redis_cli.init_app(app)
    scheduler.init_app(app)
    # 延迟导入注册任务，避免循环引用
    from .tasks.jobs import register_jobs
    register_jobs(scheduler.instance, app)

    # Blueprints
    app.register_blueprint(api_v1_bp, url_prefix="/api/v1")
    app.register_blueprint(ui_bp)

    # Health
    @app.get("/healthz")
    def healthz():
        return {"ok": True, "ver": app.config.get("APP_VERSION", "dev")}

    return app
