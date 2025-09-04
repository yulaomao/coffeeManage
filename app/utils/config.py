import os
from datetime import timedelta


def env(key: str, default=None):
    return os.environ.get(key, default)


def load_config():
    return {
        "APP_VERSION": env("APP_VERSION", "0.1.0"),
        "SECRET_KEY": env("SECRET_KEY", "dev-secret"),
        "REDIS_URL": env("REDIS_URL", "redis://localhost:6379/0"),
        "SESSION_COOKIE_NAME": env("SESSION_COOKIE_NAME", "cm_session"),
        "CORS_ORIGINS": env("CORS_ORIGINS", "*"),
        "RATE_LIMITS": {
            "menu_write_per_min": int(env("RATE_MENU_WRITE_PER_MIN", 60)),
            "batch_dispatch_per_min": int(env("RATE_BATCH_PER_MIN", 10)),
        },
        "EXPORT_MAX_RANGE": int(env("EXPORT_MAX_RANGE", 31)),
        "MENU_MAX_ITEMS": int(env("MENU_MAX_ITEMS", 500)),
        "ENABLE_SSE": env("ENABLE_SSE", "0") == "1",
    }
