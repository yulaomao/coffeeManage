import functools
from flask import request, current_app


def get_current_role() -> str:
    # 简化：从请求头 X-Role 读取，默认 admin
    return request.headers.get("X-Role", "admin")


def require_role(roles):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            role = get_current_role()
            if role not in roles:
                return {"ok": False, "error": "UNAUTHORIZED"}, 401
            return fn(*args, **kwargs)
        return wrapper
    return deco
