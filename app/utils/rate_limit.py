class RateLimited(Exception):
    pass


def check_rate(r, key: str, limit: int, window: int = 60):
    # 简单固定窗口：INCR + EXPIRE
    cnt = r.incr(key)
    if cnt == 1:
        r.expire(key, window)
    if cnt > limit:
        raise RateLimited("RATE_LIMITED")
